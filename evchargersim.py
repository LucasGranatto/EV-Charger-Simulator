"""
EVChargerSim — simulador de Charge Point OCPP 1.6J, usando mobilityhouse/ocpp.

Objetivo: simular o lado "carro/carregador" de um ponto de carga AC
genérico, conectando no seu CSMS real via WebSocket OCPP 1.6J, pra você
testar a lógica do servidor (SetChargingProfile, RemoteStartTransaction,
etc) sem precisar de hardware físico.

Uso:
    python evchargersim.py                  (usa o ID padrão EVCHARGERSIM_01
                                                e conecta em ws://localhost:9000)
    python evchargersim.py CARREGADOR_02     (ID customizado — útil para
                                                rodar várias instâncias ao
                                                mesmo tempo, simulando um
                                                site com múltiplos chargers)
    python evchargersim.py CARREGADOR_02 --url ws://192.168.15.18:9000
                                              (aponta pra um CSMS específico,
                                                sem precisar editar o arquivo)
    python evchargersim.py --config sim.json (carrega valores padrão de um
                                                arquivo JSON — ver
                                                SimConfig.FIELDS abaixo para
                                                as chaves aceitas. Flags de
                                                linha de comando, quando
                                                passadas, sempre têm
                                                prioridade sobre o arquivo.)
    python evchargersim.py --verbose         (mostra Heartbeat e
                                                GetConfiguration no terminal
                                                — por padrão ficam silenciosos
                                                em DEBUG; MeterValues já
                                                aparece sempre)

Para testar load balancing entre carregadores, abra um terminal por
instância, por exemplo:
    python evchargersim.py CARREGADOR_01
    python evchargersim.py CARREGADOR_02
    python evchargersim.py CARREGADOR_03

Se a conexão com o CSMS cair (ou ele ainda não estiver de pé), o
simulador tenta reconectar automaticamente com backoff exponencial —
não precisa reiniciar o script manualmente. Enquanto offline, a sessão
continua rodando fisicamente (SoC sobe, energia acumula) e mensagens
que não puderam ser enviadas (StatusNotification, MeterValues,
Start/StopTransaction) ficam numa fila local, entregues ao CSMS em
ordem assim que a conexão volta — ver comando "queue" no console.

Para injetar instabilidade de rede de propósito (testar a robustez do
seu CSMS sem depender de uma queda real):
    python evchargersim.py --chaos-disconnect-interval 60
                                              (derruba o WebSocket a cada
                                                ~60s ± jitter)
    python evchargersim.py --chaos-latency-min 200 --chaos-latency-max 2000
                                              (atraso artificial de 200–2000ms
                                                antes de cada mensagem)
    python evchargersim.py --chaos-drop-rate 0.1
                                              (10% das mensagens são
                                                simuladas como perdidas)
Essas flags podem ser combinadas, e o comando "disconnect" no console
derruba a conexão manualmente a qualquer momento, sem precisar delas.

Comandos disponíveis no terminal durante a execução:
    start <id_tag>   -> simula motorista passando RFID no totem (Authorize
                        ou lista local, se o id_tag estiver nela → StartTransaction).
                        Recusado se o conector estiver reservado para outro
                        id_tag, Inoperative (ChangeAvailability), ou Faulted.
    stop             -> simula cliente encerrando sessão localmente
                        (cabo desconectado / botão no carro), Reason.ev_disconnected
    pause            -> simula carro pausando o carregamento (→ SuspendedEV)
    resume           -> retoma carregamento após pause (→ Charging)
    fault <código>   -> dispara StatusNotification com erro; códigos válidos:
                        ground_failure, over_current_failure, over_voltage,
                        connector_lock_failure, power_meter_failure,
                        weak_signal, other_error
    clear            -> limpa uma falha ativa, voltando para Available
                        (necessário depois de um "fault" para poder usar
                        "start" de novo)
    datatransfer <vendor_id> [message_id] [data]
                     -> envia um DataTransfer do charger para o CSMS
    help             -> lista todos os comandos

Reserva (ReserveNow/CancelReservation), lista local de autorização
(SendLocalList) e disponibilidade (ChangeAvailability) são controladas
pelo CSMS via mensagens OCPP — "start" respeita todas automaticamente,
sem comando de console dedicado para elas.
"""

import argparse
import asyncio
import json
import logging
import random
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone

import websockets
# BUG REAL PRÉ-EXISTENTE, corrigido aqui: a lib `websockets` usa lazy
# loading (PEP 562) no seu __init__.py e só expõe automaticamente um
# conjunto fixo de nomes top-level (connect, serve, etc) — `exceptions`
# NÃO está nessa lista. Sem este import explícito, qualquer
# `websockets.exceptions.ConnectionClosed` no código (inclusive o já
# existente antes desta sessão, no except de reconexão de main())
# levanta AttributeError na hora de casar a exceção — ou seja, uma queda
# de rede REAL provavelmente nunca era capturada corretamente pelo
# except dedicado; o AttributeError substituía silenciosamente a
# exceção original. Importar o submódulo explicitamente uma vez aqui
# resolve para o arquivo inteiro (comportamento padrão de import do
# Python: o submódulo fica de fato vinculado como atributo do pacote).
import websockets.exceptions
from ocpp.routing import on
from ocpp.v16 import call, call_result
from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.v16.enums import (
    Action,
    AuthorizationStatus,
    AvailabilityStatus,
    AvailabilityType,
    CancelReservationStatus,
    ChargePointErrorCode,
    ChargePointStatus,
    DataTransferStatus,
    DiagnosticsStatus,
    FirmwareStatus,
    Reason,
    RegistrationStatus,
    RemoteStartStopStatus,
    ReservationStatus,
    ResetType,
    UnlockStatus,
    UpdateStatus,
)

# ============================================================
# CONFIGURAÇÃO — construída a partir de defaults + arquivo --config + CLI
# ============================================================

# Mapa de nomes amigáveis (console) -> ChargePointErrorCode (OCPP). Fica
# fora do SimConfig porque é uma tabela fixa do protocolo, não um
# parâmetro de simulação que faça sentido sobrescrever por instância.
FAULT_CODE_MAP = {
    "ground_failure":         ChargePointErrorCode.ground_failure,
    "over_current_failure":   ChargePointErrorCode.over_current_failure,
    "over_voltage":           ChargePointErrorCode.over_voltage,
    "connector_lock_failure": ChargePointErrorCode.connector_lock_failure,
    "power_meter_failure":    ChargePointErrorCode.power_meter_failure,
    "weak_signal":            ChargePointErrorCode.weak_signal,
    "other_error":            ChargePointErrorCode.other_error,
}


@dataclass
class SimConfig:
    """
    Configuração de uma instância do simulador — tudo aqui é fixo depois
    do boot (diferente de ChargerState, que muda a cada mensagem/comando).
    Substituiu o antigo bloco de constantes soltas no topo do módulo:
    antes, rodar duas instâncias com parâmetros diferentes (ex: baterias
    de tamanhos distintos) exigia editar o arquivo ou duplicar o script;
    agora é --config a.json / --config b.json, ou flags de linha de
    comando pontuais.

    Precedência de valores: CLI (quando a flag é passada) > arquivo JSON
    (--config) > defaults abaixo.
    """
    charge_point_id: str = "EVCHARGERSIM_01"
    url: str = "ws://localhost:9000"
    verbose: bool = False
    connector_id: int = 1

    meter_values_interval: int = 30
    heartbeat_interval: int = 120

    default_offered_amps: float = 16.0
    simulation_speed: float = 1.0

    battery_capacity_wh: float = 50_000.0
    initial_soc_percent: float = 20.0

    nominal_voltage: float = 225.0

    # Timeout para chamadas OCPP consideradas críticas (Start/StopTransaction).
    # Sem isso, um CSMS que trava sem responder deixava o simulador
    # pendurado num `await self.call(...)` para sempre — nenhuma exceção,
    # nenhum log, só um "start"/"stop" que nunca completa. Ver
    # _send_start_transaction / _send_stop_transaction.
    call_timeout_seconds: float = 30.0

    # ── INSTABILIDADE DE REDE INJETÁVEL (chaos) ─────────────────────
    # Tudo aqui é opt-in — 0/desligado por padrão, sem mudar em nada o
    # comportamento de quem não passar essas flags.

    # Desconecta o WebSocket de propósito a cada N segundos (± jitter),
    # pra testar a lógica de reconexão/fila offline do CSMS sem precisar
    # derrubar o servidor manualmente. 0 = desabilitado.
    chaos_disconnect_interval_seconds: float = 0.0
    chaos_disconnect_jitter_seconds: float = 5.0

    # Atraso artificial (ms) antes de cada mensagem enviada, simulando
    # rede lenta/alta latência. 0/0 = desabilitado.
    chaos_latency_min_ms: float = 0.0
    chaos_latency_max_ms: float = 0.0

    # Probabilidade (0.0–1.0) de uma mensagem enviada ser simulada como
    # "perdida na rede" — nunca chega a sair de verdade. 0.0 = desabilitado.
    chaos_drop_rate: float = 0.0

    @classmethod
    def load(cls, argv=None) -> "SimConfig":
        """Monta a config final combinando defaults, --config e flags de CLI."""
        args = _parse_args(argv)
        cfg = cls()

        if args.config:
            try:
                with open(args.config, "r", encoding="utf-8") as fh:
                    overrides = json.load(fh)
            except (OSError, json.JSONDecodeError) as exc:
                raise SystemExit(
                    f"Não foi possível ler --config '{args.config}': {exc}"
                )
            unknown = set(overrides) - {f for f in cfg.__dataclass_fields__}
            if unknown:
                raise SystemExit(
                    f"Chave(s) desconhecida(s) em '{args.config}': "
                    f"{', '.join(sorted(unknown))}. Chaves válidas: "
                    f"{', '.join(sorted(cfg.__dataclass_fields__))}"
                )
            for key, value in overrides.items():
                setattr(cfg, key, value)

        # Só sobrescreve com CLI o que foi de fato passado (valor != None
        # nos argumentos opcionais) — senão o default do argparse sempre
        # pisaria no valor vindo do --config.
        cli_overrides = {
            "charge_point_id": args.charge_point_id,
            "url": args.url,
            "connector_id": args.connector_id,
            "meter_values_interval": args.meter_interval,
            "heartbeat_interval": args.heartbeat_interval,
            "default_offered_amps": args.default_amps,
            "simulation_speed": args.sim_speed,
            "battery_capacity_wh": args.battery_wh,
            "initial_soc_percent": args.initial_soc,
            "nominal_voltage": args.voltage,
            "call_timeout_seconds": args.call_timeout,
            "chaos_disconnect_interval_seconds": args.chaos_disconnect_interval,
            "chaos_disconnect_jitter_seconds": args.chaos_disconnect_jitter,
            "chaos_latency_min_ms": args.chaos_latency_min,
            "chaos_latency_max_ms": args.chaos_latency_max,
            "chaos_drop_rate": args.chaos_drop_rate,
        }
        for key, value in cli_overrides.items():
            if value is not None:
                setattr(cfg, key, value)
        if args.verbose:
            cfg.verbose = True

        return cfg


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="EVChargerSim — simulador standalone de Charge Point OCPP 1.6J.")
    parser.add_argument(
        "charge_point_id", nargs="?", default=None,
        help="ID do charge point (padrão: EVCHARGERSIM_01, ou o valor de "
             "--config). Permite rodar várias instâncias simultâneas, cada "
             "uma com seu próprio ID.")
    parser.add_argument(
        "--url", default=None,
        help="URL base do CSMS, SEM o charge point ID no final "
             "(padrão: ws://localhost:9000). O ID é sempre anexado "
             "automaticamente ao conectar.")
    parser.add_argument(
        "--config", default=None,
        help="Caminho para um arquivo JSON com valores de configuração "
             "(ver SimConfig no topo do arquivo para as chaves aceitas). "
             "Flags de linha de comando têm prioridade sobre o arquivo.")
    parser.add_argument("--connector-id", type=int, default=None)
    parser.add_argument("--meter-interval", type=int, default=None,
                         help="Intervalo de MeterValues em segundos (padrão: 30).")
    parser.add_argument("--heartbeat-interval", type=int, default=None,
                         help="Intervalo inicial de Heartbeat em segundos (padrão: 120).")
    parser.add_argument("--default-amps", type=float, default=None,
                         help="Corrente aplicada ao iniciar sessão, antes do "
                              "primeiro SetChargingProfile (padrão: 16.0).")
    parser.add_argument("--sim-speed", type=float, default=None,
                         help="Fator de aceleração da simulação de bateria/energia "
                              "(padrão: 1.0 = tempo real; 60.0 = 60x mais rápido).")
    parser.add_argument("--battery-wh", type=float, default=None,
                         help="Capacidade da bateria simulada em Wh (padrão: 50000).")
    parser.add_argument("--initial-soc", type=float, default=None,
                         help="SoC inicial de cada sessão, em %% (padrão: 20.0).")
    parser.add_argument("--voltage", type=float, default=None,
                         help="Tensão nominal de referência em V (padrão: 225.0).")
    parser.add_argument("--call-timeout", type=float, default=None,
                         help="Timeout em segundos para chamadas críticas "
                              "(Start/StopTransaction) ao CSMS (padrão: 30.0).")
    parser.add_argument(
        "--chaos-disconnect-interval", type=float, default=None,
        help="Derruba o WebSocket de propósito a cada N segundos (± jitter), "
             "para testar reconexão/fila offline do CSMS. 0/omitido = desabilitado.")
    parser.add_argument(
        "--chaos-disconnect-jitter", type=float, default=None,
        help="Variação aleatória (± segundos) em torno de --chaos-disconnect-interval "
             "(padrão: 5.0).")
    parser.add_argument(
        "--chaos-latency-min", type=float, default=None,
        help="Atraso mínimo artificial (ms) antes de cada mensagem enviada "
             "(padrão: 0).")
    parser.add_argument(
        "--chaos-latency-max", type=float, default=None,
        help="Atraso máximo artificial (ms) antes de cada mensagem enviada — "
             "o atraso real de cada envio é sorteado entre min e max (padrão: 0).")
    parser.add_argument(
        "--chaos-drop-rate", type=float, default=None,
        help="Probabilidade (0.0–1.0) de uma mensagem enviada ser simulada "
             "como perdida na rede (padrão: 0.0).")
    parser.add_argument(
        "--verbose", action="store_true",
        help="Mostra Heartbeat e GetConfiguration no terminal (por padrão "
             "ficam em nível DEBUG, silenciosos, já que repetem sem trazer "
             "informação nova a cada ciclo). MeterValues aparece sempre, "
             "independente desta flag.")
    return parser.parse_args(argv)


@dataclass
class ChargerState:
    """
    Estado de sessão/runtime de UM charge point simulado — tudo aqui muda
    ao longo da execução (diferente de SimConfig, que é fixo após o
    boot). Antes vivia como ~10 variáveis `global` soltas no módulo, o
    que tornava impossível rodar duas instâncias de EVChargerSim no
    mesmo processo (ex: um teste automatizado com vários chargers) sem
    elas pisarem no estado umas das outras. Agora cada EVChargerSim tem
    seu próprio `self.state`.
    """
    # Corrente "real" que o carregador simulado está entregando neste
    # momento. Começa em 0 (sem carro) e é atualizada quando o CSMS manda
    # SetChargingProfile.
    current_offered_amps: float = 0.0
    current_actual_amps: float = 0.0  # o que o "carro" simula estar de fato puxando

    # Estado de sessão/transação — necessário para StartTransaction/
    # StopTransaction, que são as mensagens que o CSMS usa para abrir/
    # fechar sessão no banco de dados.
    active_transaction_id: int | None = None
    energy_meter_wh: float = 0.0  # contador de energia acumulada simulado (Wh)

    # Intervalo de heartbeat ATUAL — pode ser alterado em runtime via
    # ChangeConfiguration(key='HeartbeatInterval'). Separado do valor
    # inicial em SimConfig.heartbeat_interval; send_heartbeat_loop relê
    # este campo a cada ciclo, então a mudança tem efeito imediato.
    current_heartbeat_interval: int = 120

    # ── SIMULAÇÃO DE BATERIA (SoC) ─────────────────────────────────
    battery_soc_percent: float = 20.0

    # Flag de pausa — True enquanto o carro estiver em SuspendedEV.
    # O energy_accumulator_loop respeita esse flag e para de acumular.
    session_suspended: bool = False

    # Flag distinta de session_suspended acima: True enquanto o CSMS
    # estiver impondo 0A via SetChargingProfile (ex: fila de espera do
    # balanceamento de site) — SuspendedEVSE, e não SuspendedEV. São
    # duas causas de suspensão diferentes (lado do carro vs. lado do
    # equipamento) e cada uma tem seu próprio status OCPP.
    evse_suspended_by_profile: bool = False

    # True entre um comando "fault" e um "clear" — enquanto ativo, o
    # console recusa "start" (não faz sentido iniciar sessão num charger
    # em Faulted) até o operador limpar a falha explicitamente,
    # espelhando um charger físico real que não volta a Available
    # sozinho após um erro de hardware.
    is_faulted: bool = False

    # ── RESERVA (ReserveNow / CancelReservation) ────────────────────
    # Enquanto reservation_id não for None, "start" local só é aceito se
    # o id_tag bater com reserved_for_id_tag (ou com reserved_parent_id_tag,
    # quando o CSMS informou um grupo) — replica o comportamento real de
    # um charger reservado recusar motoristas sem o RFID certo.
    reservation_id: int | None = None
    reserved_for_id_tag: str | None = None
    reserved_parent_id_tag: str | None = None

    # ── LISTA LOCAL DE AUTORIZAÇÃO (SendLocalList / GetLocalListVersion) ──
    # Mapa id_tag -> status ("Accepted"/"Blocked"/"Expired"/"Invalid").
    # Quando um id_tag está aqui, o fluxo de start local usa esse status
    # diretamente em vez de chamar Authorize no CSMS — simula um charger
    # capaz de autorizar localmente/offline com uma lista pré-carregada.
    local_auth_list: dict = field(default_factory=dict)
    local_list_version: int = 0

    # ── DISPONIBILIDADE (ChangeAvailability) ────────────────────────
    # "Operative" (padrão) ou "Inoperative". Enquanto Inoperative, novas
    # sessões (local ou remota) são recusadas — replica um operador
    # marcando o conector fora de serviço no dashboard.
    availability_status: str = "Operative"
    # Guarda uma mudança para Inoperative pedida DURANTE uma sessão ativa
    # — pelo spec OCPP, nesse caso a resposta deve ser "Scheduled" e a
    # mudança só é aplicada de fato quando a sessão termina (não dá pra
    # tirar o conector de operação com o carro ainda carregando).
    pending_availability_change: str | None = None

    # ── FILA DE TRANSAÇÕES OFFLINE ──────────────────────────────────
    # Mensagens que não puderam ser enviadas porque o simulador estava
    # sem conexão (ou uma mensagem foi simulada como perdida via chaos)
    # esperam aqui até a próxima reconexão, quando são reenviadas em
    # ordem — ver EVChargerSim._call_or_queue / _flush_offline_queue.
    # Cada item: {"kind": str, "request": <objeto call.X>, "local_tx_id": int|None}.
    # local_tx_id só é usado por StartTransaction/StopTransaction que
    # aconteceram com a sessão ainda não confirmada pelo CSMS (ver
    # EVChargerSim._local_tx_counter).
    offline_queue: list = field(default_factory=list)


def read_grid_voltage(nominal_voltage: float) -> float:
    """Simula pequena flutuação natural da tensão de rede (~±1.5V)."""
    return round(nominal_voltage + random.uniform(-1.5, 1.5), 1)


class _ColorFormatter(logging.Formatter):
    """
    Formatter com cores ANSI — timestamp, charge point ID e nível de log
    cada um com sua própria cor, e a MENSAGEM em si na cor padrão do
    terminal (sem tingir). Antes a linha inteira saía na cor do nível,
    o que deixava o texto real (a parte que importa ler) tão colorido
    quanto os metadados ao redor dele; separar as cores deixa mais fácil
    escanear "quando / de qual charger / que tipo de evento" de relance
    e ainda ler o conteúdo da mensagem sem esforço extra.

    use_color desliga tudo automaticamente quando a saída não é um
    terminal real (ex: `python evchargersim.py > log.txt` ou quando um
    outro processo captura o stdout) — sem isso, o arquivo/pipe ficaria
    cheio de códigos de escape ilegíveis em vez de texto limpo.
    """
    _LEVEL_COLORS = {
        logging.DEBUG:    "\033[2m",     # cinza (dim)
        logging.INFO:     "\033[36m",    # ciano
        logging.WARNING:  "\033[33m",    # amarelo
        logging.ERROR:    "\033[31m",    # vermelho
        logging.CRITICAL: "\033[1;31m",  # vermelho negrito
    }
    _TIME_COLOR = "\033[2m"    # cinza (dim) — timestamp é o metadado menos importante
    _ID_COLOR = "\033[1;34m"   # azul negrito — destaca o charge point ID
    _RESET = "\033[0m"

    def __init__(self, datefmt, charge_point_id, use_color):
        super().__init__(datefmt=datefmt)
        self._tag = f"[{charge_point_id}]"
        self._use_color = use_color

    def format(self, record):
        timestamp = self.formatTime(record, self.datefmt)
        level = f"{record.levelname:<7}"
        message = record.getMessage()

        # Preserva o comportamento padrão do logging para exceções: se o
        # log veio de logger.exception(...)/exc_info=True, anexa o
        # traceback formatado depois da mensagem (senão o traceback
        # inteiro seria descartado silenciosamente por este formatter
        # customizado, ao contrário do logging.Formatter padrão).
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            message = f"{message}\n{record.exc_text}"

        if not self._use_color:
            return f"{timestamp} {self._tag} {level} {message}"

        level_color = self._LEVEL_COLORS.get(record.levelno, "")
        return (
            f"{self._TIME_COLOR}{timestamp}{self._RESET} "
            f"{self._ID_COLOR}{self._tag}{self._RESET} "
            f"{level_color}{level}{self._RESET} "
            f"{message}"
        )


def build_logger(charge_point_id: str, verbose: bool) -> logging.Logger:
    """
    Cria o logger deste módulo. Extraído para uma função (em vez de
    código solto no nível do módulo) para que possa ser chamado depois
    que SimConfig.load() souber o charge_point_id e a flag --verbose —
    antes, essas duas coisas eram lidas de _parse_args() direto no
    escopo do módulo, o que amarrava a configuração de logging à
    existência de argumentos globais de CLI.
    """
    use_color = sys.stdout.isatty()
    handler = logging.StreamHandler()
    handler.setFormatter(_ColorFormatter(
        datefmt="%H:%M:%S",
        charge_point_id=charge_point_id,
        use_color=use_color,
    ))
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, handlers=[handler])
    module_logger = logging.getLogger("evchargersim")

    # A biblioteca ocpp loga CADA mensagem OCPP crua (send/receive, JSON
    # completo) no logger "ocpp" em nível INFO — é isso que produz aqueles
    # blocos gigantes de JSON quebrados em várias linhas no terminal,
    # atropelando os logs legíveis deste script (ex: as linhas verdes de
    # MeterValues). Subindo para WARNING, só erros/CALLError da lib
    # aparecem; o tráfego OCPP completo continua sendo processado
    # normalmente, só não é mais IMPRESSO.
    logging.getLogger("ocpp").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    return module_logger


def compute_actual_current(offered_amps: float, soc_percent: float) -> float:
    """
    Calcula a corrente real que o "carro" puxaria dado o limite oferecido
    pelo CSMS e o estado de carga atual da bateria (SoC).

    Carregamento AC (diferente de DC rápido) tende a respeitar bem o
    limite oferecido na maior parte da curva — a redução por tapering só
    fica perceptível perto do fim (SoC alto), quando o carregador de
    bordo do veículo reduz a corrente para proteger a bateria.

    Função pura (sem estado global/de instância) de propósito — fácil de
    testar isoladamente com unittest, sem precisar montar um EVChargerSim
    inteiro. Ver test_evchargersim.py.
    """
    if offered_amps <= 0:
        return 0.0
    if soc_percent < 80:
        factor = 0.97  # praticamente o limite oferecido inteiro
    elif soc_percent < 90:
        factor = 0.75
    elif soc_percent < 97:
        factor = 0.45
    else:
        factor = 0.15  # últimos % da bateria, corrente bem reduzida
    return round(offered_amps * factor, 1)


def _meter_line_color(has_session: bool, suspended: bool, faulted: bool, use_color: bool) -> str:
    """
    Cor da linha de MeterValues conforme o estado atual do charger —
    verde carregando normalmente, amarelo suspenso (carro ou CSMS
    pausou), cinza sem sessão, vermelho em Faulted. Sem isso, a linha
    de status mais frequente do terminal saía sempre na mesma cor,
    então "está carregando de verdade ou só suspenso?" exigia ler o
    texto todo em vez de notar pela cor.
    """
    if not use_color:
        return ""
    if faulted:
        return "\033[31m"    # vermelho
    if not has_session:
        return "\033[2m"     # cinza (dim)
    if suspended:
        return "\033[33m"    # amarelo
    return "\033[32m"        # verde


class EVChargerSim(BaseChargePoint):
    """
    Representa um Charge Point AC genérico do ponto de vista do protocolo.
    Implementa os handlers de mensagens que o CSMS pode mandar PARA o charge point.
    """

    def __init__(self, charge_point_id, connection, config: SimConfig, logger: logging.Logger):
        super().__init__(charge_point_id, connection)
        self.config = config
        # IMPORTANTE: NÃO usar o nome `self.logger` aqui — BaseChargePoint
        # já usa esse atributo internamente (default: logging.getLogger
        # ("ocpp")) para logar CADA mensagem OCPP crua enviada/recebida
        # ("%s: receive message %s" / "%s: send %s" em charge_point.py da
        # lib). Um bug real aconteceu aqui antes: sobrescrever
        # self.logger com o logger deste módulo fazia com que essas
        # mensagens brutas passassem a sair pelo NOSSO logger — que não
        # está suprimido — em vez do logger "ocpp" (que build_logger()
        # sobe para WARNING de propósito). Resultado: o terminal enchia
        # de JSON cru de novo mesmo com build_logger() aparentemente
        # correto, porque a supressão em "ocpp" não tinha mais efeito
        # nenhum sobre essas chamadas. Por isso o atributo aqui se chama
        # `self.log`, não `self.logger` — e todo o resto da classe
        # também usa `self.log`, nunca `self.logger`.
        self.log = logger
        self.state = ChargerState(
            battery_soc_percent=config.initial_soc_percent,
            current_heartbeat_interval=config.heartbeat_interval,
        )
        self.use_color = sys.stdout.isatty()

        # Task de fundo que percorre os períodos do perfil de carga
        # atualmente ativo (ver _run_charging_schedule). Guardado à parte
        # de ChargerState porque é uma asyncio.Task, não um dado de
        # estado serializável — cancelado e substituído a cada novo
        # SetChargingProfile/ClearChargingProfile/fim de sessão.
        self._profile_task: asyncio.Task | None = None

        # ── Plumbing de conectividade (fila offline / reconexão) ────
        # Também instância, não ChargerState: são detalhes de transporte,
        # não "dados simulados" do carregador. main() alterna esta flag
        # (e reatribui self._connection, herdado de BaseChargePoint) a
        # cada queda/reconexão — a instância inteira de EVChargerSim
        # persiste através de reconexões, só a conexão WebSocket por
        # baixo é trocada.
        self.is_online: bool = False
        # ID negativo temporário atribuído a uma sessão que começou (via
        # StartTransaction) enquanto offline, até o CSMS confirmar um ID
        # real no flush da fila — ver _send_start_transaction.
        self._pending_local_tx_id: int | None = None
        self._local_tx_counter: int = 0

    # --------------------------------------------------------
    # Handlers de mensagens recebidas do CSMS
    # --------------------------------------------------------

    def _limit_to_amps(self, limit: float, unit: str) -> float:
        """
        Converte um limite de chargingSchedulePeriod para amperes.

        chargingRateUnit pode ser "A" (ampere, já pronto pra uso) ou "W"
        (watts totais) — um CSMS que manda limites em W é comum (ex:
        perfis pensados em kW de site) e antes esse valor era tratado
        como se já estivesse em amperes, o que inflava/reduzia a corrente
        real aplicada silenciosamente (ex: um limite de 3700W virava
        "3700A" oferecidos). Convertemos usando a tensão nominal
        configurada — simplificação de carga monofásica; ajuste aqui se
        seu CSMS testar perfis trifásicos.
        """
        if unit == "W":
            return round(limit / self.config.nominal_voltage, 2)
        if unit and unit != "A":
            self.log.warning(
                f"[PERFIL RECEBIDO] chargingRateUnit desconhecido '{unit}' — "
                "tratando como amperes (A)."
            )
        return float(limit)

    def _cancel_profile_task(self):
        """Cancela a task de agendamento de um perfil anterior, se houver."""
        if self._profile_task is not None and not self._profile_task.done():
            self._profile_task.cancel()
        self._profile_task = None

    def _enqueue_offline(self, kind: str, request, local_tx_id: int | None = None):
        """Acrescenta uma mensagem à fila offline, pra reenvio na próxima reconexão."""
        self.state.offline_queue.append(
            {"kind": kind, "request": request, "local_tx_id": local_tx_id}
        )
        self.log.info(
            f"[FILA OFFLINE] '{kind}' enfileirado "
            f"(fila agora com {len(self.state.offline_queue)} mensagem(ns))."
        )

    async def _call_or_queue(
        self,
        request,
        kind: str,
        queueable: bool = True,
        timeout: float | None = None,
        queue_on_timeout: bool = False,
        local_tx_id: int | None = None,
    ):
        """
        Ponto único por onde toda mensagem "espontânea" do charger
        (StatusNotification, MeterValues, Heartbeat, Start/StopTransaction)
        passa antes de sair pela rede de verdade. Reúne duas
        responsabilidades relacionadas:

        1) FILA OFFLINE: se o simulador está offline (ou perde a conexão
           bem no meio da tentativa), mensagens `queueable=True` são
           guardadas em vez de simplesmente falhar — ver
           _flush_offline_queue para o reenvio. Mensagens não essenciais
           (ex: Heartbeat) usam queueable=False: são só puladas enquanto
           offline, sem acumular na fila.

        2) INSTABILIDADE DE REDE INJETÁVEL: latência artificial e perda
           simulada de mensagens (SimConfig.chaos_*) são aplicadas aqui,
           antes de qualquer tentativa real de envio — assim toda rotina
           que manda mensagem ganha esse comportamento de graça, sem
           precisar implementar chaos em cada uma individualmente.

        queue_on_timeout controla se um simples "CSMS não respondeu a
        tempo" (socket ainda pode estar são) deve ser tratado como
        motivo para enfileirar — usado em Start/StopTransaction (não dá
        pra simplesmente desistir de registrar uma transação), mas não
        em mensagens periódicas como StatusNotification/MeterValues
        (reenviar uma leitura de medidor velha depois não tem tanto valor).

        Retorna a resposta do CSMS, ou None se a mensagem foi enfileirada,
        descartada (chaos) ou não teve resposta a tempo.
        """
        timeout = timeout if timeout is not None else self.config.call_timeout_seconds

        # Chaos: atraso artificial antes de sequer tentar enviar.
        if self.config.chaos_latency_max_ms > 0:
            delay_ms = random.uniform(
                self.config.chaos_latency_min_ms, self.config.chaos_latency_max_ms
            )
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000)

        # Chaos: perda de mensagem simulada — nunca tentamos enviar de verdade.
        if self.config.chaos_drop_rate > 0 and random.random() < self.config.chaos_drop_rate:
            self.log.warning(f"[CHAOS] '{kind}' descartado (perda de rede simulada).")
            if queueable:
                self._enqueue_offline(kind, request, local_tx_id=local_tx_id)
            return None

        if not self.is_online:
            if queueable:
                self._enqueue_offline(kind, request, local_tx_id=local_tx_id)
            else:
                self.log.debug(f"[OFFLINE] '{kind}' pulado (não crítico, não enfileirável).")
            return None

        try:
            return await asyncio.wait_for(self.call(request), timeout=timeout)
        except asyncio.TimeoutError:
            self.log.warning(f"[CSMS] '{kind}' não teve resposta em {timeout}s.")
            if queueable and queue_on_timeout:
                self._enqueue_offline(kind, request, local_tx_id=local_tx_id)
            return None
        except (websockets.exceptions.ConnectionClosed, OSError) as exc:
            self.log.warning(f"[OFFLINE] conexão perdida enviando '{kind}' ({exc!r}).")
            self.is_online = False
            if queueable:
                self._enqueue_offline(kind, request, local_tx_id=local_tx_id)
            return None

    async def _flush_offline_queue(self):
        """
        Reenvia, em ordem, as mensagens acumuladas enquanto o simulador
        estava offline — é isso que dá sentido a uma sessão que começou
        (ou terminou) sem conexão: StartTransaction/StopTransaction/
        StatusNotification/MeterValues enfileirados são entregues ao
        CSMS assim que a conexão volta, na mesma ordem em que aconteceram
        de verdade.

        Se um StartTransaction enfileirado usava um ID local temporário
        (negativo, atribuído por _send_start_transaction enquanto
        offline), o ID real devolvido pelo CSMS é propagado para
        qualquer StopTransaction enfileirado depois que referenciava
        esse mesmo ID local — sem essa correção, o CSMS receberia um
        StopTransaction para um transaction_id que nunca existiu do lado
        dele.

        Limitação conhecida: como não geramos IDs de mensagem próprios
        para deduplicação, se a conexão cair DEPOIS do CSMS já ter
        processado uma mensagem mas ANTES da confirmação chegar até nós,
        um reenvio no próximo flush pode registrar a mesma
        StartTransaction/MeterValues duas vezes do lado do servidor. O
        protocolo OCPP 1.6 não tem um mecanismo de idempotência embutido
        pra isso — é uma limitação real do protocolo, não só deste
        simulador.
        """
        state = self.state
        if not state.offline_queue:
            return

        queue = state.offline_queue
        state.offline_queue = []  # o que não for entregue volta pro final, abaixo
        self.log.info(
            f"[FILA OFFLINE] reconectado — reenviando {len(queue)} mensagem(ns) pendente(s)..."
        )
        local_to_real: dict[int, int] = {}

        for i, item in enumerate(queue):
            kind, request, local_tx_id = item["kind"], item["request"], item["local_tx_id"]

            # Corrige a referência de ID local -> real antes de enviar,
            # se já resolvida por um StartTransaction anterior nesta
            # mesma rodada de flush.
            if kind == "StopTransaction" and local_tx_id in local_to_real:
                request.transaction_id = local_to_real[local_tx_id]

            try:
                response = await asyncio.wait_for(
                    self.call(request), timeout=self.config.call_timeout_seconds
                )
            except (websockets.exceptions.ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                self.log.warning(
                    f"[FILA OFFLINE] conexão caiu de novo durante o flush ({exc!r}) — "
                    f"{len(queue) - i} mensagem(ns) voltam para a fila."
                )
                self.is_online = False
                state.offline_queue = queue[i:]  # este item + os que nem tentamos
                return

            self.log.info(f"[FILA OFFLINE] '{kind}' entregue com sucesso.")

            if kind == "StartTransaction" and local_tx_id is not None and response is not None:
                real_id = response.transaction_id
                local_to_real[local_tx_id] = real_id
                if state.active_transaction_id == local_tx_id:
                    state.active_transaction_id = real_id
                if self._pending_local_tx_id == local_tx_id:
                    self._pending_local_tx_id = None
                self.log.info(
                    f"[FILA OFFLINE] ID local {local_tx_id} resolvido para "
                    f"transaction_id real {real_id}"
                )

        self.log.info("[FILA OFFLINE] todas as mensagens pendentes foram entregues.")

    def _apply_offered_amps(self, offered_amps: float, source: str):
        """
        Aplica um novo limite de corrente oferecida e, se necessário,
        reflete a mudança num StatusNotification SuspendedEVSE/Charging.
        Extraído do handler de perfil original para ser reutilizável pelo
        agendador de múltiplos períodos (_run_charging_schedule) sem
        duplicar a lógica de suspensão.
        """
        state = self.state
        state.current_offered_amps = offered_amps
        state.current_actual_amps = compute_actual_current(
            offered_amps, state.battery_soc_percent
        )
        self.log.info(
            f"[{source}] limite oferecido={state.current_offered_amps}A | "
            f"corrente real (SoC {state.battery_soc_percent:.0f}%)={state.current_actual_amps}A"
        )

        # Reflete no StatusNotification quando o CSMS impõe 0A (ex: fila
        # de espera do balanceamento de site) ou restaura a corrente
        # depois — sem isso, o status ficava travado em "Charging" no
        # dashboard mesmo com a corrente zerada pelo CSMS, já que nada
        # mais dispararia uma StatusNotification nova nesse caso. Só
        # entra em jogo se houver sessão ativa e o carro não estiver
        # voluntariamente pausado (SuspendedEV tem prioridade — são
        # causas de suspensão diferentes).
        if state.active_transaction_id is not None and not state.session_suspended:
            if state.current_offered_amps <= 0.0 and not state.evse_suspended_by_profile:
                state.evse_suspended_by_profile = True
                self.log.info(f"[{source}] 0A imposto pelo CSMS → SuspendedEVSE")
                asyncio.create_task(self.send_status_notification(
                    ChargePointStatus.suspended_evse))
            elif state.current_offered_amps > 0.0 and state.evse_suspended_by_profile:
                state.evse_suspended_by_profile = False
                self.log.info(f"[{source}] corrente restaurada pelo CSMS → Charging")
                asyncio.create_task(self.send_status_notification(
                    ChargePointStatus.charging))

    async def _run_charging_schedule(self, periods: list, unit: str):
        """
        Percorre TODOS os períodos de um chargingSchedule, não só o
        primeiro. Antes, um perfil com múltiplos chargingSchedulePeriod
        (ex: 32A por 10min, depois cai pra 16A) era achatado no valor do
        primeiro período pra sessão inteira — um CSMS testando perfis com
        rampas/degraus nunca via o simulador de fato variar a corrente ao
        longo do tempo.

        Simplificação assumida: cada period["startPeriod"] é tratado como
        segundos relativos ao momento em que este SetChargingProfile foi
        recebido (não ao início da transação nem a um startSchedule
        absoluto do profile) — suficiente pra testar perfis com múltiplos
        degraus manualmente; perfis recorrentes (Daily/Weekly) e
        startSchedule absoluto não são interpretados de forma especial.
        """
        ordered = sorted(periods, key=lambda p: p.get("start_period", p.get("startPeriod", 0)))
        try:
            for i, period in enumerate(ordered):
                start_period = period.get("start_period", period.get("startPeriod", 0))
                amps = self._limit_to_amps(period["limit"], unit)
                self._apply_offered_amps(amps, source="PERFIL RECEBIDO")

                if i + 1 < len(ordered):
                    next_start = ordered[i + 1].get(
                        "start_period", ordered[i + 1].get("startPeriod", 0)
                    )
                    wait = max(0, next_start - start_period)
                    if wait > 0:
                        self.log.info(
                            f"[PERFIL RECEBIDO] período atual válido por {wait}s "
                            f"antes do próximo degrau do perfil"
                        )
                        await asyncio.sleep(wait)
        except asyncio.CancelledError:
            # Esperado sempre que um novo SetChargingProfile, um
            # ClearChargingProfile, ou o fim da sessão substitui este
            # agendamento antes que ele termine sozinho — não é um erro.
            pass

    @on(Action.set_charging_profile)
    async def on_set_charging_profile(self, connector_id, cs_charging_profiles, **kwargs):
        """
        Chamado quando o CSMS manda um novo perfil de carga (ex: limitar a
        10A, ou uma rampa de vários degraus). Aqui simulamos o charge
        point "aceitando" e agendando a aplicação de todos os períodos.
        """
        schedule = cs_charging_profiles["charging_schedule"]
        periods = schedule["charging_schedule_period"]
        unit = schedule.get("charging_rate_unit", schedule.get("chargingRateUnit", "A"))

        self._cancel_profile_task()

        if periods:
            self.log.info(
                f"[PERFIL RECEBIDO] connector={connector_id} | "
                f"{len(periods)} período(s) | unidade={unit}"
            )
            self._profile_task = asyncio.create_task(
                self._run_charging_schedule(periods, unit)
            )
        else:
            self.log.warning("SetChargingProfile recebido sem chargingSchedulePeriod")

        return call_result.SetChargingProfile(status="Accepted")

    @on(Action.clear_charging_profile)
    async def on_clear_charging_profile(self, **kwargs):
        """
        Remove o(s) perfil(is) ativo(s) e volta pro comportamento padrão:
        corrente padrão do simulador se houver sessão ativa, 0A caso
        contrário. Antes este Action não tinha handler nenhum — a
        biblioteca ocpp respondia um NotImplemented genérico pra
        qualquer CSMS que testasse esse fluxo, e mesmo que respondesse
        Accepted, current_offered_amps nunca era resetado.
        """
        self._cancel_profile_task()
        state = self.state

        fallback_amps = (
            self.config.default_offered_amps if state.active_transaction_id is not None else 0.0
        )
        self._apply_offered_amps(fallback_amps, source="PERFIL LIMPO")
        self.log.info(
            "[CLEAR CHARGING PROFILE] perfil removido — voltando à corrente "
            f"padrão ({fallback_amps:.0f}A)"
        )
        return call_result.ClearChargingProfile(status="Accepted")

    @on(Action.remote_start_transaction)
    async def on_remote_start_transaction(self, id_tag, connector_id=None, **kwargs):
        self.log.info(f"[REMOTE START] id_tag={id_tag} connector={connector_id}")
        state = self.state

        if state.availability_status == "Inoperative":
            self.log.warning(
                "[REMOTE START] conector Inoperative (ChangeAvailability) — recusando."
            )
            return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.rejected)
        if state.active_transaction_id is not None:
            self.log.warning("[REMOTE START] já existe sessão ativa — recusando.")
            return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.rejected)
        if state.is_faulted:
            self.log.warning("[REMOTE START] charger em Faulted — recusando.")
            return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.rejected)

        # Dispara o envio de StartTransaction em background, DEPOIS de responder
        # Accepted — replica o fluxo real: o carregador aceita o comando e só
        # manda StartTransaction como mensagem separada um instante depois
        # (após fechar o contator / autorizar localmente).
        asyncio.create_task(
            self._send_start_transaction(connector_id or self.config.connector_id, id_tag)
        )
        return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.accepted)

    @on(Action.remote_stop_transaction)
    async def on_remote_stop_transaction(self, transaction_id, **kwargs):
        self.log.info(f"[REMOTE STOP] transaction_id={transaction_id}")
        # Reason.remote é o motivo correto da OCPP para uma sessão encerrada
        # via comando remoto do CSMS (botão "Parar" no dashboard) — sem
        # isso, o campo "reason" ia como None/nulo, e o histórico de
        # sessões nunca mostrava motivo nenhum para o caso mais comum.
        asyncio.create_task(
            self._send_stop_transaction(transaction_id, reason=Reason.remote)
        )
        return call_result.RemoteStopTransaction(status=RemoteStartStopStatus.accepted)

    @on(Action.change_availability)
    async def on_change_availability(self, connector_id, type, **kwargs):
        """
        Antes era um stub: sempre respondia Accepted sem guardar nada,
        então o conector continuava aceitando sessões normalmente mesmo
        depois de um CSMS marcá-lo Inoperative — um teste de "coloquei o
        conector fora de serviço, motorista não deveria conseguir
        carregar" passava no CSMS e falhava silenciosamente aqui.

        Agora: Inoperative com sessão ativa -> Scheduled (aplicado só
        quando a sessão terminar, como manda o spec); sem sessão ativa
        -> aplica na hora e manda StatusNotification Unavailable.
        Operative sempre aplica na hora (cancela um Scheduled pendente,
        se houver) e volta a Available.
        """
        self.log.info(f"[CHANGE AVAILABILITY] connector={connector_id} type={type}")
        state = self.state

        if type == AvailabilityType.inoperative:
            if state.active_transaction_id is not None:
                state.pending_availability_change = "Inoperative"
                self.log.info(
                    "[CHANGE AVAILABILITY] sessão ativa — mudança para "
                    "Inoperative agendada para quando a sessão terminar."
                )
                return call_result.ChangeAvailability(status=AvailabilityStatus.scheduled)

            state.availability_status = "Inoperative"
            asyncio.create_task(self.send_status_notification(ChargePointStatus.unavailable))
            return call_result.ChangeAvailability(status=AvailabilityStatus.accepted)

        # Operative
        state.pending_availability_change = None
        state.availability_status = "Operative"
        if state.active_transaction_id is None and not state.is_faulted:
            asyncio.create_task(self.send_status_notification(ChargePointStatus.available))
        return call_result.ChangeAvailability(status=AvailabilityStatus.accepted)

    @on(Action.reset)
    async def on_reset(self, type, **kwargs):
        """
        Comportamento real de um Reset (soft ou hard) num carregador AC:
        se houver sessão ativa, ela é interrompida (StopTransaction com
        motivo SoftReset/HardReset) e o contator abre — não tem como o
        carregador continuar entregando corrente depois de reiniciar.

        Soft reset: reinicia o software sem cortar a alimentação —
        simulamos como uma interrupção breve, voltando a Available rápido.
        Hard reset: equivalente a desligar e religar fisicamente — simula
        um período maior de indisponibilidade (Unavailable) representando
        o boot do firmware, antes de voltar a Available.
        """
        self.log.info(f"[RESET] type={type}")
        is_hard = (type == ResetType.hard)
        reason = Reason.hard_reset if is_hard else Reason.soft_reset

        active_id = self.state.active_transaction_id
        if active_id is not None:
            self.log.info(
                f"[RESET] sessão ativa (tx={active_id}) será "
                f"interrompida pelo reset"
            )
            asyncio.create_task(self._handle_reset_flow(active_id, reason, is_hard))
        else:
            asyncio.create_task(self._handle_reset_flow(None, reason, is_hard))

        return call_result.Reset(status="Accepted")

    async def _handle_reset_flow(self, transaction_id, reason, is_hard: bool):
        """Executa a sequência de reset em background, após responder Accepted."""
        if transaction_id is not None:
            # skip_status_flow=True porque o reset tem sua própria sequência
            # de status abaixo (não o Finishing->Available padrão de um stop normal).
            await self._send_stop_transaction(
                transaction_id, reason=reason, skip_status_flow=True
            )

        if is_hard:
            # Hard reset: simula o carregador caindo (Unavailable) durante
            # o boot do firmware antes de voltar a responder normalmente.
            await self.send_status_notification(ChargePointStatus.unavailable)
            self.log.info("[RESET] hard reset — simulando reboot do firmware (5s)...")
            await asyncio.sleep(5)
            await self.send_boot_notification()
            await asyncio.sleep(1)
        else:
            self.log.info("[RESET] soft reset — reinício rápido do software (1s)...")
            await asyncio.sleep(1)

        await self.send_status_notification(ChargePointStatus.available)
        self.log.info("[RESET] concluído — carregador disponível novamente")

    @on(Action.trigger_message)
    async def on_trigger_message(self, requested_message, connector_id=None, **kwargs):
        """
        TriggerMessage pede para o carregador reenviar uma mensagem
        espontaneamente (ex: StatusNotification, Heartbeat). Usado pelo
        status_check() do CSMS real para forçar uma atualização de estado.
        """
        self.log.info(f"[TRIGGER MESSAGE] requested={requested_message} connector={connector_id}")
        if requested_message == "StatusNotification":
            current_status = (
                ChargePointStatus.charging if self.state.active_transaction_id is not None
                else ChargePointStatus.available
            )
            asyncio.create_task(self.send_status_notification(current_status))
        elif requested_message == "Heartbeat":
            asyncio.create_task(self.call(call.Heartbeat()))
        elif requested_message == "MeterValues":
            pass  # já é enviado periodicamente pelo loop normal
        return call_result.TriggerMessage(status="Accepted")

    @on(Action.get_configuration)
    async def on_get_configuration(self, key=None, **kwargs):
        """
        Retorna um conjunto básico de configurações, simulando o que um
        charger AC real reportaria. Ajuste/expanda essas chaves se seu
        CSMS depender de valores específicos.

        IMPORTANTE: HeartbeatInterval é reportado a partir do valor REAL
        em uso (state.current_heartbeat_interval), não um número fixo. O
        charger.py do CSMS tem um sync loop (start_sync_loop) que roda a
        cada 60s, chama GetConfiguration, e SOBRESCREVE self.st.heartbeat_interval
        com o que vier aqui — se este handler sempre respondesse um valor
        fixo (ex: "30"), qualquer mudança feita via ChangeConfiguration
        seria silenciosamente revertida no próximo ciclo de sync, mesmo
        que o loop de heartbeat deste simulador estivesse rodando no
        intervalo certo por debaixo. Foi exatamente esse o bug relatado.
        """
        # DEBUG, não INFO: o sync loop do CSMS (start_sync_loop em
        # charger.py) chama GetConfiguration a cada 60s pra sincronizar
        # HeartbeatInterval e o limite físico — mesmo padrão de ruído
        # periódico do Heartbeat, sem informação nova na maioria dos
        # ciclos. Só aparece no terminal com --verbose.
        self.log.debug(f"[GET CONFIGURATION] keys solicitadas={key}")
        all_config = [
            {"key": "HeartbeatInterval", "readonly": False,
             "value": str(self.state.current_heartbeat_interval)},
            {"key": "MeterValueSampleInterval", "readonly": False,
             "value": str(self.config.meter_values_interval)},
            {"key": "ConnectorPhaseRotation", "readonly": True, "value": "NotApplicable"},
            {"key": "NumberOfConnectors", "readonly": True, "value": "1"},
            # Atualizado para incluir Reservation, LocalAuthListManagement e
            # FirmwareManagement — ficou desatualizado (só "Core,SmartCharging")
            # depois que os handlers dessas Actions foram implementados, o que
            # fazia o simulador se anunciar com menos capacidades do que
            # realmente suporta para um CSMS que consulta isso antes de decidir
            # o que enviar.
            {"key": "SupportedFeatureProfiles", "readonly": True,
             "value": "Core,SmartCharging,Reservation,LocalAuthListManagement,FirmwareManagement"},
            {"key": "LocalAuthListEnabled", "readonly": False, "value": "true"},
            {"key": "LocalAuthListMaxLength", "readonly": True, "value": "100"},
            {"key": "SendLocalListMaxLength", "readonly": True, "value": "20"},
            # Reserva por conector (não por charge point inteiro / conector 0).
            {"key": "ReserveConnectorZeroSupported", "readonly": True, "value": "false"},
            {"key": "AvailabilityStatus", "readonly": True,
             "value": self.state.availability_status},
        ]
        if key:
            # CSMS pediu chaves específicas: filtra e reporta as desconhecidas
            requested_keys = {k.lower() for k in key}
            found = [c for c in all_config if c["key"].lower() in requested_keys]
            unknown = [k for k in key if k.lower() not in {c["key"].lower() for c in all_config}]
            return call_result.GetConfiguration(configuration_key=found, unknown_key=unknown)
        return call_result.GetConfiguration(configuration_key=all_config, unknown_key=[])

    @on(Action.change_configuration)
    async def on_change_configuration(self, key, value, **kwargs):
        self.log.info(f"[CHANGE CONFIGURATION] key={key} value={value}")

        if key == "HeartbeatInterval":
            try:
                self.state.current_heartbeat_interval = int(value)
                self.log.info(
                    f"[HEARTBEAT] intervalo atualizado para "
                    f"{self.state.current_heartbeat_interval}s — efeito no próximo ciclo"
                )
            except ValueError:
                self.log.warning(f"[CHANGE CONFIGURATION] valor inválido para HeartbeatInterval: {value}")
                return call_result.ChangeConfiguration(status="Rejected")
        # Outras chaves (ex: MeterValueSampleInterval) são aceitas mas não
        # têm efeito simulado — o intervalo de MeterValues deste simulador
        # é fixo via config.meter_values_interval no boot, já que não é
        # esse o foco do bug reportado. Expanda aqui se precisar testar
        # mudança desse valor especificamente.

        return call_result.ChangeConfiguration(status="Accepted")

    @on(Action.unlock_connector)
    async def on_unlock_connector(self, connector_id, **kwargs):
        """
        Comando do operador (dashboard "Destravar conector") para liberar
        o conector mecanicamente — ex: motorista esqueceu o cabo travado
        e precisa de ajuda remota para soltá-lo. Não tinha handler antes;
        a lib respondia um NotImplemented genérico pra qualquer CSMS que
        testasse esse fluxo.
        """
        self.log.info(f"[UNLOCK CONNECTOR] connector={connector_id}")
        if self.state.active_transaction_id is not None:
            # Comportamento simplificado: um charger físico real pode
            # recusar (UnlockFailed) se o EV ainda estiver puxando
            # corrente, ou pode destravar mesmo assim dependendo do
            # hardware. Aqui só avisamos no log e reportamos sucesso —
            # não paramos a sessão automaticamente, já que UnlockConnector
            # não é, por si só, um pedido de StopTransaction.
            self.log.warning(
                "[UNLOCK CONNECTOR] há uma sessão ativa — destravando o "
                "conector sem encerrar a sessão (comportamento simplificado)."
            )
        return call_result.UnlockConnector(status=UnlockStatus.unlocked)

    @on(Action.data_transfer)
    async def on_data_transfer(self, vendor_id, message_id=None, data=None, **kwargs):
        """
        Extensão vendor-specific do OCPP — usada para testar payloads
        fora do schema padrão sem precisar de uma Action nova. Este
        simulador só reconhece seu próprio vendor_id (echo, útil para
        confirmar que o transporte ida-e-volta funciona); qualquer outro
        vendor_id recebe UnknownVendorId, como manda o spec.
        """
        self.log.info(
            f"[DATA TRANSFER] recebido | vendor_id={vendor_id} "
            f"message_id={message_id} data={data!r}"
        )
        if vendor_id != "EVChargerSim":
            return call_result.DataTransfer(status=DataTransferStatus.unknown_vendor_id)
        return call_result.DataTransfer(status=DataTransferStatus.accepted, data=data)

    @on(Action.get_diagnostics)
    async def on_get_diagnostics(self, location, **kwargs):
        """
        CSMS pedindo upload de um arquivo de diagnóstico (logs internos).
        Simulamos o nome do arquivo e a sequência de status
        (Uploading -> Uploaded) sem de fato subir nada para `location` —
        suficiente pra testar se o CSMS reage certo às notificações.
        """
        file_name = f"diagnostics_{self.config.charge_point_id}_{int(datetime.now(timezone.utc).timestamp())}.zip"
        self.log.info(f"[GET DIAGNOSTICS] location={location} | arquivo simulado: {file_name}")
        asyncio.create_task(self._simulate_diagnostics_upload())
        return call_result.GetDiagnostics(file_name=file_name)

    async def _simulate_diagnostics_upload(self):
        await asyncio.sleep(1)
        await self.call(call.DiagnosticsStatusNotification(status=DiagnosticsStatus.uploading))
        self.log.info("[DIAGNOSTICS] status: Uploading")
        await asyncio.sleep(2)
        await self.call(call.DiagnosticsStatusNotification(status=DiagnosticsStatus.uploaded))
        self.log.info("[DIAGNOSTICS] status: Uploaded")

    @on(Action.update_firmware)
    async def on_update_firmware(self, location, retrieve_date, **kwargs):
        """
        CSMS mandando atualizar o firmware. Um update de firmware real
        interrompe qualquer sessão ativa (o charger reinicia no fim) —
        replicamos isso encerrando a transação antes da sequência de
        download/instalação, igual ao hard reset.
        """
        self.log.info(f"[UPDATE FIRMWARE] location={location} retrieve_date={retrieve_date}")
        asyncio.create_task(self._simulate_firmware_update())
        return call_result.UpdateFirmware()

    async def _simulate_firmware_update(self):
        state = self.state
        if state.active_transaction_id is not None:
            self.log.warning(
                f"[FIRMWARE] sessão ativa (tx={state.active_transaction_id}) será "
                "encerrada — o firmware update vai reiniciar o charger."
            )
            await self._send_stop_transaction(
                state.active_transaction_id, reason=Reason.other, skip_status_flow=True
            )

        for status, delay in (
            (FirmwareStatus.downloading, 1),
            (FirmwareStatus.downloaded, 1),
            (FirmwareStatus.installing, 1),
        ):
            await self.call(call.FirmwareStatusNotification(status=status))
            self.log.info(f"[FIRMWARE] status: {status.value}")
            await asyncio.sleep(delay)

        # Reboot simulado, mesma sequência do hard reset.
        await self.send_status_notification(ChargePointStatus.unavailable)
        await asyncio.sleep(3)
        await self.send_boot_notification()
        await asyncio.sleep(1)
        await self.send_status_notification(ChargePointStatus.available)

        await self.call(call.FirmwareStatusNotification(status=FirmwareStatus.installed))
        self.log.info("[FIRMWARE] status: Installed — atualização concluída")

    @on(Action.reserve_now)
    async def on_reserve_now(
        self, connector_id, expiry_date, id_tag, reservation_id, parent_id_tag=None, **kwargs
    ):
        """
        Reserva o conector para um id_tag (ou grupo, via parent_id_tag)
        específico até expiry_date. Enquanto reservado, "start" local só
        aceita esse id_tag — ver console_command_loop.
        """
        state = self.state
        self.log.info(
            f"[RESERVE NOW] connector={connector_id} id_tag={id_tag} "
            f"reservation_id={reservation_id} expiry={expiry_date}"
        )

        if state.is_faulted:
            return call_result.ReserveNow(status=ReservationStatus.faulted)
        if state.active_transaction_id is not None or state.reservation_id is not None:
            self.log.warning(
                "[RESERVE NOW] conector já ocupado (sessão ativa ou já "
                "reservado) — rejeitando com Occupied."
            )
            return call_result.ReserveNow(status=ReservationStatus.occupied)

        state.reservation_id = reservation_id
        state.reserved_for_id_tag = id_tag
        state.reserved_parent_id_tag = parent_id_tag
        asyncio.create_task(self.send_status_notification(ChargePointStatus.reserved))
        asyncio.create_task(self._expire_reservation_at(reservation_id, expiry_date))
        return call_result.ReserveNow(status=ReservationStatus.accepted)

    async def _expire_reservation_at(self, reservation_id: int, expiry_date: str):
        """
        Limpa a reserva sozinha quando expiry_date passa, sem precisar de
        um CancelReservation explícito — replica o comportamento real de
        uma reserva não usada expirar e o conector voltar a Available.
        """
        try:
            expiry = datetime.fromisoformat(expiry_date.replace("Z", "+00:00"))
            delay = (expiry - datetime.now(timezone.utc)).total_seconds()
        except ValueError:
            self.log.warning(
                f"[RESERVE NOW] expiry_date inválido/não-ISO8601 ('{expiry_date}') — "
                "reserva não expira automaticamente, só via CancelReservation."
            )
            return

        if delay > 0:
            await asyncio.sleep(delay)

        state = self.state
        if state.reservation_id == reservation_id:
            self.log.info(f"[RESERVE NOW] reserva {reservation_id} expirou sem uso")
            state.reservation_id = None
            state.reserved_for_id_tag = None
            state.reserved_parent_id_tag = None
            if state.active_transaction_id is None and not state.is_faulted:
                await self.send_status_notification(ChargePointStatus.available)

    @on(Action.cancel_reservation)
    async def on_cancel_reservation(self, reservation_id, **kwargs):
        state = self.state
        self.log.info(f"[CANCEL RESERVATION] reservation_id={reservation_id}")
        if state.reservation_id != reservation_id:
            return call_result.CancelReservation(status=CancelReservationStatus.rejected)

        state.reservation_id = None
        state.reserved_for_id_tag = None
        state.reserved_parent_id_tag = None
        if state.active_transaction_id is None and not state.is_faulted:
            asyncio.create_task(self.send_status_notification(ChargePointStatus.available))
        return call_result.CancelReservation(status=CancelReservationStatus.accepted)

    @on(Action.get_local_list_version)
    async def on_get_local_list_version(self, **kwargs):
        self.log.debug(f"[GET LOCAL LIST VERSION] atual={self.state.local_list_version}")
        return call_result.GetLocalListVersion(list_version=self.state.local_list_version)

    @on(Action.send_local_list)
    async def on_send_local_list(
        self, list_version, update_type, local_authorization_list=None, **kwargs
    ):
        """
        Recebe (parte d)a lista local de autorização do CSMS. "Full"
        substitui a lista inteira; "Differential" aplica só as entradas
        enviadas (uma entrada sem id_tag_info remove aquele id_tag da
        lista — comportamento padrão OCPP 1.6 para remoção diferencial).
        """
        state = self.state
        entries = local_authorization_list or []

        if update_type == "Full":
            state.local_auth_list = {}

        for entry in entries:
            entry_id_tag = entry.get("id_tag") or entry.get("idTag")
            id_tag_info = entry.get("id_tag_info") or entry.get("idTagInfo")
            if not entry_id_tag:
                continue
            if id_tag_info is None:
                state.local_auth_list.pop(entry_id_tag, None)
                continue
            state.local_auth_list[entry_id_tag] = id_tag_info.get("status", "Accepted")

        state.local_list_version = list_version
        self.log.info(
            f"[SEND LOCAL LIST] update_type={update_type} | "
            f"nova versão={list_version} | {len(state.local_auth_list)} id_tag(s) na lista"
        )
        return call_result.SendLocalList(status=UpdateStatus.accepted)

    # --------------------------------------------------------
    # Rotinas que o charge point envia PARA o CSMS
    # --------------------------------------------------------

    async def send_boot_notification(self):
        """
        IMPORTANTE: não reseta mais SoC/is_faulted aqui. Antes fazia
        sentido porque uma nova conexão sempre significava uma instância
        nova de EVChargerSim — mas agora a MESMA instância persiste
        através de reconexões (ver main()), especificamente para que uma
        sessão em andamento (ou uma falha real) sobreviva a uma queda de
        rede em vez de ser silenciosamente apagada. Resetar esses campos
        aqui destruiria justamente o estado que a fila offline existe
        para preservar.

        Não é enfileirável (queueable=False): não faz sentido acumular
        BootNotifications na fila — se ficarmos offline, isso já é
        tratado pelo laço de reconexão em main(), que manda um novo
        BootNotification assim que a conexão volta.
        """
        request = call.BootNotification(
            charge_point_model="EVChargerSim",
            charge_point_vendor="EVChargerSim",
            firmware_version="SIM-1.0",
        )
        response = await self._call_or_queue(request, kind="BootNotification", queueable=False)
        if response is None:
            return
        if response.status == RegistrationStatus.accepted:
            self.log.info("BootNotification aceito pelo CSMS.")
        else:
            self.log.warning(f"BootNotification respondido com status: {response.status}")

    async def send_status_notification(self, status: str):
        request = call.StatusNotification(
            connector_id=self.config.connector_id,
            error_code=ChargePointErrorCode.no_error,
            status=status,
        )
        response = await self._call_or_queue(request, kind=f"StatusNotification({status})")
        if response is not None:
            self.log.info(f"StatusNotification enviado: {status}")

    async def _send_start_transaction(self, connector_id: int, id_tag: str):
        """
        Envia StartTransaction simulando o carregador autorizando e fechando
        o contator. Se estiver offline (ou a mensagem for perdida por
        chaos), a sessão roda localmente do mesmo jeito — um EV já
        autorizado (ex: via lista local) continua carregando fisicamente
        mesmo sem conexão — com um ID de transação temporário (negativo)
        até o CSMS confirmar um ID real no próximo flush da fila offline.
        """
        state = self.state
        try:
            # Cancela qualquer agendamento de perfil de uma sessão
            # anterior — sem isso, uma _run_charging_schedule pendente
            # (ex: perfil de 3 degraus que não tinha terminado quando a
            # sessão anterior encerrou) podia "acordar" no meio desta
            # nova sessão e pisar na corrente que ela acabou de aplicar.
            self._cancel_profile_task()

            # Cada nova sessão reseta SoC e medidor, evitando que sessões
            # sucessivas encadeiem o estado da sessão anterior.
            state.battery_soc_percent = self.config.initial_soc_percent
            state.energy_meter_wh = 0.0
            state.session_suspended = False
            self.log.info(f"[BATERIA] SoC inicial desta sessão: {state.battery_soc_percent:.1f}%")

            # Aplica a corrente padrão residencial imediatamente, antes de
            # qualquer SetChargingProfile chegar do CSMS. Sem isso, a sessão
            # começa em 0A e fica sem acumular energia até o CSMS reagir —
            # o que é artificial, pois um carregador físico começa a entregar
            # corrente assim que o contator fecha. O CSMS ainda pode sobrescrever
            # este valor com SetChargingProfile a qualquer momento.
            state.current_offered_amps = self.config.default_offered_amps
            state.current_actual_amps = compute_actual_current(
                state.current_offered_amps, state.battery_soc_percent
            )
            self.log.info(
                f"[SESSION] Corrente inicial: {state.current_offered_amps:.0f}A oferecido "
                f"/ {state.current_actual_amps:.1f}A real (aguardando SetChargingProfile do CSMS)"
            )

            # Simula o veículo sendo conectado e o carregador preparando
            # a sessão (LED branco piscando, conector travado etc). Se
            # estivermos offline, send_status_notification já enfileira
            # isso sozinho — não precisamos de tratamento especial aqui.
            await self.send_status_notification(ChargePointStatus.preparing)
            await asyncio.sleep(1)  # simula o pequeno delay real de fechamento do contator

            # Reserva um ID local para esta sessão ANTES de tentar enviar —
            # se a mensagem for enfileirada por qualquer motivo (offline,
            # timeout, chaos), já temos um ID consistente pra usar.
            self._local_tx_counter -= 1
            local_id = self._local_tx_counter

            request = call.StartTransaction(
                connector_id=connector_id,
                id_tag=id_tag,
                meter_start=int(state.energy_meter_wh),
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            # queue_on_timeout=True: mesmo um simples "CSMS não respondeu"
            # (sem a conexão necessariamente ter caído) ainda assim
            # enfileira — não dá pra simplesmente desistir de registrar
            # uma transação que fisicamente já está em andamento.
            response = await self._call_or_queue(
                request,
                kind="StartTransaction",
                queueable=True,
                queue_on_timeout=True,
                local_tx_id=local_id,
            )

            if response is not None:
                # Confirmado na hora — fluxo normal, online.
                state.active_transaction_id = response.transaction_id
                self.log.info(
                    f"⚡ [START TRANSACTION] aceito pelo CSMS | "
                    f"transaction_id={state.active_transaction_id} | id_tag={id_tag}"
                )
            else:
                # _call_or_queue com queueable=True + queue_on_timeout=True
                # cobre TODO caminho de falha (offline, timeout, chaos) —
                # response is None aqui sempre significa que foi
                # enfileirado. A sessão já está "fisicamente" rodando do
                # lado do carro; só a confirmação do CSMS que fica pendente.
                state.active_transaction_id = local_id
                self._pending_local_tx_id = local_id
                self.log.warning(
                    f"[FILA OFFLINE] StartTransaction enfileirado — sessão "
                    f"rodando localmente com ID temporário {local_id} até "
                    "reconectar e confirmar com o CSMS."
                )

            # Uma sessão que começa consome a reserva do conector, se
            # houver uma — um charger físico real libera a reserva assim
            # que o id_tag correto é usado, não só quando ela expira.
            if state.reservation_id is not None:
                self.log.info(
                    f"[SESSION] reserva {state.reservation_id} consumida pelo início desta sessão"
                )
                state.reservation_id = None
                state.reserved_for_id_tag = None
                state.reserved_parent_id_tag = None

            # Nota: não definimos uma corrente "chute inicial" aqui além da
            # já aplicada acima. O CSMS real já envia um SetChargingProfile
            # logo após o boot — é esse comando que vai popular
            # current_offered_amps de forma correta, refletindo o limite
            # configurado de verdade.

            await self.send_status_notification(ChargePointStatus.charging)
        except Exception:
            # Cobre qualquer coisa inesperada fora do fluxo normal de
            # offline/timeout já tratado acima (esse já não propaga mais
            # exceção — só entra aqui algo genuinamente imprevisto). Sem
            # isso, uma falha aqui morre silenciosamente — a task roda em
            # segundo plano via create_task e ninguém nunca dá "await"
            # nela para propagar o erro.
            self.log.exception(
                "[START TRANSACTION] erro inesperado — sessão pode não ter "
                "sido registrada corretamente."
            )

    async def _send_stop_transaction(
        self,
        transaction_id: int,
        reason=None,
        skip_status_flow: bool = False,
    ):
        """
        Envia StopTransaction encerrando a sessão no CSMS.

        reason: motivo OCPP do encerramento (ocpp.v16.enums.Reason). Usado
        quando o stop não vem de um RemoteStopTransaction normal — ex:
        Reason.hard_reset / Reason.soft_reset quando a sessão é
        interrompida por um comando de Reset.

        skip_status_flow: quando True, não manda Finishing->Available
        automaticamente (usado pelo hard reset, que tem sua própria
        sequência de status simulando o reboot do firmware).
        """
        state = self.state
        # Cancela o agendamento de perfil ativo — sem sessão, não faz
        # sentido continuar aplicando degraus de corrente de um perfil
        # cujo alvo (a transação) acabou de encerrar.
        self._cancel_profile_task()

        # A sessão para FISICAMENTE agora, mesmo que o CSMS ainda não
        # saiba (ex: offline) — replica um charger real, que abre o
        # contator na hora e só avisa o servidor depois, quando puder.
        # Isso também é o que torna a fila offline coerente: se
        # continuássemos "carregando" até a confirmação chegar, um
        # StopTransaction enfileirado não faria sentido nenhum.
        local_id_being_stopped = transaction_id if transaction_id is not None and transaction_id < 0 else None
        state.active_transaction_id = None
        state.current_offered_amps = 0.0
        state.current_actual_amps = 0.0
        state.session_suspended = False
        state.evse_suspended_by_profile = False

        try:
            await asyncio.sleep(0.5)

            request = call.StopTransaction(
                meter_stop=int(state.energy_meter_wh),
                timestamp=datetime.now(timezone.utc).isoformat(),
                transaction_id=transaction_id,
                reason=reason,
            )
            # queue_on_timeout=True pelo mesmo motivo do
            # _send_start_transaction: a sessão já parou de verdade, não
            # dá pra simplesmente desistir de avisar o CSMS. Se
            # `transaction_id` ainda é um ID local (StartTransaction
            # correspondente também ainda não confirmado), local_tx_id
            # aqui permite ao flush da fila corrigir a referência depois.
            response = await self._call_or_queue(
                request,
                kind="StopTransaction",
                queueable=True,
                queue_on_timeout=True,
                local_tx_id=local_id_being_stopped,
            )
            if response is not None:
                self.log.info(
                    f"🛑 [STOP TRANSACTION] enviado | transaction_id={transaction_id}"
                    + (f" | motivo={reason.value}" if reason else "")
                )
            else:
                self.log.warning(
                    f"[FILA OFFLINE] StopTransaction enfileirado "
                    f"(transaction_id={transaction_id}) — será entregue ao "
                    "CSMS na próxima reconexão."
                )

            if skip_status_flow:
                # Usado por reset/fault/firmware, que têm sua própria
                # sequência final de status. Uma mudança de disponibilidade
                # agendada durante a sessão não é aplicada aqui para não
                # brigar com essa sequência própria — só avisamos no log
                # que ela ficou pendente, em vez de aplicá-la silenciosamente
                # e ela ser sobrescrita um instante depois pelo Available
                # final do reset.
                if state.pending_availability_change is not None:
                    self.log.warning(
                        "[CHANGE AVAILABILITY] mudança para Inoperative estava "
                        "agendada, mas a sessão terminou via reset/fault/firmware "
                        "(sequência de status própria) — reenvie ChangeAvailability "
                        "se ainda quiser aplicá-la."
                    )
                    state.pending_availability_change = None
                return

            # Uma mudança para Inoperative pedida durante esta sessão só é
            # aplicada agora que ela terminou — ver on_change_availability.
            if state.pending_availability_change == "Inoperative":
                state.availability_status = "Inoperative"
                state.pending_availability_change = None
                self.log.info(
                    "[CHANGE AVAILABILITY] aplicando mudança para Inoperative "
                    "agendada, agora que a sessão terminou."
                )
                await self.send_status_notification(ChargePointStatus.unavailable)
                return

            # Sequência realista de encerramento: Finishing (carregador
            # liberando o conector / EV ainda fisicamente plugado por um
            # instante) e, pouco depois, Available (pronto para o próximo
            # veículo). Sem isso, o conector ficava "preso" em Charging
            # mesmo sem nenhuma sessão ativa, e o MeterValues continuava
            # sendo reportado como se ainda houvesse carregamento.
            await self.send_status_notification(ChargePointStatus.finishing)
            await asyncio.sleep(2)
            await self.send_status_notification(ChargePointStatus.available)
        except Exception:
            # Cobre algo genuinamente inesperado fora do fluxo de
            # offline/timeout já tratado acima. O estado local já foi
            # limpo no início da função — o que pode ficar pendente aqui
            # é só a sequência de status pós-stop (Finishing/Available),
            # não a integridade da sessão em si.
            self.log.exception(
                "[STOP TRANSACTION] erro inesperado após a sessão já ter "
                "sido encerrada localmente."
            )

    async def energy_accumulator_loop(self, interval_seconds: int = 30):
        """
        Acumula energia (Wh) enquanto há transação ativa e não suspensa.
        A cada ciclo avança o SoC e recalcula a corrente (tapering).
        Ao atingir 100%, manda StopTransaction automaticamente — simula o
        EV sinalizando que não aceita mais carga (BMS cheio).

        config.simulation_speed multiplica o delta de energia por ciclo —
        note que isso só acelera o acúmulo de energia/SoC, o intervalo
        real entre ciclos (interval_seconds) não muda, então o
        MeterValues continua sendo reportado no cadência OCPP normal;
        só o quanto de energia é somado por ciclo é que anda mais rápido.
        Antes esse fator existia como constante (SIMULATION_SPEED) mas
        nunca era de fato aplicado em lugar nenhum — sessões "aceleradas"
        não tinham efeito algum na prática.

        Este loop agora é iniciado UMA VEZ (em main()) e roda para
        sempre, independente de quedas/reconexões — continuar
        "carregando" fisicamente mesmo offline é justamente o que torna
        a fila offline coerente. Por isso cada ciclo tem seu próprio
        try/except: um erro isolado não pode derrubar a simulação de
        carregamento para sempre.
        """
        state = self.state
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                if state.active_transaction_id is None:
                    continue
                if state.session_suspended or state.current_actual_amps <= 0:
                    continue

                power_w = self.config.nominal_voltage * state.current_actual_amps
                energy_delta_wh = (
                    power_w * (interval_seconds / 3600) * self.config.simulation_speed
                )
                state.energy_meter_wh += energy_delta_wh

                state.battery_soc_percent = min(
                    100.0,
                    state.battery_soc_percent
                    + (energy_delta_wh / self.config.battery_capacity_wh) * 100,
                )
                state.current_actual_amps = compute_actual_current(
                    state.current_offered_amps, state.battery_soc_percent
                )

                if state.battery_soc_percent >= 100.0:
                    state.current_actual_amps = 0.0
                    self.log.info(
                        "[BATERIA] SoC atingiu 100% — EV sinalizou bateria cheia. "
                        "Encerrando sessão automaticamente (Reason.ev_disconnected)."
                    )
                    asyncio.create_task(
                        self._send_stop_transaction(
                            state.active_transaction_id, reason=Reason.ev_disconnected
                        )
                    )
            except Exception:
                self.log.exception(
                    "[BATERIA] erro inesperado no acumulador de energia — "
                    "continuando no próximo ciclo."
                )

    async def send_heartbeat_loop(self):
        """
        O intervalo usado é state.current_heartbeat_interval, relido a
        cada ciclo — assim, uma mudança de HeartbeatInterval feita via
        CSMS (on_change_configuration) tem efeito já no próximo
        heartbeat, sem precisar reiniciar o simulador.

        Roda para sempre (iniciado uma vez em main()) — Heartbeat não é
        enfileirável (queueable=False): enquanto offline, cada ciclo só
        é pulado silenciosamente (em DEBUG), sem acumular na fila —
        reenviar heartbeats "atrasados" depois de reconectar não tem
        valor nenhum, o próximo heartbeat ao vivo já resolve.
        """
        while True:
            try:
                response = await self._call_or_queue(
                    call.Heartbeat(), kind="Heartbeat", queueable=False
                )
                if response is not None:
                    # DEBUG, não INFO: essa linha nunca traz informação
                    # nova (é literalmente "ainda estou vivo" a cada
                    # ciclo) — só aparece no terminal com --verbose.
                    self.log.debug(
                        f"Heartbeat enviado (intervalo atual: "
                        f"{self.state.current_heartbeat_interval}s)."
                    )
            except Exception:
                self.log.exception("[HEARTBEAT] erro inesperado — continuando no próximo ciclo.")
            await asyncio.sleep(self.state.current_heartbeat_interval)

    async def send_meter_values_loop(self, interval_seconds: int = 30):
        """
        Manda MeterValues periodicamente reportando a corrente "real" simulada.
        Isso é o que vai aparecer no seu dashboard como se fosse o charger reportando.

        Roda para sempre (iniciado uma vez em main()) — enquanto offline,
        cada MeterValues é enfileirado (queueable=True, o padrão) e
        entregue ao CSMS em ordem na próxima reconexão, preservando o
        histórico de carregamento mesmo durante a queda.
        """
        state = self.state
        while True:
            try:
                timestamp = datetime.now(timezone.utc).isoformat()
                voltage_now = read_grid_voltage(self.config.nominal_voltage)
                request = call.MeterValues(
                    connector_id=self.config.connector_id,
                    meter_value=[
                        {
                            "timestamp": timestamp,
                            "sampledValue": [
                                {
                                    "value": str(state.current_actual_amps),
                                    "context": "Sample.Periodic",
                                    "measurand": "Current.Import",
                                    "unit": "A",
                                },
                                {
                                    "value": str(state.current_offered_amps),
                                    "context": "Sample.Periodic",
                                    "measurand": "Current.Offered",
                                    "unit": "A",
                                },
                                {
                                    "value": str(voltage_now),
                                    "context": "Sample.Periodic",
                                    "measurand": "Voltage",
                                    "unit": "V",
                                },
                                {
                                    "value": str(round(voltage_now * state.current_actual_amps, 1)),
                                    "context": "Sample.Periodic",
                                    "measurand": "Power.Active.Import",
                                    "unit": "W",
                                },
                                {
                                    "value": str(int(state.energy_meter_wh)),
                                    "context": "Sample.Periodic",
                                    "measurand": "Energy.Active.Import.Register",
                                    "unit": "Wh",
                                },
                            ],
                        }
                    ],
                )
                await self._call_or_queue(request, kind="MeterValues")

                power_kw = round((voltage_now * state.current_actual_amps) / 1000, 2)
                energy_kwh = round(state.energy_meter_wh / 1000, 2)

                has_session = state.active_transaction_id is not None
                suspended = state.session_suspended or state.evse_suspended_by_profile
                color = _meter_line_color(has_session, suspended, state.is_faulted, self.use_color)
                offline_marker = " 📡✗" if not self.is_online else ""
                reset = "\033[0m" if self.use_color else ""

                # INFO (visível por padrão) — diferente do Heartbeat, esta é a
                # única linha que mostra o que está de fato acontecendo com a
                # sessão (SoC, corrente, potência), então vale ficar visível
                # sem precisar de --verbose.
                if has_session:
                    self.log.info(
                        f"{color}🔋 SoC {state.battery_soc_percent:5.1f}%  "
                        f"⚡ {state.current_actual_amps:4.1f}/{state.current_offered_amps:4.1f}A  "
                        f"{power_kw:5.2f}kW  Σ{energy_kwh:6.2f}kWh{offline_marker}{reset}"
                    )
                else:
                    self.log.info(f"{color}🔋 sem sessão ativa{offline_marker}{reset}")
            except Exception:
                self.log.exception(
                    "[METER VALUES] erro inesperado — continuando no próximo ciclo."
                )

            await asyncio.sleep(interval_seconds)

    async def console_command_loop(self):
        """
        Lê comandos do terminal em background (via run_in_executor para não
        bloquear o event loop) e simula ações locais do motorista/carro —
        eventos que nunca chegam via CSMS, mas que um charger físico real
        geraria sozinho.
        """
        state = self.state
        loop = asyncio.get_running_loop()
        self.log.info(
            "[CONSOLE] Pronto. Comandos: start <id_tag> | stop | pause | "
            "resume | fault <código> | clear | datatransfer | queue | "
            "disconnect | help"
        )
        # Prompt visível (">> ") em vez de input() sem marcador nenhum —
        # sem isso, era fácil perder de vista onde exatamente o terminal
        # esperava você digitar algo no meio do stream de heartbeats e
        # meter values rolando por cima.
        prompt = "\033[32m>> \033[0m" if self.use_color else ">> "
        while True:
            raw = await loop.run_in_executor(None, input, prompt)
            parts = raw.strip().split()
            if not parts:
                continue
            cmd = parts[0].lower()

            # ── start <id_tag> ──────────────────────────────────────────
            if cmd == "start":
                if state.active_transaction_id is not None:
                    self.log.warning("[CONSOLE] Já existe uma sessão ativa.")
                    continue
                if state.is_faulted:
                    self.log.warning(
                        "[CONSOLE] Charger em Faulted — rode 'clear' antes "
                        "de iniciar uma nova sessão."
                    )
                    continue
                if state.availability_status == "Inoperative":
                    self.log.warning(
                        "[CONSOLE] Conector Inoperative (ChangeAvailability do "
                        "CSMS) — sessão não pode ser iniciada."
                    )
                    continue
                id_tag = parts[1] if len(parts) > 1 else "LOCAL_TAG"
                # Conector reservado: só o id_tag (ou parent_id_tag) da
                # reserva pode iniciar sessão — qualquer outro é recusado
                # sem nem chamar Authorize, igual a um charger físico
                # reservado recusando um RFID errado no totem.
                if state.reservation_id is not None and id_tag not in (
                    state.reserved_for_id_tag, state.reserved_parent_id_tag
                ):
                    self.log.warning(
                        f"[CONSOLE] Conector reservado (reservation_id="
                        f"{state.reservation_id}) para outro id_tag — "
                        f"'{id_tag}' recusado."
                    )
                    continue
                self.log.info(
                    f"[CONSOLE] RFID local: autorizando id_tag='{id_tag}' ..."
                )
                asyncio.create_task(
                    self._local_start_flow(self.config.connector_id, id_tag)
                )

            # ── stop ────────────────────────────────────────────────────
            elif cmd == "stop":
                if state.active_transaction_id is None:
                    self.log.warning("[CONSOLE] Nenhuma sessão ativa para encerrar.")
                    continue
                self.log.info(
                    f"[CONSOLE] Encerrando sessão pelo cliente "
                    f"(tx={state.active_transaction_id})"
                )
                asyncio.create_task(
                    self._send_stop_transaction(
                        state.active_transaction_id, reason=Reason.ev_disconnected
                    )
                )

            # ── pause ───────────────────────────────────────────────────
            elif cmd == "pause":
                if state.active_transaction_id is None:
                    self.log.warning("[CONSOLE] Nenhuma sessão ativa para pausar.")
                    continue
                if state.session_suspended:
                    self.log.warning("[CONSOLE] Sessão já está suspensa.")
                    continue
                state.session_suspended = True
                self.log.info("⏸️  [CONSOLE] Carregamento pausado → SuspendedEV")
                asyncio.create_task(
                    self.send_status_notification(ChargePointStatus.suspended_ev)
                )

            # ── resume ──────────────────────────────────────────────────
            elif cmd == "resume":
                if state.active_transaction_id is None:
                    self.log.warning("[CONSOLE] Nenhuma sessão ativa para retomar.")
                    continue
                if not state.session_suspended:
                    self.log.warning("[CONSOLE] Sessão não está suspensa.")
                    continue
                state.session_suspended = False
                self.log.info("▶️  [CONSOLE] Carregamento retomado → Charging")
                asyncio.create_task(
                    self.send_status_notification(ChargePointStatus.charging)
                )

            # ── fault <código> ──────────────────────────────────────────
            elif cmd == "fault":
                code_str = parts[1].lower() if len(parts) > 1 else ""
                error_code = FAULT_CODE_MAP.get(code_str)
                if error_code is None:
                    self.log.warning(
                        f"[CONSOLE] Código de falha desconhecido: '{code_str}'. "
                        f"Válidos: {', '.join(FAULT_CODE_MAP)}"
                    )
                    continue
                self.log.warning(
                    f"[CONSOLE] Simulando falha: {error_code.value}"
                )
                asyncio.create_task(
                    self._send_fault_notification(error_code)
                )

            # ── clear ───────────────────────────────────────────────────
            elif cmd == "clear":
                if not state.is_faulted:
                    self.log.warning("[CONSOLE] Nenhuma falha ativa para limpar.")
                    continue
                asyncio.create_task(self._send_fault_clear())

            # ── datatransfer <vendor_id> [message_id] [data...] ───────────
            # Envia um DataTransfer do charger PARA o CSMS — útil para
            # testar o handler DataTransfer do lado do servidor sem
            # precisar de um evento OCPP padrão que dispare isso sozinho.
            elif cmd == "datatransfer":
                if len(parts) < 2:
                    self.log.warning(
                        "[CONSOLE] Uso: datatransfer <vendor_id> [message_id] [data...]"
                    )
                    continue
                vendor_id = parts[1]
                message_id = parts[2] if len(parts) > 2 else None
                data = " ".join(parts[3:]) if len(parts) > 3 else None
                asyncio.create_task(
                    self._send_data_transfer(vendor_id, message_id, data)
                )

            # ── queue ───────────────────────────────────────────────────
            # Mostra o que está acumulado na fila offline agora — útil
            # pra confirmar que mensagens estão sendo enfileiradas
            # corretamente durante um teste de queda de rede, sem
            # precisar esperar a reconexão pra descobrir.
            elif cmd == "queue":
                n = len(state.offline_queue)
                if n == 0:
                    self.log.info("[CONSOLE] fila offline vazia.")
                else:
                    kinds = ", ".join(item["kind"] for item in state.offline_queue)
                    self.log.info(f"[CONSOLE] fila offline com {n} mensagem(ns): {kinds}")
                self.log.info(
                    f"[CONSOLE] conectividade: {'online' if self.is_online else 'OFFLINE'}"
                )

            # ── disconnect ──────────────────────────────────────────────
            # Derruba a conexão WebSocket na hora, de propósito — gatilho
            # manual de chaos, complementar às flags --chaos-disconnect-*
            # (que fazem isso sozinho em intervalos). Útil pra testar um
            # cenário específico sem esperar o sorteio automático.
            elif cmd == "disconnect":
                if not self.is_online or self._connection is None:
                    self.log.warning("[CONSOLE] já está offline.")
                    continue
                self.log.warning("[CONSOLE] forçando desconexão manual (teste de rede)...")
                asyncio.create_task(self._connection.close())

            elif cmd == "help":
                self.log.info(
                    "[CONSOLE] Comandos:\n"
                    "  start <id_tag>   — RFID local (Authorize/lista local → StartTransaction)\n"
                    "  stop             — cliente encerra sessão (ev_disconnected)\n"
                    "  pause            — carro pausa carregamento (SuspendedEV)\n"
                    "  resume           — carro retoma carregamento (Charging)\n"
                    "  fault <código>   — simula falha de hardware (Faulted)\n"
                    f"  códigos de fault: {', '.join(FAULT_CODE_MAP)}\n"
                    "  clear            — limpa a falha ativa (volta a Available)\n"
                    "  datatransfer <vendor_id> [message_id] [data]\n"
                    "                   — envia DataTransfer para o CSMS\n"
                    "  queue            — mostra a fila offline e o status de conectividade\n"
                    "  disconnect       — derruba a conexão de propósito (teste de rede)\n"
                    "  help             — esta mensagem\n"
                    "\n"
                    "  Reserva (ReserveNow/CancelReservation) e lista local "
                    "(SendLocalList) são\n"
                    "  controladas pelo CSMS — 'start' respeita ambas automaticamente.\n"
                    "  Offline, mensagens (StatusNotification/MeterValues/Start·StopTransaction)\n"
                    "  são enfileiradas e reenviadas automaticamente ao reconectar."
                )
            elif cmd:
                self.log.warning(f"[CONSOLE] Comando desconhecido: '{cmd}'. Digite 'help'.")

    async def _local_start_flow(self, connector_id: int, id_tag: str):
        """
        Fluxo de start iniciado localmente pelo motorista (RFID no totem).
        Diferente do RemoteStart (que vem do CSMS pronto para iniciar):
        aqui o carregador precisa autorizar o id_tag antes de iniciar a
        transação.

        Se o id_tag estiver na lista local (carregada via SendLocalList),
        usamos o status de lá direto — sem round-trip nenhum ao CSMS,
        simulando um charger capaz de autorizar offline/localmente com
        uma lista pré-carregada. Só cai no Authorize remoto quando o
        id_tag não está na lista local — e, se estivermos offline nesse
        caso, recusamos direto: sem lista local e sem conexão, não tem
        como confirmar autorização nenhuma (Authorize precisa de
        resposta síncrona pra decidir se libera a sessão — não é algo
        que faça sentido simplesmente enfileirar para depois).
        """
        try:
            local_status = self.state.local_auth_list.get(id_tag)
            if local_status is not None:
                status = local_status
                self.log.info(
                    f"[LOCAL START] id_tag='{id_tag}' encontrado na lista local "
                    f"(status={status}) — sem chamada Authorize ao CSMS."
                )
            elif not self.is_online:
                self.log.warning(
                    f"[LOCAL START] offline e id_tag='{id_tag}' não está na "
                    "lista local — não é possível autorizar sem conexão. "
                    "Sessão não iniciada."
                )
                return
            else:
                auth_request = call.Authorize(id_tag=id_tag)
                auth_response = await self._call_or_queue(
                    auth_request, kind="Authorize", queueable=False
                )
                if auth_response is None:
                    self.log.warning(
                        f"[LOCAL START] Authorize para id_tag='{id_tag}' não "
                        "teve resposta a tempo. Sessão não iniciada."
                    )
                    return
                status = auth_response.id_tag_info.get("status", "Invalid")

            if status != AuthorizationStatus.accepted:
                self.log.warning(
                    f"[LOCAL START] id_tag='{id_tag}' não autorizado "
                    f"(status={status}). Sessão não iniciada."
                )
                return

            self.log.info(
                f"[LOCAL START] id_tag='{id_tag}' autorizado → iniciando transação"
            )
            await self._send_start_transaction(connector_id, id_tag)
        except Exception:
            self.log.exception("[LOCAL START] Falha no fluxo de autorização local.")

    async def _send_fault_notification(self, error_code: ChargePointErrorCode):
        """
        Envia StatusNotification com status Faulted e o error_code informado.
        Se havia sessão ativa, encerra com Reason.other — comportamento real:
        um carregador que falha não pode simplesmente continuar a sessão,
        então manda StopTransaction antes de reportar o fault.
        """
        state = self.state
        if state.active_transaction_id is not None:
            self.log.warning(
                f"[FAULT] Sessão ativa (tx={state.active_transaction_id}) será "
                "encerrada pelo fault antes de reportar o erro."
            )
            await self._send_stop_transaction(
                state.active_transaction_id,
                reason=Reason.other,
                skip_status_flow=True,
            )

        state.current_offered_amps = 0.0
        state.current_actual_amps = 0.0
        state.is_faulted = True

        request = call.StatusNotification(
            connector_id=self.config.connector_id,
            error_code=error_code,
            status=ChargePointStatus.faulted,
        )
        await self.call(request)
        self.log.warning(
            f"⚠️  [FAULT] StatusNotification enviado: Faulted / {error_code.value} "
            "— use 'clear' para voltar a Available."
        )

    async def _send_fault_clear(self):
        """
        Limpa uma falha simulada, enviando StatusNotification(Available,
        no_error) — sem isso, o único jeito de sair de Faulted era matar
        e reiniciar o processo inteiro, o que também derrubava a conexão
        WebSocket com o CSMS (evento diferente de "falha resolvida").
        """
        self.state.is_faulted = False
        await self.send_status_notification(ChargePointStatus.available)
        self.log.info("✅ [FAULT] Falha limpa — charger voltou para Available")

    async def _send_data_transfer(self, vendor_id: str, message_id: str | None, data: str | None):
        """Envia um DataTransfer arbitrário do charger para o CSMS (comando 'datatransfer' do console)."""
        try:
            request = call.DataTransfer(vendor_id=vendor_id, message_id=message_id, data=data)
            response = await self.call(request)
            self.log.info(
                f"[DATA TRANSFER] enviado | vendor_id={vendor_id} → "
                f"resposta: status={response.status} data={response.data!r}"
            )
        except Exception:
            self.log.exception("[DATA TRANSFER] Falha ao enviar.")

    async def run_first_boot_sequence(self):
        """
        Sequência da primeira conexão bem-sucedida da execução: fica em
        'Available' (sem veículo conectado) até receber um
        RemoteStartTransaction ou um "start" local — é
        _send_start_transaction que avança para Preparing -> Charging, e
        _send_stop_transaction que volta para Available ao final.
        """
        await self.send_boot_notification()
        await asyncio.sleep(1)
        await self.send_status_notification(ChargePointStatus.available)

    async def run_reconnect_sequence(self):
        """
        Sequência executada toda vez que a MESMA instância (com todo o
        estado que ela acumulou — sessão em andamento, SoC, fila
        offline) reconecta depois de uma queda: reenvia BootNotification,
        esvazia a fila de mensagens pendentes (ver _flush_offline_queue)
        e informa ao CSMS o status atual do conector — que pode não ser
        'Available' se, por exemplo, uma sessão continuou rodando durante
        toda a queda.
        """
        self.log.info(
            "[RECONEXÃO] reenviando BootNotification e esvaziando fila offline..."
        )
        await self.send_boot_notification()
        await self._flush_offline_queue()

        state = self.state
        if state.active_transaction_id is not None:
            await self.send_status_notification(ChargePointStatus.charging)
        elif state.is_faulted:
            await self.send_status_notification(ChargePointStatus.faulted)
        elif state.availability_status == "Inoperative":
            await self.send_status_notification(ChargePointStatus.unavailable)
        else:
            await self.send_status_notification(ChargePointStatus.available)


def _print_banner(config: SimConfig):
    """
    Painel de orientação rápida, impresso uma única vez ao ligar o
    simulador (não a cada reconexão) — sem isso, ao abrir o terminal
    você só via a primeira linha de log ("Conectando em...") e tinha que
    ir catando os valores de configuração (bateria, intervalos, URL)
    espalhados pelo topo do arquivo.
    """
    bar = "═" * 70
    lines = [
        bar,
        "  EVChargerSim — simulador de Charge Point OCPP 1.6J",
        bar,
        f"  Charge Point ID   : {config.charge_point_id}",
        f"  CSMS              : {config.url}/{config.charge_point_id}",
        f"  Conector          : {config.connector_id}",
        f"  Bateria simulada  : {config.battery_capacity_wh / 1000:.1f} kWh"
        f" | SoC inicial: {config.initial_soc_percent:.0f}%",
        f"  Heartbeat         : {config.heartbeat_interval}s"
        f" | MeterValues: {config.meter_values_interval}s"
        f" | Corrente padrão: {config.default_offered_amps:.0f}A",
        bar,
    ]
    if config.chaos_disconnect_interval_seconds > 0 or config.chaos_drop_rate > 0 or config.chaos_latency_max_ms > 0:
        lines.insert(len(lines) - 1,
            f"  ⚠ CHAOS ativo     : desconexão a cada ~{config.chaos_disconnect_interval_seconds:.0f}s"
            if config.chaos_disconnect_interval_seconds > 0 else "  ⚠ CHAOS ativo     :"
        )
        if config.chaos_latency_max_ms > 0:
            lines.insert(len(lines) - 1,
                f"                      latência {config.chaos_latency_min_ms:.0f}"
                f"–{config.chaos_latency_max_ms:.0f}ms")
        if config.chaos_drop_rate > 0:
            lines.insert(len(lines) - 1,
                f"                      perda de mensagens {config.chaos_drop_rate * 100:.0f}%")
    if sys.stdout.isatty():
        cyan, reset = "\033[36m", "\033[0m"
        lines = [f"{cyan}{line}{reset}" for line in lines]
    print("\n".join(lines))


async def _chaos_disconnect_loop(cp: "EVChargerSim", config: SimConfig, logger: logging.Logger):
    """
    Se configurado (--chaos-disconnect-interval), derruba o WebSocket de
    propósito em intervalos (± jitter) — pra testar a robustez de
    reconexão/fila offline do seu CSMS sem precisar derrubar o servidor
    manualmente toda hora. Roda para sempre, independente de
    reconexões — cada vez que a conexão atual cai (por este loop ou por
    qualquer outro motivo), o próximo ciclo simplesmente espera de novo
    antes de derrubar a próxima.
    """
    if config.chaos_disconnect_interval_seconds <= 0:
        return
    while True:
        jitter = random.uniform(
            -config.chaos_disconnect_jitter_seconds, config.chaos_disconnect_jitter_seconds
        )
        wait = max(1.0, config.chaos_disconnect_interval_seconds + jitter)
        await asyncio.sleep(wait)
        if cp.is_online and cp._connection is not None:
            logger.warning("[CHAOS] derrubando conexão de propósito (chaos_disconnect_interval)...")
            try:
                await cp._connection.close()
            except Exception:
                pass  # cp.start()/main() vão detectar a queda e reconectar normalmente


async def main(argv=None):
    """
    Loop de reconexão com backoff exponencial (2s -> 4s -> 8s ... até um
    teto de 30s, resetando assim que uma conexão fica de pé com sucesso).
    Espelha o comportamento de um charger físico real — se o servidor
    cair ou ainda não estiver no ar, tenta de novo em vez de derrubar o
    processo.

    IMPORTANTE: a instância de EVChargerSim é criada UMA ÚNICA VEZ, na
    primeira conexão bem-sucedida, e persiste através de todas as
    reconexões seguintes — só a conexão WebSocket por baixo dela é
    trocada (`cp._connection = ws`). Isso é o que permite a uma sessão
    em andamento (SoC, energia acumulada, fila offline) sobreviver a uma
    queda de rede em vez de ser apagada a cada reconexão, como acontecia
    antes (quando uma instância nova era criada em toda tentativa).

    Pelo mesmo motivo, os loops de fundo (heartbeat, meter values,
    acumulador de energia, console, chaos) também são iniciados UMA VEZ
    e rodam para sempre — eles não fazem parte do ciclo de
    conexão/reconexão abaixo, só `cp.start()` (o listener de mensagens
    do CSMS, que é específico de cada conexão) é.
    """
    config = SimConfig.load(argv)
    logger = build_logger(config.charge_point_id, config.verbose)

    _print_banner(config)
    backoff = 2
    max_backoff = 30
    cp: EVChargerSim | None = None

    while True:
        url = f"{config.url}/{config.charge_point_id}"
        logger.info(f"Conectando em {url} ...")
        try:
            async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
                logger.info("🔌 Conectado ao CSMS")

                if cp is None:
                    cp = EVChargerSim(config.charge_point_id, ws, config, logger)
                    cp.is_online = True
                    asyncio.create_task(cp.send_heartbeat_loop())
                    asyncio.create_task(
                        cp.send_meter_values_loop(interval_seconds=config.meter_values_interval)
                    )
                    asyncio.create_task(
                        cp.energy_accumulator_loop(interval_seconds=config.meter_values_interval)
                    )
                    asyncio.create_task(cp.console_command_loop())
                    asyncio.create_task(_chaos_disconnect_loop(cp, config, logger))
                    await cp.run_first_boot_sequence()
                else:
                    cp._connection = ws
                    cp.is_online = True
                    await cp.run_reconnect_sequence()

                backoff = 2
                # A partir daqui, só o listener de protocolo DESTA conexão
                # específica é aguardado — as rotinas de fundo já estão
                # rodando à parte (criadas acima, uma única vez) e
                # sobrevivem à queda desta conexão sozinhas.
                await cp.start()

            logger.warning("Conexão encerrada pelo CSMS — tentando reconectar...")
        except (OSError, asyncio.TimeoutError,
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.InvalidHandshake) as e:
            logger.warning(
                f"Não foi possível conectar/manter conexão com o CSMS "
                f"({e!r}) — nova tentativa em {backoff}s"
            )
        except Exception:
            logger.exception(
                f"Erro inesperado na sessão com o CSMS — nova tentativa em {backoff}s"
            )
        finally:
            if cp is not None:
                cp.is_online = False

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger("evchargersim").info("Simulador encerrado manualmente.")
