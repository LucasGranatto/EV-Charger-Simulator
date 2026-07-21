"""
EVChargerSim — simulador de Charge Point OCPP 1.6J (mobilityhouse/ocpp).

Simula o lado carro/carregador de um ponto AC genérico, conectando no
seu CSMS real via WebSocket, pra testar a lógica do servidor sem
hardware físico.

Uso:
    python evchargersim.py                             # ID padrão, ws://localhost:9001
    python evchargersim.py CARREGADOR_02 --url ws://host:9000
    python evchargersim.py --config sim.json            # valores padrão via JSON (ver SimConfig)
    python evchargersim.py --verbose                    # mostra Heartbeat/GetConfiguration

Para simular vários chargers, rode uma instância por terminal, cada
uma com um charge_point_id diferente.

Reconexão automática com backoff exponencial. Enquanto offline, a
sessão continua rodando fisicamente (SoC sobe, energia acumula) e
mensagens não entregues ficam numa fila local, reenviadas em ordem ao
reconectar — ver comando "queue" no console.

Instabilidade de rede injetável (--chaos-disconnect-interval,
--chaos-latency-min/max, --chaos-drop-rate) e o comando de console
"disconnect" ajudam a testar a robustez do CSMS sem depender de uma
queda real.

Comandos de console: digite "help" com o simulador rodando pra ver a
lista completa (start/stop/pause/resume/fault/clear/datatransfer/
queue/disconnect).
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
# `websockets` usa lazy loading e não expõe `exceptions` por padrão —
# sem este import explícito, `websockets.exceptions.ConnectionClosed`
# levanta AttributeError na hora de casar a exceção, mascarando quedas
# de rede reais em vez de capturá-las (bug real, corrigido aqui).
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
    ClearCacheStatus,
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
    Configuração de uma instância — fixa após o boot (ao contrário de
    ChargerState, que muda a cada mensagem). Precedência: CLI > --config
    (JSON) > defaults abaixo.
    """
    charge_point_id: str = "EVCHARGERSIM_01"
    url: str = "ws://localhost:9001"
    verbose: bool = False
    connector_id: int = 1

    meter_values_interval: int = 30
    heartbeat_interval: int = 120

    default_offered_amps: float = 16.0
    simulation_speed: float = 1.0

    battery_capacity_wh: float = 50_000.0
    initial_soc_percent: float = 20.0

    nominal_voltage: float = 225.0

    # Timeout para chamadas críticas (Start/StopTransaction) — sem isso,
    # um CSMS que trava sem responder deixa o simulador pendurado pra
    # sempre. Ver _send_start_transaction / _send_stop_transaction.
    call_timeout_seconds: float = 30.0

    # ── Instabilidade de rede injetável (chaos) — tudo opt-in, 0/desligado
    # por padrão. Ver README para exemplos de uso.
    chaos_disconnect_interval_seconds: float = 0.0  # 0 = desabilitado
    chaos_disconnect_jitter_seconds: float = 5.0
    chaos_latency_min_ms: float = 0.0
    chaos_latency_max_ms: float = 0.0
    chaos_drop_rate: float = 0.0  # 0.0-1.0

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

        # CLI só sobrescreve o que foi de fato passado (senão o default
        # do argparse sempre pisaria no valor vindo do --config).
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
    parser.add_argument("charge_point_id", nargs="?", default=None,
                         help="ID do charge point (padrão: EVCHARGERSIM_01).")
    parser.add_argument("--url", default=None,
                         help="URL base do CSMS, sem o ID (padrão: ws://localhost:9001).")
    parser.add_argument("--config", default=None,
                         help="Arquivo JSON com valores padrão (ver SimConfig). CLI tem prioridade.")
    parser.add_argument("--connector-id", type=int, default=None)
    parser.add_argument("--meter-interval", type=int, default=None,
                         help="Intervalo de MeterValues em segundos (padrão: 30).")
    parser.add_argument("--heartbeat-interval", type=int, default=None,
                         help="Intervalo inicial de Heartbeat em segundos (padrão: 120).")
    parser.add_argument("--default-amps", type=float, default=None,
                         help="Corrente ao iniciar sessão, antes do 1º SetChargingProfile (padrão: 16.0).")
    parser.add_argument("--sim-speed", type=float, default=None,
                         help="Fator de aceleração da simulação (padrão: 1.0 = tempo real).")
    parser.add_argument("--battery-wh", type=float, default=None,
                         help="Capacidade da bateria simulada em Wh (padrão: 50000).")
    parser.add_argument("--initial-soc", type=float, default=None,
                         help="SoC inicial de cada sessão, em %% (padrão: 20.0).")
    parser.add_argument("--voltage", type=float, default=None,
                         help="Tensão nominal de referência em V (padrão: 225.0).")
    parser.add_argument("--call-timeout", type=float, default=None,
                         help="Timeout (s) para Start/StopTransaction (padrão: 30.0).")
    parser.add_argument("--chaos-disconnect-interval", type=float, default=None,
                         help="Derruba o WebSocket a cada N segundos ± jitter (padrão: desabilitado).")
    parser.add_argument("--chaos-disconnect-jitter", type=float, default=None,
                         help="Variação (± segundos) em torno do intervalo acima (padrão: 5.0).")
    parser.add_argument("--chaos-latency-min", type=float, default=None,
                         help="Atraso mínimo artificial (ms) por mensagem (padrão: 0).")
    parser.add_argument("--chaos-latency-max", type=float, default=None,
                         help="Atraso máximo artificial (ms) por mensagem (padrão: 0).")
    parser.add_argument("--chaos-drop-rate", type=float, default=None,
                         help="Probabilidade (0.0–1.0) de perda simulada de mensagem (padrão: 0.0).")
    parser.add_argument("--verbose", action="store_true",
                         help="Mostra Heartbeat/GetConfiguration no terminal (padrão: silenciosos).")
    return parser.parse_args(argv)


@dataclass
class ChargerState:
    """
    Estado de sessão/runtime de UM charge point simulado — muda ao
    longo da execução (diferente de SimConfig, fixo após o boot). Cada
    EVChargerSim tem seu próprio `self.state`, evitando que múltiplas
    instâncias no mesmo processo pisem umas nas outras.
    """
    current_offered_amps: float = 0.0  # limite vindo do CSMS (SetChargingProfile)
    current_actual_amps: float = 0.0   # o que o "carro" simula puxar de fato

    active_transaction_id: int | None = None
    energy_meter_wh: float = 0.0  # contador de energia acumulada (Wh)

    # Relido a cada ciclo por send_heartbeat_loop — uma mudança via
    # ChangeConfiguration(HeartbeatInterval) tem efeito imediato.
    current_heartbeat_interval: int = 120

    battery_soc_percent: float = 20.0

    session_suspended: bool = False        # True em SuspendedEV (pausa do carro)
    evse_suspended_by_profile: bool = False  # True em SuspendedEVSE (0A imposto pelo CSMS)

    # True entre "fault" e "clear" — console recusa "start" até limpar,
    # espelhando um charger real que não sai de Faulted sozinho.
    is_faulted: bool = False

    # ── Reserva (ReserveNow/CancelReservation): "start" local só aceita
    # o id_tag (ou parent_id_tag) reservado enquanto reservation_id != None.
    reservation_id: int | None = None
    reserved_for_id_tag: str | None = None
    reserved_parent_id_tag: str | None = None

    # ── Lista local de autorização (SendLocalList): id_tag -> status.
    # Se presente, o start local usa esse status sem chamar Authorize.
    local_auth_list: dict = field(default_factory=dict)
    local_list_version: int = 0

    # ── Disponibilidade (ChangeAvailability): "Operative"/"Inoperative".
    availability_status: str = "Operative"
    # Mudança p/ Inoperative pedida DURANTE sessão ativa: fica pendente
    # (resposta "Scheduled") até a sessão terminar — ver spec OCPP.
    pending_availability_change: str | None = None

    # ── Fila de mensagens não entregues (offline ou chaos), reenviadas
    # em ordem na reconexão — ver _call_or_queue / _flush_offline_queue.
    # Item: {"kind": str, "request": call.X, "local_tx_id": int|None}.
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
        # NÃO usar `self.logger` — BaseChargePoint já usa esse nome
        # internamente pra logar toda mensagem OCPP crua (via logger
        # "ocpp", suprimido em build_logger()). Sobrescrever com o
        # logger deste módulo faz esse tráfego vazar pro terminal. Por
        # isso o logger próprio da classe se chama `self.log`.
        self.log = logger
        self.state = ChargerState(
            battery_soc_percent=config.initial_soc_percent,
            current_heartbeat_interval=config.heartbeat_interval,
        )
        self.use_color = sys.stdout.isatty()

        # Task de agendamento do perfil de carga ativo (ver
        # _run_charging_schedule) — instância, não ChargerState, porque é
        # uma asyncio.Task, não dado serializável.
        self._profile_task: asyncio.Task | None = None

        # Plumbing de conectividade — também instância, não ChargerState
        # (são detalhes de transporte, não "dados simulados"). main()
        # alterna is_online e reatribui self._connection a cada
        # queda/reconexão; a instância inteira persiste entre elas.
        self.is_online: bool = False
        self._local_tx_counter: int = 0

        # Guarda "um início de sessão já está em andamento" — cobre a
        # janela entre aceitar um RemoteStart/start local e
        # active_transaction_id ser de fato gravado em
        # _send_start_transaction. SEM isso, um segundo
        # RemoteStartTransaction (ex.: reenviado pelo próprio CSMS após
        # seu client-side timeout, enquanto o primeiro ainda nem tinha
        # terminado) passa pelo guard `active_transaction_id is not
        # None` porque esse campo ainda está vazio, e dispara uma SEGUNDA
        # _send_start_transaction concorrente — resultando em dois
        # StartTransaction completos pro mesmo conector físico. Ver
        # _try_begin_start/_end_start.
        self._start_in_progress: bool = False

        # Sinaliza pra _send_start_transaction que, assim que
        # active_transaction_id resolver (sucesso, enfileirado, ou
        # timeout sem confirmação), a sessão deve ser encerrada
        # IMEDIATAMENTE em vez de seguir pra Charging — usado por
        # on_reset quando o Reset chega enquanto o start ainda está em
        # andamento (active_transaction_id ainda None, então o guard
        # "sessão ativa" normal do on_reset não vê nada pra parar ainda).
        # Sem isso, um Reset nesse instante exato é silenciosamente
        # ignorado pela sessão que só termina de iniciar um instante
        # depois — o carregador fica "carregando" apesar do reset.
        self._abort_pending_start_reason: "Reason | None" = None

        # Reentrância de _flush_offline_queue: send_meter_values_loop
        # tenta esvaziar a fila a cada ciclo (oportunista) E
        # run_reconnect_sequence também chama no reconnect — se um flush
        # demorado (item lento/timeout) ainda estiver rodando quando o
        # próximo gatilho disparar, duas chamadas concorrentes podem
        # entrelaçar o envio das mensagens fora de ordem. Ver
        # _flush_offline_queue.
        self._flush_in_progress: bool = False

    def _try_begin_start(self) -> bool:
        """
        Tenta reservar "iniciando sessão" de forma atômica (sem await
        entre o check e o set — por isso é um método síncrono comum,
        não uma coroutine, e deve ser chamado ANTES de qualquer await
        no fluxo de start). Retorna False se já há um início em
        andamento; quem chama deve recusar o pedido nesse caso.
        """
        if self._start_in_progress:
            return False
        self._start_in_progress = True
        return True

    def _end_start(self):
        """Libera a reserva de _try_begin_start — SEMPRE via finally, em qualquer desfecho."""
        self._start_in_progress = False

    # --------------------------------------------------------
    # Handlers de mensagens recebidas do CSMS
    # (a lib ocpp converte todo o payload recursivamente de camelCase
    # pra snake_case antes de chamar estes handlers — nunca precisa de
    # fallback pra chaves tipo "startPeriod"/"idTag")
    # --------------------------------------------------------

    def _limit_to_amps(self, limit: float, unit: str) -> float:
        """
        Converte um limite de chargingSchedulePeriod para amperes.
        "W" é convertido usando a tensão nominal (simplificação
        monofásica); "A" passa direto.
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
        local_tx_id: int | None = None,
        return_queued: bool = False,
    ):
        """
        Ponto único por onde toda mensagem espontânea do charger
        (StatusNotification, MeterValues, Heartbeat, Start/StopTransaction)
        passa antes de sair pela rede. Duas responsabilidades:

        1) Fila offline: SÓ enfileira quando a mensagem de fato não saiu
           pela rede — offline já na entrada, chaos derrubando antes de
           tentar, ou a conexão caindo durante a tentativa
           (ConnectionClosed/OSError). Um simples asyncio.TimeoutError
           NÃO enfileira mais: a mensagem já tinha sido enviada (self.call
           manda o CALL antes de esperar a resposta) e o socket continua
           de pé — não há como saber se o CSMS recebeu/processou.
           Reenviar a mesma Start/StopTransaction nessa hora é o jeito
           mais fácil de o CSMS acabar com uma transação fantasma
           duplicada, caso ele só estivesse lento (não caído) — visto na
           prática contra um CSMS real que ocasionalmente estourava
           call_timeout_seconds só de lento, e cada timeout virava um
           reenvio silencioso registrado como uma segunda sessão pro
           mesmo conector. Ver _send_start_transaction/_send_stop_transaction
           pra como o "sem resposta, mas sem reenviar" é tratado.
        2) Chaos: latência artificial e perda simulada (SimConfig.chaos_*)
           são aplicadas aqui, antes de qualquer tentativa de envio — mas
           só quando há de fato uma conexão pra "perturbar" (offline
           checado primeiro, ver abaixo).

        return_queued=True muda o retorno para (response, queued) — só os
        dois chamadores que precisam saber se a mensagem foi de fato
        salva pra reenvio usam isso (_send_start_transaction/
        _send_stop_transaction), pra decidir como representar uma sessão
        cujo destino no CSMS ficou desconhecido. Todo o resto continua
        recebendo só `response | None`, como sempre.

        Retorna a resposta do CSMS, ou None se enfileirada, descartada
        (chaos) ou sem resposta a tempo.
        """
        def _result(response, queued):
            return (response, queued) if return_queued else response

        timeout = timeout if timeout is not None else self.config.call_timeout_seconds

        # Offline de verdade: chaos não tem nada pra perturbar aqui — a
        # mensagem já não ia sair mesmo. Resolver isso primeiro evita
        # pagar latência/drop artificiais em cima de algo que já está
        # simplesmente sem conexão.
        if not self.is_online:
            if queueable:
                self._enqueue_offline(kind, request, local_tx_id=local_tx_id)
                return _result(None, True)
            self.log.debug(f"[OFFLINE] '{kind}' pulado (não crítico, não enfileirável).")
            return _result(None, False)

        # Chaos: perda de mensagem simulada — a mensagem nunca chega a
        # sair, então enfileirar é seguro (equivalente a estar offline).
        if self.config.chaos_drop_rate > 0 and random.random() < self.config.chaos_drop_rate:
            self.log.warning(f"[CHAOS] '{kind}' descartado (perda de rede simulada).")
            if queueable:
                self._enqueue_offline(kind, request, local_tx_id=local_tx_id)
                return _result(None, True)
            return _result(None, False)

        # Chaos: atraso artificial, contabilizado DENTRO do orçamento de
        # timeout (não somado por fora) — chaos_latency_max_ms acima de
        # call_timeout_seconds simula "o CSMS não respondeu a tempo".
        remaining_timeout = timeout
        if self.config.chaos_latency_max_ms > 0:
            delay_ms = random.uniform(
                self.config.chaos_latency_min_ms, self.config.chaos_latency_max_ms
            )
            delay_s = delay_ms / 1000
            if delay_s >= remaining_timeout:
                await asyncio.sleep(remaining_timeout)
                self.log.warning(
                    f"[CSMS] '{kind}' não teve resposta em {timeout}s (orçamento "
                    "consumido por latência simulada — chaos_latency)."
                )
                return _result(None, False)
            if delay_s > 0:
                await asyncio.sleep(delay_s)
                remaining_timeout -= delay_s

        try:
            response = await asyncio.wait_for(self.call(request), timeout=remaining_timeout)
            return _result(response, False)
        except asyncio.TimeoutError:
            # A mensagem SAIU e o socket segue de pé — não sabemos se o
            # CSMS recebeu/processou. Deliberadamente NÃO enfileira (ver
            # docstring): quem chama decide como lidar com "sem resposta,
            # conexão ok".
            self.log.warning(
                f"[CSMS] '{kind}' não teve resposta em {timeout}s — conexão "
                "segue online, então NÃO reenviando automaticamente (o CSMS "
                "pode só estar lento, não ter perdido a mensagem)."
            )
            return _result(None, False)
        except (websockets.exceptions.ConnectionClosed, OSError) as exc:
            self.log.warning(f"[OFFLINE] conexão perdida enviando '{kind}' ({exc!r}).")
            self.is_online = False
            if queueable:
                self._enqueue_offline(kind, request, local_tx_id=local_tx_id)
                return _result(None, True)
            return _result(None, False)

    async def _flush_offline_queue(self):
        """
        Reenvia em ordem as mensagens acumuladas enquanto offline —
        StatusNotification/MeterValues/Start·StopTransaction chegam ao
        CSMS na mesma ordem em que aconteceram de verdade.

        Se um StartTransaction enfileirado usava um ID local temporário
        (negativo, atribuído por _send_start_transaction enquanto
        offline), o ID real devolvido pelo CSMS é propagado para
        qualquer StopTransaction enfileirado depois com esse mesmo ID
        local — senão o CSMS receberia um StopTransaction pra um
        transaction_id que nunca existiu do lado dele.

        Limitação conhecida (do protocolo, não deste simulador): OCPP
        1.6 não tem idempotência embutida — se a conexão cair depois do
        CSMS já ter processado uma mensagem mas antes da confirmação
        chegar aqui, um reenvio no próximo flush pode duplicar essa
        mensagem do lado do servidor.
        """
        if self._flush_in_progress:
            # send_meter_values_loop tenta esvaziar a fila a cada ciclo
            # (oportunista) e run_reconnect_sequence também chama no
            # reconnect — se um flush anterior ainda estiver rodando
            # (ex.: um item lento perto do call_timeout_seconds), uma
            # segunda chamada concorrente poderia pegar itens novos
            # enfileirados nesse meio tempo e mandá-los ANTES dos itens
            # mais antigos ainda em trânsito no primeiro flush — quebra
            # a ordem que esta função promete manter. Só pula; o próximo
            # ciclo tenta de novo.
            self.log.debug("[FILA OFFLINE] flush já em andamento — pulando chamada concorrente.")
            return

        state = self.state
        if not state.offline_queue:
            return

        self._flush_in_progress = True
        try:
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
                except asyncio.TimeoutError:
                    # A mensagem SAIU e o socket pode muito bem seguir de
                    # pé — um timeout aqui NÃO prova que a conexão caiu
                    # (mesmo raciocínio de _call_or_queue). Diferente do
                    # ConnectionClosed abaixo, deliberadamente NÃO marca
                    # is_online=False por causa disso: fazer isso deixaria
                    # o simulador "preso" acreditando estar offline pra
                    # sempre mesmo com o socket saudável, já que nada mais
                    # detectaria essa queda que nunca aconteceu (o listener
                    # central — cp.start() — segue lendo normalmente).
                    # Também não reenfileira este item — seria o mesmo
                    # risco de duplicar já corrigido em _call_or_queue.
                    # Loga como ERROR (merece atenção manual) e segue pro
                    # próximo item, em vez de abortar o resto do flush por
                    # causa de UM item incerto.
                    self.log.error(
                        f"[FILA OFFLINE] '{kind}' sem resposta do CSMS em "
                        f"{self.config.call_timeout_seconds}s durante o flush "
                        "— conexão segue online, então NÃO reenfileirando "
                        "(evita duplicar) e seguindo para o próximo item. "
                        "Verifique manualmente se o CSMS recebeu esta mensagem."
                    )
                    continue
                except (websockets.exceptions.ConnectionClosed, OSError) as exc:
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
                    self.log.info(
                        f"[FILA OFFLINE] ID local {local_tx_id} resolvido para "
                        f"transaction_id real {real_id}"
                    )
                    if not self._start_transaction_authorized(response):
                        tag_status = response.id_tag_info.get("status")
                        self.log.warning(
                            f"[FILA OFFLINE] StartTransaction confirmado mas "
                            f"id_tag_info.status={tag_status} — abortando a sessão "
                            "que já rodava offline, StopTransaction imediato."
                        )
                        asyncio.create_task(self._send_stop_transaction(real_id, reason=Reason.other))

            self.log.info("[FILA OFFLINE] todas as mensagens pendentes foram entregues.")
        finally:
            self._flush_in_progress = False

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

        # Reflete no StatusNotification quando o CSMS impõe/restaura 0A —
        # senão o status ficava travado em "Charging" mesmo com corrente
        # zerada. Só entra em jogo com sessão ativa e sem SuspendedEV
        # (que tem prioridade — é uma causa de suspensão diferente).
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
        primeiro — antes um perfil com múltiplos degraus era achatado no
        valor do primeiro pra sessão inteira.

        Simplificação: cada start_period é tratado como segundos
        relativos ao momento em que este SetChargingProfile foi
        recebido (não ao início da transação nem a um startSchedule
        absoluto) — suficiente pra testar degraus manualmente; perfis
        recorrentes (Daily/Weekly) não são interpretados de forma especial.
        """
        ordered = sorted(periods, key=lambda p: p.get("start_period", 0))
        try:
            for i, period in enumerate(ordered):
                start_period = period.get("start_period", 0)
                amps = self._limit_to_amps(period["limit"], unit)
                self._apply_offered_amps(amps, source="PERFIL RECEBIDO")

                if i + 1 < len(ordered):
                    next_start = ordered[i + 1].get("start_period", 0)
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
        unit = schedule.get("charging_rate_unit", "A")

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
        Remove o(s) perfil(is) ativo(s) e volta à corrente padrão do
        simulador (se sessão ativa) ou 0A.
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
        if not self._try_begin_start():
            # active_transaction_id só é gravado dentro de
            # _send_start_transaction — antes disso, o guard acima não
            # pega um segundo RemoteStart que chegue enquanto o primeiro
            # ainda está a caminho (ex.: o próprio CSMS reenviando após
            # dar timeout na resposta dele, sem cancelar a tentativa
            # anterior). Sem este guard, os dois seguem em paralelo e o
            # CSMS acaba com duas transações completas pro mesmo conector.
            self.log.warning(
                "[REMOTE START] já existe um início de sessão em andamento "
                "(aguardando StartTransaction confirmar) — recusando para "
                "evitar StartTransaction duplicado."
            )
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
        Inoperative com sessão ativa -> Scheduled (aplicado só quando a
        sessão terminar, conforme o spec); sem sessão -> aplica na hora
        e manda StatusNotification Unavailable. Operative sempre aplica
        na hora (cancela um Scheduled pendente) e volta a Available.
        """
        self.log.info(f"[CHANGE AVAILABILITY] connector={connector_id} type={type}")
        state = self.state

        if type == AvailabilityType.inoperative:
            if state.active_transaction_id is not None or self._start_in_progress:
                # _start_in_progress cobre o mesmo instante que o Reset
                # trata (ver on_reset/_abort_pending_start_reason): um
                # start aceito mas ainda sem active_transaction_id
                # gravado. Aqui não precisa de um sinal de abort à parte
                # — quando a sessão resolver e active_transaction_id for
                # gravado, o mecanismo normal de "Scheduled" já aplica o
                # Inoperative no fim dela, então só tratar como sessão
                # ativa já resolve.
                state.pending_availability_change = "Inoperative"
                self.log.info(
                    "[CHANGE AVAILABILITY] sessão ativa (ou iniciando) — "
                    "mudança para Inoperative agendada para quando a "
                    "sessão terminar."
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
        Sessão ativa é interrompida (StopTransaction com motivo
        soft/hard_reset) — não tem como continuar entregando corrente
        depois de reiniciar. Soft: interrupção breve, volta a Available
        rápido. Hard: fica Unavailable por um tempo, simulando o boot
        do firmware, antes de voltar.
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
        elif self._start_in_progress:
            # Um start foi aceito e está a caminho, mas
            # active_transaction_id ainda não foi gravado — não há nada
            # pra _handle_reset_flow parar ainda. Sinaliza pra
            # _send_start_transaction encerrar a sessão assim que ela
            # resolver, em vez de deixá-la completar e ir pra Charging
            # como se o reset nunca tivesse acontecido.
            self.log.info(
                "[RESET] início de sessão em andamento (ainda sem "
                "confirmação) — será encerrada assim que resolver."
            )
            self._abort_pending_start_reason = reason
            asyncio.create_task(self._handle_reset_flow(None, reason, is_hard))
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
            # Via _call_or_queue, não self.call direto — offline, isso
            # levantaria ConnectionClosed numa task sem ninguém aguardando.
            asyncio.create_task(
                self._call_or_queue(call.Heartbeat(), kind="Heartbeat", queueable=False)
            )
        elif requested_message == "MeterValues":
            # Amostra IMEDIATA — é esse o propósito do trigger, não
            # esperar até 30s pelo próximo ciclo do loop periódico.
            asyncio.create_task(
                self._call_or_queue(self._build_meter_values_request(), kind="MeterValues")
            )
        return call_result.TriggerMessage(status="Accepted")

    @on(Action.get_configuration)
    async def on_get_configuration(self, key=None, **kwargs):
        """
        Retorna configurações simuladas de um charger AC real.
        HeartbeatInterval reporta o valor REAL em uso
        (state.current_heartbeat_interval) — um CSMS com sync loop
        periódico que sobrescreve seu próprio estado a partir daqui
        reverteria silenciosamente qualquer ChangeConfiguration se este
        handler respondesse um valor fixo em vez do atual.
        """
        # DEBUG: alguns CSMS chamam isso periodicamente (sync loop) —
        # mesmo padrão de ruído do Heartbeat. Só aparece com --verbose.
        self.log.debug(f"[GET CONFIGURATION] keys solicitadas={key}")
        all_config = [
            {"key": "HeartbeatInterval", "readonly": False,
             "value": str(self.state.current_heartbeat_interval)},
            {"key": "MeterValueSampleInterval", "readonly": False,
             "value": str(self.config.meter_values_interval)},
            {"key": "ConnectorPhaseRotation", "readonly": True, "value": "NotApplicable"},
            {"key": "NumberOfConnectors", "readonly": True, "value": "1"},
            {"key": "SupportedFeatureProfiles", "readonly": True,
             "value": "Core,SmartCharging,Reservation,LocalAuthListManagement,FirmwareManagement"},
            {"key": "LocalAuthListEnabled", "readonly": False, "value": "true"},
            {"key": "LocalAuthListMaxLength", "readonly": True, "value": "100"},
            {"key": "SendLocalListMaxLength", "readonly": True, "value": "20"},
            {"key": "ReserveConnectorZeroSupported", "readonly": True, "value": "false"},
            {"key": "AvailabilityStatus", "readonly": True,
             "value": self.state.availability_status},
        ]
        if key:
            # CSMS pediu chaves específicas: filtra e reporta as desconhecidas
            known_keys_lower = {c["key"].lower() for c in all_config}
            requested_keys = {k.lower() for k in key}
            found = [c for c in all_config if c["key"].lower() in requested_keys]
            unknown = [k for k in key if k.lower() not in known_keys_lower]
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
        # Outras chaves são aceitas mas sem efeito simulado (ex:
        # MeterValueSampleInterval é fixo via config no boot).

        return call_result.ChangeConfiguration(status="Accepted")

    @on(Action.unlock_connector)
    async def on_unlock_connector(self, connector_id, **kwargs):
        """Libera o conector mecanicamente (ex: cabo travado)."""
        self.log.info(f"[UNLOCK CONNECTOR] connector={connector_id}")
        if self.state.active_transaction_id is not None:
            # Comportamento simplificado: não paramos a sessão
            # automaticamente — UnlockConnector não é, por si só, um
            # pedido de StopTransaction.
            self.log.warning(
                "[UNLOCK CONNECTOR] há uma sessão ativa — destravando o "
                "conector sem encerrar a sessão (comportamento simplificado)."
            )
        return call_result.UnlockConnector(status=UnlockStatus.unlocked)

    @on(Action.data_transfer)
    async def on_data_transfer(self, vendor_id, message_id=None, data=None, **kwargs):
        """
        Extensão vendor-specific do OCPP. Só reconhece o próprio
        vendor_id (echo, confirma que o transporte funciona); qualquer
        outro recebe UnknownVendorId, como manda o spec.
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
        """Simula o nome do arquivo e a sequência Uploading -> Uploaded, sem subir nada de verdade."""
        file_name = f"diagnostics_{self.config.charge_point_id}_{int(datetime.now(timezone.utc).timestamp())}.zip"
        self.log.info(f"[GET DIAGNOSTICS] location={location} | arquivo simulado: {file_name}")
        asyncio.create_task(self._simulate_diagnostics_upload())
        return call_result.GetDiagnostics(file_name=file_name)

    async def _simulate_diagnostics_upload(self):
        """
        queueable=False: não faz sentido enfileirar "Uploading" pra
        chegar depois de um "Uploaded" já enfileirado — quebraria a
        ordem. try/except na função inteira: task solta de vida curta,
        sem isso uma falha no meio viraria "Task exception was never
        retrieved" mudo.
        """
        try:
            await asyncio.sleep(1)
            await self._call_or_queue(
                call.DiagnosticsStatusNotification(status=DiagnosticsStatus.uploading),
                kind="DiagnosticsStatusNotification(Uploading)", queueable=False,
            )
            self.log.info("[DIAGNOSTICS] status: Uploading")
            await asyncio.sleep(2)
            await self._call_or_queue(
                call.DiagnosticsStatusNotification(status=DiagnosticsStatus.uploaded),
                kind="DiagnosticsStatusNotification(Uploaded)", queueable=False,
            )
            self.log.info("[DIAGNOSTICS] status: Uploaded")
        except Exception:
            self.log.exception("[DIAGNOSTICS] erro inesperado durante a simulação de upload.")

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
        """Mesmo cuidado do _simulate_diagnostics_upload: try/except na função inteira, notificações via _call_or_queue com queueable=False."""
        try:
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
                await self._call_or_queue(
                    call.FirmwareStatusNotification(status=status),
                    kind=f"FirmwareStatusNotification({status.value})", queueable=False,
                )
                self.log.info(f"[FIRMWARE] status: {status.value}")
                await asyncio.sleep(delay)

            # Reboot simulado, mesma sequência do hard reset.
            await self.send_status_notification(ChargePointStatus.unavailable)
            await asyncio.sleep(3)
            await self.send_boot_notification()
            await asyncio.sleep(1)
            await self.send_status_notification(ChargePointStatus.available)

            await self._call_or_queue(
                call.FirmwareStatusNotification(status=FirmwareStatus.installed),
                kind="FirmwareStatusNotification(Installed)", queueable=False,
            )
            self.log.info("[FIRMWARE] status: Installed — atualização concluída")
        except Exception:
            self.log.exception("[FIRMWARE] erro inesperado durante a simulação de atualização.")

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
            entry_id_tag = entry.get("id_tag")
            id_tag_info = entry.get("id_tag_info")
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

    @on(Action.clear_cache)
    async def on_clear_cache(self, **kwargs):
        """
        Limpa a Authorization Cache — que no protocolo é conceitualmente
        separada da lista local de autorização (local_auth_list, gerida
        por SendLocalList/GetLocalListVersion). Este simulador não mantém
        um cache de Authorize.conf à parte hoje, então não há estado pra
        limpar aqui; ainda assim expõe o handler (em vez de deixar cair
        no NotImplemented padrão da lib) porque ClearCache é uma das
        chamadas CSMS->CP mais comumente testadas.
        """
        self.log.info("[CLEAR CACHE] solicitado pelo CSMS — nenhum cache de autorização separado para limpar.")
        return call_result.ClearCache(status=ClearCacheStatus.accepted)

    # --------------------------------------------------------
    # Rotinas que o charge point envia PARA o CSMS
    # --------------------------------------------------------

    async def send_boot_notification(self) -> tuple[bool, float]:
        """
        Não reseta SoC/is_faulted aqui — a mesma instância persiste
        através de reconexões (ver main()), então isso apagaria uma
        sessão/falha real em andamento. queueable=False: offline já é
        tratado pelo laço de reconexão em main().

        Retorna (accepted, retry_after_seconds). Em Accepted, o campo
        `interval` da resposta é aplicado como o heartbeat interval
        (é o comportamento definido pelo protocolo — o CSMS usa
        BootNotification pra sincronizar isso, não só ChangeConfiguration).
        Em Pending/Rejected, o mesmo campo `interval` diz quanto esperar
        antes de tentar de novo; quem decide se/quantas vezes tentar de
        novo é o chamador (run_first_boot_sequence / run_reconnect_sequence).
        """
        request = call.BootNotification(
            charge_point_model="EVChargerSim",
            charge_point_vendor="EVChargerSim",
            firmware_version="SIM-1.0",
        )
        response = await self._call_or_queue(request, kind="BootNotification", queueable=False)
        if response is None:
            return False, 10.0
        if response.status == RegistrationStatus.accepted:
            if response.interval and response.interval > 0:
                self.state.current_heartbeat_interval = response.interval
                self.log.info(
                    f"BootNotification aceito pelo CSMS — heartbeat ajustado "
                    f"para {response.interval}s (definido pelo CSMS)."
                )
            else:
                self.log.info("BootNotification aceito pelo CSMS.")
            return True, 0.0
        retry_after = response.interval if response.interval and response.interval > 0 else 10.0
        self.log.warning(
            f"BootNotification respondido com status={response.status} — "
            f"CSMS ainda não aceitou o registro, nova tentativa em {retry_after:.0f}s."
        )
        return False, retry_after

    async def send_status_notification(self, status: str):
        request = call.StatusNotification(
            connector_id=self.config.connector_id,
            error_code=ChargePointErrorCode.no_error,
            status=status,
        )
        response = await self._call_or_queue(request, kind=f"StatusNotification({status})")
        if response is not None:
            self.log.info(f"StatusNotification enviado: {status}")

    @staticmethod
    def _start_transaction_authorized(response) -> bool:
        """
        True se a StartTransactionResponse veio com id_tag_info.status
        Accepted. Isso é ortogonal a "o CSMS respondeu" — um CSMS pode
        aceitar a chamada RPC (e devolver um transaction_id de verdade)
        e ainda assim recusar o id_tag (Invalid/Blocked/Expired/
        ConcurrentTx), caso em que um carregador real não entrega
        energia mesmo com a transação já registrada do lado do servidor.
        """
        status = (response.id_tag_info or {}).get("status", AuthorizationStatus.accepted)
        return status == AuthorizationStatus.accepted

    async def _send_start_transaction(self, connector_id: int, id_tag: str):
        """
        Envia StartTransaction simulando o carregador autorizando e
        fechando o contator. Offline (ou mensagem perdida por chaos), a
        sessão roda localmente do mesmo jeito, com um ID de transação
        temporário (negativo) até o CSMS confirmar um ID real no
        próximo flush da fila offline.
        """
        state = self.state
        try:
            # Evita que um agendamento de perfil pendente da sessão
            # anterior "acorde" no meio desta e pise na corrente aplicada.
            self._cancel_profile_task()

            # Reseta SoC/medidor pra não encadear com a sessão anterior.
            state.battery_soc_percent = self.config.initial_soc_percent
            state.energy_meter_wh = 0.0
            state.session_suspended = False
            self.log.info(f"[BATERIA] SoC inicial desta sessão: {state.battery_soc_percent:.1f}%")

            # Aplica a corrente padrão imediatamente, antes de qualquer
            # SetChargingProfile chegar — um carregador físico começa a
            # entregar corrente assim que o contator fecha, não fica em
            # 0A esperando o CSMS reagir. O CSMS ainda pode sobrescrever
            # isso a qualquer momento.
            state.current_offered_amps = self.config.default_offered_amps
            state.current_actual_amps = compute_actual_current(
                state.current_offered_amps, state.battery_soc_percent
            )
            self.log.info(
                f"[SESSION] Corrente inicial: {state.current_offered_amps:.0f}A oferecido "
                f"/ {state.current_actual_amps:.1f}A real (aguardando SetChargingProfile do CSMS)"
            )

            await self.send_status_notification(ChargePointStatus.preparing)
            await asyncio.sleep(1)  # simula o delay real de fechamento do contator

            # ID local reservado ANTES de tentar enviar — se a mensagem
            # for enfileirada por qualquer motivo, já temos um ID pronto.
            self._local_tx_counter -= 1
            local_id = self._local_tx_counter

            request = call.StartTransaction(
                connector_id=connector_id,
                id_tag=id_tag,
                meter_start=int(state.energy_meter_wh),
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            # return_queued=True: precisamos saber SE foi enfileirada de
            # verdade (offline/chaos/conexão caiu — mensagem nunca saiu,
            # reenviar depois é seguro) ou se só deu timeout com a conexão
            # de pé (mensagem saiu, destino desconhecido — NÃO reenviar
            # sozinho, ver _call_or_queue).
            response, queued = await self._call_or_queue(
                request,
                kind="StartTransaction",
                queueable=True,
                return_queued=True,
                local_tx_id=local_id,
            )

            if response is not None:
                state.active_transaction_id = response.transaction_id
                if not self._start_transaction_authorized(response):
                    tag_status = response.id_tag_info.get("status")
                    self.log.warning(
                        f"[START TRANSACTION] CSMS registrou a transação "
                        f"(transaction_id={state.active_transaction_id}) mas "
                        f"id_tag_info.status={tag_status} — abortando sem "
                        "entregar energia, StopTransaction imediato."
                    )
                    asyncio.create_task(
                        self._send_stop_transaction(state.active_transaction_id, reason=Reason.other)
                    )
                    return
                self.log.info(
                    f"⚡ [START TRANSACTION] aceito pelo CSMS | "
                    f"transaction_id={state.active_transaction_id} | id_tag={id_tag}"
                )
            elif queued:
                # Realmente offline (ou chaos derrubou a mensagem, ou a
                # conexão caiu na tentativa) — nunca chegou no CSMS, então
                # reenviar no próximo flush é seguro e necessário.
                state.active_transaction_id = local_id
                self.log.warning(
                    f"[FILA OFFLINE] StartTransaction enfileirado — sessão "
                    f"rodando localmente com ID temporário {local_id} até "
                    "reconectar e confirmar com o CSMS."
                )
            else:
                # Timeout com a conexão de pé: a mensagem SAIU e não
                # sabemos se o CSMS processou. A sessão roda localmente
                # com o ID temporário (fisicamente o carro já está
                # carregando), mas DELIBERADAMENTE sem reenvio automático
                # — reenviar arriscaria uma transação duplicada do lado
                # do CSMS se ele só estivesse lento, não caído (é
                # exatamente esse cenário que gerou o bug original desta
                # correção). Limitação inerente do OCPP 1.6 (sem
                # idempotência) — não tem como o simulador resolver isso
                # sozinho sem arriscar duplicar; fica registrado como
                # ERROR porque merece atenção manual do operador.
                state.active_transaction_id = local_id
                self.log.error(
                    f"[START TRANSACTION] sem resposta do CSMS em "
                    f"{self.config.call_timeout_seconds}s (conexão segue "
                    f"online) — sessão rodando localmente com ID temporário "
                    f"{local_id}, SEM confirmação do CSMS e SEM reenvio "
                    "automático (evita duplicar a transação do lado dele). "
                    "Verifique manualmente se o CSMS registrou esta sessão."
                )

            # Sessão consome a reserva do conector, se houver uma.
            if state.reservation_id is not None:
                self.log.info(
                    f"[SESSION] reserva {state.reservation_id} consumida pelo início desta sessão"
                )
                state.reservation_id = None
                state.reserved_for_id_tag = None
                state.reserved_parent_id_tag = None

            if self._abort_pending_start_reason is not None:
                # on_reset chegou enquanto esta sessão ainda não tinha
                # active_transaction_id gravado — o guard normal dele não
                # viu nada pra parar na hora, então sinalizou aqui.
                # active_transaction_id já está resolvido agora (real ou
                # placeholder local), então dá pra encerrar de verdade.
                abort_reason = self._abort_pending_start_reason
                self._abort_pending_start_reason = None
                self.log.warning(
                    f"[START TRANSACTION] reset foi pedido enquanto esta "
                    f"sessão ainda não tinha confirmado — encerrando "
                    f"imediatamente (transaction_id={state.active_transaction_id})."
                )
                asyncio.create_task(
                    self._send_stop_transaction(state.active_transaction_id, reason=abort_reason)
                )
                return

            # O CSMS real já manda um SetChargingProfile logo após o
            # boot — não precisamos de um "chute" além do já aplicado acima.
            await self.send_status_notification(ChargePointStatus.charging)
        except Exception:
            # Cobre algo genuinamente imprevisto fora do fluxo já
            # tratado acima — sem isso, uma falha aqui morreria em
            # silêncio (task via create_task, ninguém dá await nela).
            self.log.exception(
                "[START TRANSACTION] erro inesperado — sessão pode não ter "
                "sido registrada corretamente."
            )
        finally:
            # A partir daqui active_transaction_id (real ou o ID local
            # temporário) já reflete o desfecho, e esse campo é quem passa
            # a bloquear um novo start — libera a reserva feita por
            # _try_begin_start em on_remote_start_transaction/console.
            self._end_start()

    async def _send_stop_transaction(
        self,
        transaction_id: int,
        reason=None,
        skip_status_flow: bool = False,
    ):
        """
        Envia StopTransaction encerrando a sessão no CSMS.

        reason: motivo OCPP do encerramento — ex: Reason.hard_reset/
        soft_reset quando é um Reset que interrompe a sessão.
        skip_status_flow: True pula Finishing->Available (usado pelo
        hard reset, que tem sua própria sequência de status).
        """
        state = self.state
        self._cancel_profile_task()

        # Para FISICAMENTE agora, mesmo que o CSMS ainda não saiba —
        # replica um charger real (abre o contator na hora, avisa o
        # servidor depois) e é o que torna a fila offline coerente: se
        # continuássemos "carregando" até a confirmação, um
        # StopTransaction enfileirado não faria sentido.
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
            # return_queued=True: só pra logar com precisão se isto foi
            # de fato salvo pra reenvio (offline/chaos/conexão caiu) ou
            # se só deu timeout com a conexão de pé — local_tx_id permite
            # ao flush corrigir a referência se o Start correspondente
            # também ainda não foi confirmado.
            response, queued = await self._call_or_queue(
                request,
                kind="StopTransaction",
                queueable=True,
                return_queued=True,
                local_tx_id=local_id_being_stopped,
            )
            if response is not None:
                self.log.info(
                    f"🛑 [STOP TRANSACTION] enviado | transaction_id={transaction_id}"
                    + (f" | motivo={reason.value}" if reason else "")
                )
            elif queued:
                self.log.warning(
                    f"[FILA OFFLINE] StopTransaction enfileirado "
                    f"(transaction_id={transaction_id}) — será entregue ao "
                    "CSMS na próxima reconexão."
                )
            else:
                # Timeout com a conexão de pé: a mensagem SAIU e não
                # sabemos se o CSMS processou — mas a sessão já parou de
                # verdade localmente (contator aberto no topo da função),
                # então NÃO reenviamos automaticamente pelo mesmo motivo
                # do StartTransaction: se o CSMS só estava lento (não
                # caído), reenviar arrisca ele processar duas vezes o
                # encerramento da mesma transação.
                self.log.error(
                    f"[STOP TRANSACTION] sem resposta do CSMS em "
                    f"{self.config.call_timeout_seconds}s (conexão segue "
                    f"online) — sessão já encerrada localmente (transaction_id="
                    f"{transaction_id}), mas SEM confirmação do CSMS e SEM "
                    "reenvio automático. Verifique manualmente se o CSMS "
                    "registrou o encerramento desta sessão."
                )

            if skip_status_flow:
                # reset/fault/firmware têm sua própria sequência final de
                # status — uma mudança de disponibilidade pendente não é
                # aplicada aqui pra não brigar com ela.
                if state.pending_availability_change is not None:
                    self.log.warning(
                        "[CHANGE AVAILABILITY] mudança para Inoperative estava "
                        "agendada, mas a sessão terminou via reset/fault/firmware "
                        "(sequência de status própria) — reenvie ChangeAvailability "
                        "se ainda quiser aplicá-la."
                    )
                    state.pending_availability_change = None
                return

            # Mudança pra Inoperative pedida durante a sessão só é
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

            # Finishing (conector liberando) -> Available — sem isso o
            # conector ficaria "preso" em Charging mesmo sem sessão.
            await self.send_status_notification(ChargePointStatus.finishing)
            await asyncio.sleep(2)
            await self.send_status_notification(ChargePointStatus.available)
        except Exception:
            # Estado local já foi limpo acima — o que fica pendente aqui
            # é só a sequência de status pós-stop, não a sessão em si.
            self.log.exception(
                "[STOP TRANSACTION] erro inesperado após a sessão já ter "
                "sido encerrada localmente."
            )

    async def energy_accumulator_loop(self, interval_seconds: int = 30):
        """
        Acumula energia (Wh) enquanto há transação ativa e não suspensa,
        avançando SoC e corrente (tapering) a cada ciclo. Ao atingir
        100%, manda StopTransaction automaticamente (EV sinalizando
        bateria cheia). config.simulation_speed multiplica o delta de
        energia por ciclo (não o intervalo real entre ciclos).

        Iniciado uma vez em main() e roda para sempre, independente de
        reconexões — continuar "carregando" fisicamente mesmo offline é
        o que torna a fila offline coerente. Cada ciclo tem seu próprio
        try/except pra um erro isolado não derrubar o loop de vez.
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
        Intervalo relido a cada ciclo (state.current_heartbeat_interval)
        — uma mudança via ChangeConfiguration tem efeito no próximo
        ciclo. Roda para sempre; Heartbeat é queueable=False (reenviar
        um "atrasado" depois de reconectar não tem valor).
        """
        while True:
            try:
                response = await self._call_or_queue(
                    call.Heartbeat(), kind="Heartbeat", queueable=False
                )
                if response is not None:
                    # DEBUG: "ainda estou vivo" a cada ciclo, sem info
                    # nova — só aparece com --verbose.
                    self.log.debug(
                        f"Heartbeat enviado (intervalo atual: "
                        f"{self.state.current_heartbeat_interval}s)."
                    )
            except Exception:
                self.log.exception("[HEARTBEAT] erro inesperado — continuando no próximo ciclo.")
            await asyncio.sleep(self.state.current_heartbeat_interval)

    def _build_meter_values_request(self, voltage_now: float | None = None) -> "call.MeterValues":
        """
        Monta o payload de MeterValues a partir do estado atual —
        reutilizado por send_meter_values_loop e por
        on_trigger_message (amostra imediata). voltage_now opcional: o
        loop passa a leitura que já tirou, pra bater com a linha de log.
        """
        state = self.state
        if voltage_now is None:
            voltage_now = read_grid_voltage(self.config.nominal_voltage)
        return call.MeterValues(
            connector_id=self.config.connector_id,
            meter_value=[
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
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

    async def send_meter_values_loop(self, interval_seconds: int = 30):
        """
        Manda MeterValues periodicamente com a corrente "real" simulada
        — o que aparece no dashboard. Roda para sempre; offline, cada
        amostra é enfileirada e entregue em ordem na reconexão.

        Também tenta esvaziar a fila a cada ciclo se já estiver online —
        cobre o caso de uma mensagem "perdida" só por chaos_drop_rate
        (sem disconnect real), que senão ficaria presa até a próxima
        queda de conexão de verdade.
        """
        state = self.state
        while True:
            try:
                if self.is_online and state.offline_queue:
                    await self._flush_offline_queue()

                voltage_now = read_grid_voltage(self.config.nominal_voltage)
                await self._call_or_queue(
                    self._build_meter_values_request(voltage_now), kind="MeterValues"
                )

                power_kw = round((voltage_now * state.current_actual_amps) / 1000, 2)
                energy_kwh = round(state.energy_meter_wh / 1000, 2)

                has_session = state.active_transaction_id is not None
                suspended = state.session_suspended or state.evse_suspended_by_profile
                color = _meter_line_color(has_session, suspended, state.is_faulted, self.use_color)
                offline_marker = " 📡✗" if not self.is_online else ""
                reset = "\033[0m" if self.use_color else ""

                # INFO (visível por padrão, ao contrário do Heartbeat):
                # única linha que mostra o que está acontecendo de fato.
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
                if not self._try_begin_start():
                    self.log.warning(
                        "[CONSOLE] já existe um início de sessão em andamento "
                        "— aguarde confirmar antes de tentar de novo."
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

            # ── datatransfer <vendor_id> [message_id] [data...] ─────────
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

            # ── queue — mostra o que está pendente na fila offline ──────
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

            # ── disconnect — derruba a conexão de propósito (chaos manual) ──
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
        Start local (RFID no totem) — diferente do RemoteStart, precisa
        autorizar o id_tag antes de iniciar. Se estiver na lista local
        (SendLocalList), usa o status de lá direto, sem round-trip ao
        CSMS. Senão cai no Authorize remoto — e se estivermos offline
        nesse caso, recusa direto (Authorize precisa de resposta
        síncrona, não dá pra enfileirar).
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
        finally:
            # Cobre os returns antecipados acima (offline, Authorize sem
            # resposta, id_tag recusado) — nesses casos _send_start_transaction
            # nunca roda, então ninguém mais soltaria a reserva feita pelo
            # comando "start" no console antes de chamar esta função.
            # Chamar de novo depois de _send_start_transaction já ter
            # soltado é inofensivo (_end_start só zera a flag).
            self._end_start()

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
        # Via _call_or_queue (não self.call direto): offline, um fault
        # nunca chegava ao CSMS nem na reconexão (nunca era enfileirado).
        response = await self._call_or_queue(request, kind="StatusNotification(Faulted)")
        if response is not None:
            self.log.warning(
                f"⚠️  [FAULT] StatusNotification enviado: Faulted / {error_code.value} "
                "— use 'clear' para voltar a Available."
            )

    async def _send_fault_clear(self):
        """Limpa uma falha simulada — StatusNotification(Available, no_error)."""
        self.state.is_faulted = False
        await self.send_status_notification(ChargePointStatus.available)
        self.log.info("✅ [FAULT] Falha limpa — charger voltou para Available")

    async def _send_data_transfer(self, vendor_id: str, message_id: str | None, data: str | None):
        """
        Envia um DataTransfer arbitrário do charger para o CSMS (comando
        'datatransfer' do console). queueable=False: é um comando
        interativo de debug, a resposta é o próprio propósito de rodá-lo
        — não faz sentido enfileirar pra entregar minutos depois, sem
        ninguém olhando o terminal esperando a resposta. Ainda assim
        passa por _call_or_queue (em vez de self.call direto) para
        ganhar o timeout: antes, um CSMS que nunca respondesse deixava
        este comando pendurado pra sempre, sem erro nem log nenhum.
        """
        request = call.DataTransfer(vendor_id=vendor_id, message_id=message_id, data=data)
        response = await self._call_or_queue(request, kind="DataTransfer", queueable=False)
        if response is not None:
            self.log.info(
                f"[DATA TRANSFER] enviado | vendor_id={vendor_id} → "
                f"resposta: status={response.status} data={response.data!r}"
            )

    async def run_first_boot_sequence(self):
        """
        Primeira conexão: fica em Available até um RemoteStart/"start"
        local — _send_start_transaction avança pra Charging depois.

        Só avança para StatusNotification depois de um BootNotification
        Accepted — em Pending/Rejected um charger real não se apresenta
        como disponível, só fica retentando no intervalo que o CSMS mandou.
        """
        if not await self._boot_until_accepted():
            return  # ficou offline no meio das tentativas; main() reconecta e chama de novo
        await asyncio.sleep(1)
        await self.send_status_notification(ChargePointStatus.available)

    async def _boot_until_accepted(self) -> bool:
        """
        Repete BootNotification até Accepted, esperando entre tentativas
        o `interval` que o próprio CSMS mandou na resposta (fallback 10s
        se o CSMS não mandar nada útil). Para de tentar se a conexão cair
        no meio — quem trata a reconexão é o laço em main(), que chama
        run_reconnect_sequence (e portanto isso de novo) quando voltar.
        """
        while True:
            accepted, retry_after = await self.send_boot_notification()
            if accepted:
                return True
            if not self.is_online:
                return False
            await asyncio.sleep(retry_after)

    async def run_reconnect_sequence(self):
        """
        Reconexão da mesma instância (com todo o estado acumulado):
        reenvia BootNotification, esvazia a fila offline e informa o
        status atual do conector — que pode não ser Available se uma
        sessão continuou rodando durante a queda.
        """
        self.log.info(
            "[RECONEXÃO] reenviando BootNotification e esvaziando fila offline..."
        )
        if not await self._boot_until_accepted():
            return  # caiu de novo durante as tentativas; main() chama de novo ao reconectar
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
    """Painel de orientação impresso uma vez ao ligar (não a cada reconexão)."""
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
    Se configurado, derruba o WebSocket em intervalos (± jitter) — pra
    testar reconexão/fila offline sem derrubar o servidor manualmente.
    Roda para sempre; cada ciclo espera de novo antes da próxima queda.
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
    Loop de reconexão com backoff exponencial (2s -> 4s -> 8s ... até
    30s). A instância de EVChargerSim é criada UMA VEZ, na primeira
    conexão bem-sucedida, e persiste através de todas as reconexões —
    só a conexão WebSocket é trocada (`cp._connection = ws`), o que
    permite a uma sessão em andamento (SoC, energia, fila offline)
    sobreviver a uma queda de rede. Pelo mesmo motivo, os loops de
    fundo (heartbeat, meter values, acumulador, console, chaos) também
    são iniciados uma vez e rodam pra sempre.

    cp.start() (o listener desta conexão específica) é lançado como
    task ANTES de esperar o boot/reconexão, não depois — precisa estar
    rodando pra sequer entregar a resposta do próprio BootNotification
    (ver comentário no laço abaixo).
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
        listener_task = None
        try:
            async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
                logger.info("🔌 Conectado ao CSMS")

                first_connection = cp is None
                if first_connection:
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
                else:
                    cp._connection = ws
                    cp.is_online = True

                # cp.start() PRECISA rodar em paralelo com o boot/reconexão,
                # nunca depois — é o listener que entrega toda CALLRESULT
                # recebida (inclusive a resposta do próprio
                # BootNotification) pra quem está esperando via
                # self.call(). Chamar run_first_boot_sequence/
                # run_reconnect_sequence ANTES de start() estar rodando é
                # um deadlock: _boot_until_accepted não retorna sem uma
                # resposta, e a resposta nunca chega sem alguém lendo o
                # socket — trava pra sempre, e por tabela NADA MAIS
                # (heartbeat, meter values, Authorize, o que for) recebe
                # resposta nenhuma daí em diante, já que é o mesmo listener
                # que entrega tudo. (Isso passou despercebido antes porque
                # o boot original tentava só uma vez e seguia em frente
                # mesmo sem resposta; virou travamento permanente quando
                # o retry-até-Accepted foi adicionado.)
                listener_task = asyncio.create_task(cp.start())

                if first_connection:
                    await cp.run_first_boot_sequence()
                else:
                    await cp.run_reconnect_sequence()

                backoff = 2
                # Rotinas de fundo (heartbeat/meter/acumulador/console/chaos)
                # já rodam à parte desde a primeira conexão; só falta
                # esperar o listener desta conexão específica encerrar.
                await listener_task

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
            # Cobre saídas por exceção do boot/reconexão (não do próprio
            # listener) — sem isso, cp.start() ficaria rodando sozinho,
            # órfão, em cima de uma conexão que main() já desistiu.
            if listener_task is not None and not listener_task.done():
                listener_task.cancel()
                try:
                    await listener_task
                except (asyncio.CancelledError, Exception):
                    pass

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger("evchargersim").info("Simulador encerrado manualmente.")
