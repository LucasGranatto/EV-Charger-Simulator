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
não precisa reiniciar o script manualmente.

Configurações editáveis estão no topo do arquivo (CONNECTOR_ID, intervalos,
parâmetros de bateria etc).

Comandos disponíveis no terminal durante a execução:
    start <id_tag>   -> simula motorista passando RFID no totem (Authorize →
                        StartTransaction), sem precisar de RemoteStart do CSMS
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
    help             -> lista todos os comandos
"""

import argparse
import asyncio
import logging
import random
import sys
from datetime import datetime, timezone

import websockets
from ocpp.routing import on
from ocpp.v16 import call, call_result
from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.v16.enums import (
    Action,
    AuthorizationStatus,
    AvailabilityStatus,
    ChargePointErrorCode,
    ChargePointStatus,
    Reason,
    RegistrationStatus,
    RemoteStartStopStatus,
    ResetType,
)

# ============================================================
# CONFIGURAÇÃO — ajuste conforme seu CSMS
# ============================================================
def _parse_args():
    parser = argparse.ArgumentParser(
        description="EVChargerSim — simulador standalone de Charge Point OCPP 1.6J.")
    parser.add_argument(
        "charge_point_id", nargs="?", default="EVCHARGERSIM_01",
        help="ID do charge point (padrão: EVCHARGERSIM_01). Permite rodar "
             "várias instâncias simultâneas, cada uma com seu próprio ID.")
    parser.add_argument(
        "--url", default="ws://localhost:9000",
        help="URL base do CSMS, SEM o charge point ID no final "
             "(padrão: ws://localhost:9000). O ID é sempre anexado "
             "automaticamente ao conectar.")
    parser.add_argument(
        "--verbose", action="store_true",
        help="Mostra Heartbeat e GetConfiguration no terminal (por padrão "
             "ficam em nível DEBUG, silenciosos, já que repetem sem trazer "
             "informação nova a cada ciclo). MeterValues aparece sempre, "
             "independente desta flag.")
    return parser.parse_args()


_args = _parse_args()

# CHARGE_POINT_ID e CSMS_URL vêm de linha de comando, permitindo rodar
# múltiplas instâncias deste script simultaneamente (cada uma com seu
# próprio ID e, se necessário, apontando pra um CSMS diferente) — sem
# isso, duas instâncias tentariam usar o mesmo ID e colidiriam no CSMS,
# ou precisariam editar o arquivo pra apontar pra outro servidor.
CHARGE_POINT_ID = _args.charge_point_id
CSMS_URL = _args.url
VERBOSE = _args.verbose
CONNECTOR_ID = 1                   # conector simulado (chargers AC residenciais tipicamente têm 1)

# Intervalos em segundos — valores padrão típicos de chargers AC reais
# (MeterValueSampleInterval=30, HeartbeatInterval varia por deployment).
# Ajuste aqui se quiser um ciclo mais rápido para debug pontual.
METER_VALUES_INTERVAL = 30
HEARTBEAT_INTERVAL = 120  # padrao realista (deployments reais tipicamente
                          # usam 30-300s; 120s e um meio-termo comum)

# Corrente padrão aplicada assim que uma sessão começa, antes de qualquer
# SetChargingProfile chegar do CSMS. Para carregadores AC residenciais:
#   16A @ 225V ≈ 3.6 kW  → bateria de 50 kWh (20→100%) em ~10h (realista)
#   32A @ 225V ≈ 7.2 kW  → mesma bateria em ~5h
# SetChargingProfile do CSMS sobrescreve esse valor normalmente — isso só
# garante que a sessão não fique parada em 0A esperando o perfil chegar.
DEFAULT_OFFERED_AMPS = 16.0

# Fator de aceleração da simulação. SIMULATION_SPEED = 1.0 é tempo real.
# Use valores maiores para testes que não precisam esperar horas reais:
#   6.0  → 1h de sessão simulada em 10min
#   60.0 → bateria 20→100% em ~10min (com 16A)
# Só afeta o acumulador de energia/SoC — heartbeat e MeterValues continuam
# sendo enviados no intervalo real (comportamento OCPP não muda).
SIMULATION_SPEED = 1.0

# Corrente "real" que o carregador simulado está entregando neste momento.
# Começa em 0 (sem carro) e é atualizada quando o CSMS manda SetChargingProfile.
current_offered_amps = 0.0
current_actual_amps = 0.0  # o que o "carro" simula estar de fato puxando

# Estado de sessão/transação — necessário para StartTransaction/StopTransaction,
# que são as mensagens que o CSMS usa para abrir/fechar sessão no banco de dados.
active_transaction_id = None
energy_meter_wh = 0.0  # contador de energia acumulada simulado (Wh)

# Intervalo de heartbeat ATUAL — pode ser alterado em runtime via
# ChangeConfiguration(key='HeartbeatInterval'). Separado da constante
# HEARTBEAT_INTERVAL (que é só o valor inicial); send_heartbeat_loop
# relê esta variável a cada ciclo, então a mudança tem efeito imediato.
current_heartbeat_interval = HEARTBEAT_INTERVAL

# ── SIMULAÇÃO DE BATERIA (SoC) ─────────────────────────────────────────
# Em carregamento AC real, a corrente entregue não fica simplesmente fixa
# em ~95% do limite oferecido a sessão inteira — ela cai conforme a
# bateria se aproxima da carga completa ("tapering"), efeito visível
# sobretudo acima de ~80% de SoC. Simulamos isso de forma simplificada:
# um EV de bateria média (~50 kWh, ex: faixa de um VW ID.3/Nissan Leaf).
BATTERY_CAPACITY_WH = 50_000.0
INITIAL_SOC_PERCENT = 20.0   # toda sessão começa com 20% para testes
                              # reproduzíveis. Mude aqui se quiser outro valor.
battery_soc_percent = INITIAL_SOC_PERCENT

# Flag de pausa — True enquanto o carro estiver em SuspendedEV.
# O energy_accumulator_loop respeita esse flag e para de acumular.
session_suspended = False

# Flag distinta de session_suspended acima: True enquanto o CSMS estiver
# impondo 0A via SetChargingProfile (ex: fila de espera do balanceamento
# de site) — SuspendedEVSE, e não SuspendedEV. São duas causas de
# suspensão diferentes (lado do carro vs. lado do equipamento) e cada
# uma tem seu próprio status OCPP.
evse_suspended_by_profile = False

# True entre um comando "fault" e um "clear" — enquanto ativo, o console
# recusa "start" (não faz sentido iniciar sessão num charger em Faulted)
# até o operador limpar a falha explicitamente, espelhando um charger
# físico real que não volta a Available sozinho após um erro de hardware.
is_faulted = False

# Mapa de nomes amigáveis (console) -> ChargePointErrorCode (OCPP).
FAULT_CODE_MAP = {
    "ground_failure":         ChargePointErrorCode.ground_failure,
    "over_current_failure":   ChargePointErrorCode.over_current_failure,
    "over_voltage":           ChargePointErrorCode.over_voltage,
    "connector_lock_failure": ChargePointErrorCode.connector_lock_failure,
    "power_meter_failure":    ChargePointErrorCode.power_meter_failure,
    "weak_signal":            ChargePointErrorCode.weak_signal,
    "other_error":            ChargePointErrorCode.other_error,
}

# Tensão nominal de referência para os cálculos de potência/energia.
# Faixa típica de rede monofásica/bifásica residencial (~220-230V). A
# função read_grid_voltage() adiciona uma pequena variação em torno
# desse valor para cada leitura, já que a rede elétrica real nunca fica
# perfeitamente constante.
NOMINAL_VOLTAGE = 225.0


def read_grid_voltage() -> float:
    """Simula pequena flutuação natural da tensão de rede (~±1.5V)."""
    return round(NOMINAL_VOLTAGE + random.uniform(-1.5, 1.5), 1)

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


_USE_COLOR = sys.stdout.isatty()

_handler = logging.StreamHandler()
_handler.setFormatter(_ColorFormatter(
    datefmt="%H:%M:%S",
    charge_point_id=CHARGE_POINT_ID,
    use_color=_USE_COLOR,
))
logging.basicConfig(level=logging.DEBUG if VERBOSE else logging.INFO, handlers=[_handler])
logger = logging.getLogger("evchargersim")

# A biblioteca ocpp loga CADA mensagem OCPP crua (send/receive, JSON
# completo) no logger "ocpp" em nível INFO — é isso que produz aqueles
# blocos gigantes de JSON quebrados em várias linhas no terminal,
# atropelando os logs legíveis deste script (ex: as linhas verdes de
# MeterValues). Subindo para WARNING, só erros/CALLError da lib
# aparecem; o tráfego OCPP completo continua sendo processado
# normalmente, só não é mais IMPRESSO. O mesmo já é feito do lado do
# CSMS real (ver dashboard_serverEV.py: install_log_handler()).
logging.getLogger("ocpp").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)


def compute_actual_current(offered_amps: float, soc_percent: float) -> float:
    """
    Calcula a corrente real que o "carro" puxaria dado o limite oferecido
    pelo CSMS e o estado de carga atual da bateria (SoC).

    Carregamento AC (diferente de DC rápido) tende a respeitar bem o
    limite oferecido na maior parte da curva — a redução por tapering só
    fica perceptível perto do fim (SoC alto), quando o carregador de
    bordo do veículo reduz a corrente para proteger a bateria.
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


def _soc_bar(soc_percent: float, width: int = 10) -> str:
    """
    Barra de progresso ASCII do SoC da bateria, tipo [██████░░░░] 62% —
    mais fácil de captar de relance do que só o número, especialmente
    útil pra notar visualmente o "tapering" (corrente caindo) perto do
    fim da barra sem precisar fazer a conta de cabeça.
    """
    filled = max(0, min(width, round(soc_percent / 100 * width)))
    return f"[{'█' * filled}{'░' * (width - filled)}] {soc_percent:5.1f}%"


def _meter_line_color(has_session: bool, suspended: bool, faulted: bool) -> str:
    """
    Cor da linha de MeterValues conforme o estado atual do charger —
    verde carregando normalmente, amarelo suspenso (carro ou CSMS
    pausou), cinza sem sessão, vermelho em Faulted. Sem isso, a linha
    de status mais frequente do terminal saía sempre na mesma cor,
    então "está carregando de verdade ou só suspenso?" exigia ler o
    texto todo em vez de notar pela cor.
    """
    if not _USE_COLOR:
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

    # --------------------------------------------------------
    # Handlers de mensagens recebidas do CSMS
    # --------------------------------------------------------

    @on(Action.set_charging_profile)
    async def on_set_charging_profile(self, connector_id, cs_charging_profiles, **kwargs):
        """
        Chamado quando o CSMS manda um novo perfil de carga (ex: limitar a 10A).
        Aqui simulamos o charge point "aceitando" e ajustando a corrente.
        """
        global current_offered_amps, current_actual_amps, evse_suspended_by_profile

        # O payload recebido via @on chega como dict puro, mas a lib converte
        # as chaves de camelCase (wire) para snake_case automaticamente.
        schedule = cs_charging_profiles["charging_schedule"]
        periods = schedule["charging_schedule_period"]

        if periods:
            # Pega o limite do primeiro período (cenário simples: perfil sem múltiplos períodos)
            limit = periods[0]["limit"]
            current_offered_amps = float(limit)
            current_actual_amps = compute_actual_current(
                current_offered_amps, battery_soc_percent
            )

            logger.info(
                f"[PERFIL RECEBIDO] connector={connector_id} | "
                f"limite oferecido={current_offered_amps}A | "
                f"corrente real (SoC {battery_soc_percent:.0f}%)={current_actual_amps}A"
            )

            # Reflete no StatusNotification quando o CSMS impõe 0A (ex:
            # fila de espera do balanceamento de site) ou restaura a
            # corrente depois — sem isso, o status ficava travado em
            # "Charging" no dashboard mesmo com a corrente zerada pelo
            # CSMS, já que nada mais dispararia uma StatusNotification
            # nova nesse caso. Só entra em jogo se houver sessão ativa e
            # o carro não estiver voluntariamente pausado (SuspendedEV
            # tem prioridade — são causas de suspensão diferentes).
            if active_transaction_id is not None and not session_suspended:
                if current_offered_amps <= 0.0 and not evse_suspended_by_profile:
                    evse_suspended_by_profile = True
                    logger.info(
                        "[PERFIL RECEBIDO] 0A imposto pelo CSMS → SuspendedEVSE")
                    asyncio.create_task(self.send_status_notification(
                        ChargePointStatus.suspended_evse))
                elif current_offered_amps > 0.0 and evse_suspended_by_profile:
                    evse_suspended_by_profile = False
                    logger.info(
                        "[PERFIL RECEBIDO] corrente restaurada pelo CSMS → Charging")
                    asyncio.create_task(self.send_status_notification(
                        ChargePointStatus.charging))
        else:
            logger.warning("SetChargingProfile recebido sem chargingSchedulePeriod")

        return call_result.SetChargingProfile(status="Accepted")

    @on(Action.remote_start_transaction)
    async def on_remote_start_transaction(self, id_tag, connector_id=None, **kwargs):
        logger.info(f"[REMOTE START] id_tag={id_tag} connector={connector_id}")
        # Dispara o envio de StartTransaction em background, DEPOIS de responder
        # Accepted — replica o fluxo real: o carregador aceita o comando e só
        # manda StartTransaction como mensagem separada um instante depois
        # (após fechar o contator / autorizar localmente).
        asyncio.create_task(
            self._send_start_transaction(connector_id or CONNECTOR_ID, id_tag)
        )
        return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.accepted)

    @on(Action.remote_stop_transaction)
    async def on_remote_stop_transaction(self, transaction_id, **kwargs):
        logger.info(f"[REMOTE STOP] transaction_id={transaction_id}")
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
        logger.info(f"[CHANGE AVAILABILITY] connector={connector_id} type={type}")
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
        logger.info(f"[RESET] type={type}")
        is_hard = (type == ResetType.hard)
        reason = Reason.hard_reset if is_hard else Reason.soft_reset

        if active_transaction_id is not None:
            logger.info(
                f"[RESET] sessão ativa (tx={active_transaction_id}) será "
                f"interrompida pelo reset"
            )
            asyncio.create_task(self._handle_reset_flow(active_transaction_id, reason, is_hard))
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
            logger.info("[RESET] hard reset — simulando reboot do firmware (5s)...")
            await asyncio.sleep(5)
            await self.send_boot_notification()
            await asyncio.sleep(1)
        else:
            logger.info("[RESET] soft reset — reinício rápido do software (1s)...")
            await asyncio.sleep(1)

        await self.send_status_notification(ChargePointStatus.available)
        logger.info("[RESET] concluído — carregador disponível novamente")

    @on(Action.trigger_message)
    async def on_trigger_message(self, requested_message, connector_id=None, **kwargs):
        """
        TriggerMessage pede para o carregador reenviar uma mensagem
        espontaneamente (ex: StatusNotification, Heartbeat). Usado pelo
        status_check() do CSMS real para forçar uma atualização de estado.
        """
        logger.info(f"[TRIGGER MESSAGE] requested={requested_message} connector={connector_id}")
        if requested_message == "StatusNotification":
            current_status = (
                ChargePointStatus.charging if active_transaction_id is not None
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
        em uso (current_heartbeat_interval), não um número fixo. O
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
        logger.debug(f"[GET CONFIGURATION] keys solicitadas={key}")
        all_config = [
            {"key": "HeartbeatInterval", "readonly": False, "value": str(current_heartbeat_interval)},
            {"key": "MeterValueSampleInterval", "readonly": False, "value": str(METER_VALUES_INTERVAL)},
            {"key": "ConnectorPhaseRotation", "readonly": True, "value": "NotApplicable"},
            {"key": "NumberOfConnectors", "readonly": True, "value": "1"},
            {"key": "SupportedFeatureProfiles", "readonly": True, "value": "Core,SmartCharging"},
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
        global current_heartbeat_interval
        logger.info(f"[CHANGE CONFIGURATION] key={key} value={value}")

        if key == "HeartbeatInterval":
            try:
                current_heartbeat_interval = int(value)
                logger.info(
                    f"[HEARTBEAT] intervalo atualizado para "
                    f"{current_heartbeat_interval}s — efeito no próximo ciclo"
                )
            except ValueError:
                logger.warning(f"[CHANGE CONFIGURATION] valor inválido para HeartbeatInterval: {value}")
                return call_result.ChangeConfiguration(status="Rejected")
        # Outras chaves (ex: MeterValueSampleInterval) são aceitas mas não
        # têm efeito simulado — o intervalo de MeterValues deste simulador
        # é fixo via METER_VALUES_INTERVAL no topo do arquivo, já que não
        # é esse o foco do bug reportado. Expanda aqui se precisar testar
        # mudança desse valor especificamente.

        return call_result.ChangeConfiguration(status="Accepted")

    # --------------------------------------------------------
    # Rotinas que o charge point envia PARA o CSMS
    # --------------------------------------------------------

    async def send_boot_notification(self):
        global battery_soc_percent, is_faulted
        battery_soc_percent = INITIAL_SOC_PERCENT
        # Um (re)boot é um bom ponto pra limpar uma falha simulada — cada
        # nova conexão começa "limpa" em vez de herdar Faulted de uma
        # sessão de simulador anterior (ex: depois de uma reconexão
        # automática).
        is_faulted = False
        logger.info(f"[BATERIA] SoC inicial: {_soc_bar(battery_soc_percent)}")

        request = call.BootNotification(
            charge_point_model="EVChargerSim",
            charge_point_vendor="EVChargerSim",
            firmware_version="SIM-1.0",
        )
        response = await self.call(request)
        if response.status == RegistrationStatus.accepted:
            logger.info("BootNotification aceito pelo CSMS.")
        else:
            logger.warning(f"BootNotification respondido com status: {response.status}")

    async def send_status_notification(self, status: str):
        request = call.StatusNotification(
            connector_id=CONNECTOR_ID,
            error_code=ChargePointErrorCode.no_error,
            status=status,
        )
        await self.call(request)
        logger.info(f"StatusNotification enviado: {status}")

    async def _send_start_transaction(self, connector_id: int, id_tag: str):
        """
        Envia StartTransaction simulando o carregador autorizando e fechando
        o contator. Sem isso, o CSMS nunca recebe um transaction_id e a
        sessão nunca é registrada (nem no dashboard, nem no banco).
        """
        global active_transaction_id, current_offered_amps, current_actual_amps
        global battery_soc_percent, energy_meter_wh, session_suspended

        try:
            # Cada nova sessão reseta SoC e medidor, evitando que sessões
            # sucessivas encadeiem o estado da sessão anterior.
            battery_soc_percent = INITIAL_SOC_PERCENT
            energy_meter_wh = 0.0
            session_suspended = False
            logger.info(f"[BATERIA] SoC inicial desta sessão: {_soc_bar(battery_soc_percent)}")

            # Aplica a corrente padrão residencial imediatamente, antes de
            # qualquer SetChargingProfile chegar do CSMS. Sem isso, a sessão
            # começa em 0A e fica sem acumular energia até o CSMS reagir —
            # o que é artificial, pois um carregador físico começa a entregar
            # corrente assim que o contator fecha. O CSMS ainda pode sobrescrever
            # este valor com SetChargingProfile a qualquer momento.
            current_offered_amps = DEFAULT_OFFERED_AMPS
            current_actual_amps = compute_actual_current(
                current_offered_amps, battery_soc_percent
            )
            logger.info(
                f"[SESSION] Corrente inicial: {current_offered_amps:.0f}A oferecido "
                f"/ {current_actual_amps:.1f}A real (aguardando SetChargingProfile do CSMS)"
            )

            # Simula o veículo sendo conectado e o carregador preparando
            # a sessão (LED branco piscando, conector travado etc).
            await self.send_status_notification(ChargePointStatus.preparing)
            await asyncio.sleep(1)  # simula o pequeno delay real de fechamento do contator

            request = call.StartTransaction(
                connector_id=connector_id,
                id_tag=id_tag,
                meter_start=int(energy_meter_wh),
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            response = await self.call(request)
            active_transaction_id = response.transaction_id
            logger.info(
                f"⚡ [START TRANSACTION] aceito pelo CSMS | "
                f"transaction_id={active_transaction_id} | id_tag={id_tag}"
            )

            # Nota: não definimos uma corrente "chute inicial" aqui. O CSMS real
            # (visto em main.py/run_tests) já envia um SetChargingProfile logo
            # após o boot — é esse comando que vai popular current_offered_amps
            # de forma correta, refletindo o limite configurado de verdade.
            # Até esse comando chegar, o carregador fica em Charging com 0A,
            # que é exatamente o que acontece num charger AC real entre o
            # fechamento do contator e a aplicação do primeiro perfil.

            await self.send_status_notification(ChargePointStatus.charging)
        except Exception:
            # Sem isso, uma falha aqui (ex: conexão caiu nesse meio tempo)
            # morre silenciosamente — a task roda em segundo plano via
            # create_task e ninguém nunca dá "await" nela para propagar o erro.
            logger.exception(
                "[START TRANSACTION] FALHOU ao enviar — sessão NÃO foi "
                "registrada no CSMS. Verifique se a conexão ainda está ativa."
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
        global active_transaction_id, current_offered_amps, current_actual_amps
        global session_suspended, evse_suspended_by_profile

        try:
            await asyncio.sleep(0.5)

            request = call.StopTransaction(
                meter_stop=int(energy_meter_wh),
                timestamp=datetime.now(timezone.utc).isoformat(),
                transaction_id=transaction_id,
                reason=reason,
            )
            await self.call(request)
            logger.info(
                f"🛑 [STOP TRANSACTION] enviado | transaction_id={transaction_id}"
                + (f" | motivo={reason.value}" if reason else "")
            )

            active_transaction_id = None
            current_offered_amps = 0.0
            current_actual_amps = 0.0
            session_suspended = False
            evse_suspended_by_profile = False

            if skip_status_flow:
                return

            # Sequência realista de encerramento: Finishing (carregador
            # liberando o conector / EV ainda fisicamente plugado por um
            # instante) e, pouco depois, Available (pronto para o próximo
            # veículo). Sem isso, o conector ficava "presto" em Charging
            # mesmo sem nenhuma sessão ativa, e o MeterValues continuava
            # sendo reportado como se ainda houvesse carregamento.
            await self.send_status_notification(ChargePointStatus.finishing)
            await asyncio.sleep(2)
            await self.send_status_notification(ChargePointStatus.available)
        except Exception:
            # Mesmo cuidado do _send_start_transaction: sem isso, a sessão
            # fica "pendurada" no banco (started_at preenchido, stopped_at
            # nulo para sempre) sem nenhum aviso de que algo falhou.
            logger.exception(
                "[STOP TRANSACTION] FALHOU ao enviar — a sessão "
                f"(transaction_id={transaction_id}) vai continuar 'pendurada' "
                "como ativa no banco de dados até ser encerrada manualmente."
            )

    async def energy_accumulator_loop(self, interval_seconds: int = 30):
        """
        Acumula energia (Wh) enquanto há transação ativa e não suspensa.
        A cada ciclo avança o SoC e recalcula a corrente (tapering).
        Ao atingir 100%, manda StopTransaction automaticamente — simula o
        EV sinalizando que não aceita mais carga (BMS cheio).
        """
        global energy_meter_wh, battery_soc_percent, current_actual_amps
        while True:
            await asyncio.sleep(interval_seconds)

            if active_transaction_id is None:
                continue
            if session_suspended or current_actual_amps <= 0:
                continue

            power_w = NOMINAL_VOLTAGE * current_actual_amps
            energy_delta_wh = power_w * (interval_seconds / 3600)
            energy_meter_wh += energy_delta_wh

            battery_soc_percent = min(
                100.0,
                battery_soc_percent + (energy_delta_wh / BATTERY_CAPACITY_WH) * 100,
            )
            current_actual_amps = compute_actual_current(
                current_offered_amps, battery_soc_percent
            )

            if battery_soc_percent >= 100.0:
                current_actual_amps = 0.0
                logger.info(
                    "[BATERIA] SoC atingiu 100% — EV sinalizou bateria cheia. "
                    "Encerrando sessão automaticamente (Reason.ev_disconnected)."
                )
                asyncio.create_task(
                    self._send_stop_transaction(
                        active_transaction_id, reason=Reason.ev_disconnected
                    )
                )

    async def send_heartbeat_loop(self):
        """
        O intervalo usado é current_heartbeat_interval (variável global),
        relido a cada ciclo — assim, uma mudança de HeartbeatInterval feita
        via CSMS (on_change_configuration) tem efeito já no próximo
        heartbeat, sem precisar reiniciar o simulador.
        """
        while True:
            await self.call(call.Heartbeat())
            # DEBUG, não INFO: essa linha nunca traz informação nova (é
            # literalmente "ainda estou vivo" a cada ciclo) — só aparece
            # no terminal com --verbose. Use --verbose se precisar
            # confirmar visualmente que o heartbeat está saindo no
            # intervalo certo.
            logger.debug(f"Heartbeat enviado (intervalo atual: {current_heartbeat_interval}s).")
            await asyncio.sleep(current_heartbeat_interval)

    async def send_meter_values_loop(self, interval_seconds: int = 30):
        """
        Manda MeterValues periodicamente reportando a corrente "real" simulada.
        Isso é o que vai aparecer no seu dashboard como se fosse o charger reportando.
        """
        while True:
            timestamp = datetime.now(timezone.utc).isoformat()
            voltage_now = read_grid_voltage()
            request = call.MeterValues(
                connector_id=CONNECTOR_ID,
                meter_value=[
                    {
                        "timestamp": timestamp,
                        "sampledValue": [
                            {
                                "value": str(current_actual_amps),
                                "context": "Sample.Periodic",
                                "measurand": "Current.Import",
                                "unit": "A",
                            },
                            {
                                "value": str(current_offered_amps),
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
                                "value": str(round(voltage_now * current_actual_amps, 1)),
                                "context": "Sample.Periodic",
                                "measurand": "Power.Active.Import",
                                "unit": "W",
                            },
                            {
                                "value": str(int(energy_meter_wh)),
                                "context": "Sample.Periodic",
                                "measurand": "Energy.Active.Import.Register",
                                "unit": "Wh",
                            },
                        ],
                    }
                ],
            )
            await self.call(request)

            power_kw = round((voltage_now * current_actual_amps) / 1000, 2)
            energy_kwh = round(energy_meter_wh / 1000, 2)

            has_session = active_transaction_id is not None
            suspended = session_suspended or evse_suspended_by_profile
            color = _meter_line_color(has_session, suspended, is_faulted)
            reset = "\033[0m" if _USE_COLOR else ""

            # INFO (visível por padrão) — diferente do Heartbeat, esta é a
            # única linha que mostra o que está de fato acontecendo com a
            # sessão (SoC, corrente, potência), então vale ficar visível
            # sem precisar de --verbose. Formato compacto: sem as palavras
            # "ofertado"/"acumulado" ocupando espaço — a barra de unidades
            # (A, kW, kWh) já deixa claro o que é o quê, e a notação
            # "real/oferecido" com "/" é mais rápida de escanear do que
            # dois blocos de texto separados por "|".
            if has_session:
                logger.info(
                    f"{color}🔋 {_soc_bar(battery_soc_percent)}  "
                    f"⚡ {current_actual_amps:4.1f}/{current_offered_amps:4.1f}A  "
                    f"{power_kw:5.2f}kW  Σ{energy_kwh:6.2f}kWh{reset}"
                )
            else:
                logger.info(f"{color}🔋 sem sessão ativa{reset}")

            await asyncio.sleep(interval_seconds)

    async def console_command_loop(self):
        """
        Lê comandos do terminal em background (via run_in_executor para não
        bloquear o event loop) e simula ações locais do motorista/carro —
        eventos que nunca chegam via CSMS, mas que um charger físico real
        geraria sozinho.
        """
        global session_suspended

        loop = asyncio.get_running_loop()
        logger.info(
            "[CONSOLE] Pronto. Comandos: start <id_tag> | stop | pause | "
            "resume | fault <código> | clear | help"
        )
        # Prompt visível (">> ") em vez de input() sem marcador nenhum —
        # sem isso, era fácil perder de vista onde exatamente o terminal
        # esperava você digitar algo no meio do stream de heartbeats e
        # meter values rolando por cima. Nota: como o prompt é escrito
        # pelo input() da thread do executor, ele ainda pode ficar
        # visualmente "cortado" por uma linha de log que chega bem no
        # instante em que ele é impresso — cosmético, sem efeito na
        # leitura do comando em si.
        prompt = "\033[32m>> \033[0m" if _USE_COLOR else ">> "
        while True:
            raw = await loop.run_in_executor(None, input, prompt)
            parts = raw.strip().split()
            if not parts:
                continue
            cmd = parts[0].lower()

            # ── start <id_tag> ──────────────────────────────────────────
            # Simula o motorista passando o RFID no totem: o carregador
            # chama Authorize localmente e, se aceito, inicia a transação —
            # sem precisar de RemoteStartTransaction vindo do CSMS.
            if cmd == "start":
                if active_transaction_id is not None:
                    logger.warning("[CONSOLE] Já existe uma sessão ativa.")
                    continue
                if is_faulted:
                    logger.warning(
                        "[CONSOLE] Charger em Faulted — rode 'clear' antes "
                        "de iniciar uma nova sessão."
                    )
                    continue
                id_tag = parts[1] if len(parts) > 1 else "LOCAL_TAG"
                logger.info(
                    f"[CONSOLE] RFID local: autorizando id_tag='{id_tag}' ..."
                )
                asyncio.create_task(
                    self._local_start_flow(CONNECTOR_ID, id_tag)
                )

            # ── stop ────────────────────────────────────────────────────
            # Motorista desconectou o cabo ou apertou parar no carro.
            # Reason.ev_disconnected é diferente de Reason.remote (que é
            # quando o STOP vem do CSMS via RemoteStopTransaction).
            elif cmd == "stop":
                if active_transaction_id is None:
                    logger.warning("[CONSOLE] Nenhuma sessão ativa para encerrar.")
                    continue
                logger.info(
                    f"[CONSOLE] Encerrando sessão pelo cliente "
                    f"(tx={active_transaction_id})"
                )
                asyncio.create_task(
                    self._send_stop_transaction(
                        active_transaction_id, reason=Reason.ev_disconnected
                    )
                )

            # ── pause ───────────────────────────────────────────────────
            # Carro pausou o carregamento (timer do veículo, bateria
            # aquecendo, etc). StatusNotification → SuspendedEV.
            # O accumulator para de somar energia enquanto suspenso.
            elif cmd == "pause":
                if active_transaction_id is None:
                    logger.warning("[CONSOLE] Nenhuma sessão ativa para pausar.")
                    continue
                if session_suspended:
                    logger.warning("[CONSOLE] Sessão já está suspensa.")
                    continue
                session_suspended = True
                logger.info("⏸️  [CONSOLE] Carregamento pausado → SuspendedEV")
                asyncio.create_task(
                    self.send_status_notification(ChargePointStatus.suspended_ev)
                )

            # ── resume ──────────────────────────────────────────────────
            # Carro retomou o carregamento após pause.
            elif cmd == "resume":
                if active_transaction_id is None:
                    logger.warning("[CONSOLE] Nenhuma sessão ativa para retomar.")
                    continue
                if not session_suspended:
                    logger.warning("[CONSOLE] Sessão não está suspensa.")
                    continue
                session_suspended = False
                logger.info("▶️  [CONSOLE] Carregamento retomado → Charging")
                asyncio.create_task(
                    self.send_status_notification(ChargePointStatus.charging)
                )

            # ── fault <código> ──────────────────────────────────────────
            # Dispara StatusNotification com Faulted + error code, simulando
            # uma falha de hardware (ex: fusível queimado, falha de aterramento).
            # O CSMS deve detectar isso e marcar o conector como indisponível.
            elif cmd == "fault":
                code_str = parts[1].lower() if len(parts) > 1 else ""
                error_code = FAULT_CODE_MAP.get(code_str)
                if error_code is None:
                    logger.warning(
                        f"[CONSOLE] Código de falha desconhecido: '{code_str}'. "
                        f"Válidos: {', '.join(FAULT_CODE_MAP)}"
                    )
                    continue
                logger.warning(
                    f"[CONSOLE] Simulando falha: {error_code.value}"
                )
                asyncio.create_task(
                    self._send_fault_notification(error_code)
                )

            # ── clear ───────────────────────────────────────────────────
            # Limpa uma falha simulada e volta para Available. Necessário
            # depois de um "fault" — um charger físico real não sai de
            # Faulted sozinho, e sem esse comando o único jeito era matar
            # e reiniciar o processo inteiro (derrubando a conexão WS).
            elif cmd == "clear":
                if not is_faulted:
                    logger.warning("[CONSOLE] Nenhuma falha ativa para limpar.")
                    continue
                asyncio.create_task(self._send_fault_clear())

            elif cmd == "help":
                logger.info(
                    "[CONSOLE] Comandos:\n"
                    "  start <id_tag>   — RFID local (Authorize → StartTransaction)\n"
                    "  stop             — cliente encerra sessão (ev_disconnected)\n"
                    "  pause            — carro pausa carregamento (SuspendedEV)\n"
                    "  resume           — carro retoma carregamento (Charging)\n"
                    "  fault <código>   — simula falha de hardware (Faulted)\n"
                    f"  códigos de fault: {', '.join(FAULT_CODE_MAP)}\n"
                    "  clear            — limpa a falha ativa (volta a Available)\n"
                    "  help             — esta mensagem"
                )
            elif cmd:
                logger.warning(f"[CONSOLE] Comando desconhecido: '{cmd}'. Digite 'help'.")

    async def _local_start_flow(self, connector_id: int, id_tag: str):
        """
        Fluxo de start iniciado localmente pelo motorista (RFID no totem).
        Diferente do RemoteStart (que vem do CSMS pronto para iniciar):
        aqui o carregador precisa primeiro pedir autorização ao CSMS via
        Authorize, e só então iniciar a transação se a resposta for Accepted.
        """
        try:
            auth_request = call.Authorize(id_tag=id_tag)
            auth_response = await self.call(auth_request)
            status = auth_response.id_tag_info.get("status", "Invalid")

            if status != AuthorizationStatus.accepted:
                logger.warning(
                    f"[LOCAL START] id_tag='{id_tag}' não autorizado pelo CSMS "
                    f"(status={status}). Sessão não iniciada."
                )
                return

            logger.info(
                f"[LOCAL START] id_tag='{id_tag}' autorizado → iniciando transação"
            )
            await self._send_start_transaction(connector_id, id_tag)
        except Exception:
            logger.exception("[LOCAL START] Falha no fluxo de autorização local.")

    async def _send_fault_notification(self, error_code: ChargePointErrorCode):
        """
        Envia StatusNotification com status Faulted e o error_code informado.
        Se havia sessão ativa, encerra com Reason.other — comportamento real:
        um carregador que falha não pode simplesmente continuar a sessão,
        então manda StopTransaction antes de reportar o fault.
        """
        global current_offered_amps, current_actual_amps, is_faulted

        if active_transaction_id is not None:
            logger.warning(
                f"[FAULT] Sessão ativa (tx={active_transaction_id}) será "
                "encerrada pelo fault antes de reportar o erro."
            )
            await self._send_stop_transaction(
                active_transaction_id,
                reason=Reason.other,
                skip_status_flow=True,
            )

        current_offered_amps = 0.0
        current_actual_amps = 0.0
        is_faulted = True

        request = call.StatusNotification(
            connector_id=CONNECTOR_ID,
            error_code=error_code,
            status=ChargePointStatus.faulted,
        )
        await self.call(request)
        logger.warning(
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
        global is_faulted
        is_faulted = False
        await self.send_status_notification(ChargePointStatus.available)
        logger.info("✅ [FAULT] Falha limpa — charger voltou para Available")

    async def simulate_connection_flow(self):
        """
        Simula o boot do carregador. Importante: fica em 'Available' (sem
        veículo conectado) até receber um RemoteStartTransaction — é
        _send_start_transaction que avança para Preparing -> Charging,
        e _send_stop_transaction que volta para Available ao final.
        """
        await self.send_boot_notification()
        await asyncio.sleep(1)
        await self.send_status_notification(ChargePointStatus.available)


def _print_banner():
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
        f"  Charge Point ID   : {CHARGE_POINT_ID}",
        f"  CSMS              : {CSMS_URL}/{CHARGE_POINT_ID}",
        f"  Conector          : {CONNECTOR_ID}",
        f"  Bateria simulada  : {BATTERY_CAPACITY_WH / 1000:.1f} kWh"
        f" | SoC inicial: {INITIAL_SOC_PERCENT:.0f}%",
        f"  Heartbeat         : {HEARTBEAT_INTERVAL}s"
        f" | MeterValues: {METER_VALUES_INTERVAL}s"
        f" | Corrente padrão: {DEFAULT_OFFERED_AMPS:.0f}A",
        bar,
    ]
    if _USE_COLOR:
        cyan, reset = "\033[36m", "\033[0m"
        lines = [f"{cyan}{line}{reset}" for line in lines]
    print("\n".join(lines))


async def _run_session():
    """
    Uma única tentativa de conexão + sessão completa com o CSMS. Se a
    conexão cair (ou nunca se estabelecer), a exceção sobe para quem
    chamou decidir se tenta de novo — ver main().
    """
    url = f"{CSMS_URL}/{CHARGE_POINT_ID}"
    logger.info(f"Conectando em {url} ...")

    async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
        logger.info("🔌 Conectado ao CSMS")
        cp = EVChargerSim(CHARGE_POINT_ID, ws)

        # Roda o "listener" do protocolo (escuta mensagens do CSMS) em paralelo
        # com as rotinas de envio (boot, status, heartbeat, meter values).
        #
        # Nota: console_command_loop é recriado a cada (re)conexão. O
        # input() dele roda numa thread do executor padrão que não pode
        # ser cancelada de verdade (mesma limitação documentada em
        # logging_setupEV.async_input) — numa reconexão ela fica parada
        # em segundo plano até o usuário digitar algo, e só então é
        # descartada. Inofensivo no uso normal (algumas reconexões
        # durante um teste manual); não é pensado para reconectar em
        # loop rápido e indefinido sem nunca reiniciar o processo.
        await asyncio.gather(
            cp.start(),
            cp.simulate_connection_flow(),
            cp.send_heartbeat_loop(),
            cp.send_meter_values_loop(interval_seconds=METER_VALUES_INTERVAL),
            cp.energy_accumulator_loop(interval_seconds=METER_VALUES_INTERVAL),
            cp.console_command_loop(),
        )


async def main():
    """
    Loop de reconexão com backoff exponencial (2s -> 4s -> 8s ... até um
    teto de 30s, resetando assim que uma conexão fica de pé com sucesso).
    Espelha o comportamento de um charger físico real — e do próprio
    CSMS (ver start_sync_loop em charger.py): se o servidor cair ou
    ainda não estiver no ar, tenta de novo em vez de derrubar o
    processo. Sem isso, testar cenários de reconexão no dashboard exigia
    rodar o script manualmente de novo a cada queda.
    """
    _print_banner()
    backoff = 2
    max_backoff = 30
    while True:
        try:
            await _run_session()
            logger.warning("Conexão encerrada pelo CSMS — tentando reconectar...")
            backoff = 2
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

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Simulador encerrado manualmente.")
