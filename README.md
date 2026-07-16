# EVChargerSim

Simulador de Charge Point OCPP 1.6J (usando [mobilityhouse/ocpp](https://github.com/mobilityhouse/ocpp)), pra testar a lógica do seu CSMS real sem precisar de hardware físico. Conecta via WebSocket, simula o lado carro/carregador (bateria, corrente, tapering) e responde à maior parte das mensagens que um CSMS manda em produção.

## Instalação

```bash
pip install ocpp websockets
```

## Uso básico

```bash
python evchargersim.py                        # ID padrão EVCHARGERSIM_01, conecta em ws://localhost:9000
python evchargersim.py CARREGADOR_02           # ID customizado
python evchargersim.py CARREGADOR_02 --url ws://192.168.15.18:9000
python evchargersim.py --config sim.json       # carrega valores padrão de um arquivo JSON
python evchargersim.py --verbose               # mostra Heartbeat e GetConfiguration no terminal
```

Para testar load balancing entre carregadores, abra um terminal por instância:

```bash
python evchargersim.py CARREGADOR_01
python evchargersim.py CARREGADOR_02
python evchargersim.py CARREGADOR_03
```

## Configuração

### Flags de linha de comando

| Flag | Descrição | Padrão |
|---|---|---|
| `charge_point_id` (posicional) | ID do charge point | `EVCHARGERSIM_01` |
| `--url` | URL base do CSMS (sem o ID no final) | `ws://localhost:9000` |
| `--config` | Caminho para um arquivo JSON com valores padrão (ver abaixo) | — |
| `--connector-id` | ID do conector | `1` |
| `--meter-interval` | Intervalo de MeterValues (segundos) | `30` |
| `--heartbeat-interval` | Intervalo inicial de Heartbeat (segundos) | `120` |
| `--default-amps` | Corrente aplicada ao iniciar sessão, antes do primeiro SetChargingProfile | `16.0` |
| `--sim-speed` | Fator de aceleração do acúmulo de energia/SoC (1.0 = tempo real) | `1.0` |
| `--battery-wh` | Capacidade da bateria simulada (Wh) | `50000` |
| `--initial-soc` | SoC inicial de cada sessão (%) | `20.0` |
| `--voltage` | Tensão nominal de referência (V) | `225.0` |
| `--call-timeout` | Timeout (segundos) para chamadas críticas (Start/StopTransaction) | `30.0` |
| `--verbose` | Mostra Heartbeat/GetConfiguration no terminal | desligado |

Flags de instabilidade de rede (chaos) — ver seção própria abaixo:

| Flag | Descrição | Padrão |
|---|---|---|
| `--chaos-disconnect-interval` | Derruba o WebSocket a cada N segundos (± jitter) | desligado (`0`) |
| `--chaos-disconnect-jitter` | Variação (± segundos) em torno do intervalo acima | `5.0` |
| `--chaos-latency-min` / `--chaos-latency-max` | Atraso artificial (ms) antes de cada envio | desligado (`0`/`0`) |
| `--chaos-drop-rate` | Probabilidade (0.0–1.0) de uma mensagem ser simulada como perdida | desligado (`0.0`) |

### Arquivo `--config`

JSON com qualquer subconjunto dos campos de `SimConfig` (mesmos nomes dos atributos, não das flags CLI). Exemplo:

```json
{
  "charge_point_id": "CARREGADOR_01",
  "battery_capacity_wh": 75000,
  "initial_soc_percent": 15,
  "simulation_speed": 20,
  "default_offered_amps": 32
}
```

**Precedência**: flag de CLI (quando passada) > arquivo `--config` > defaults embutidos. Uma chave desconhecida no arquivo faz o script abortar com uma mensagem de erro listando as chaves válidas.

## Cobertura do protocolo OCPP 1.6

**CSMS → Charge Point** (mensagens que o simulador responde):

`SetChargingProfile` (múltiplos períodos, unidades A e W) · `ClearChargingProfile` · `RemoteStartTransaction` · `RemoteStopTransaction` · `ChangeAvailability` (Operative/Inoperative, com `Scheduled` durante sessão ativa) · `Reset` (soft/hard) · `TriggerMessage` · `GetConfiguration` · `ChangeConfiguration` · `UnlockConnector` · `DataTransfer` · `GetDiagnostics` · `UpdateFirmware` · `ReserveNow` / `CancelReservation` (com expiração automática) · `SendLocalList` / `GetLocalListVersion`

**Charge Point → CSMS** (mensagens espontâneas): `BootNotification` · `StatusNotification` · `Heartbeat` · `MeterValues` (Current.Import/Offered, Voltage, Power.Active.Import, Energy.Active.Import.Register) · `StartTransaction` / `StopTransaction` · `Authorize` · `DiagnosticsStatusNotification` · `FirmwareStatusNotification` · `DataTransfer`

Não coberto: suporte a múltiplos conectores por charge point (um único conector por instância, hoje).

## Comandos do console

Digitados no terminal enquanto o simulador roda:

| Comando | Efeito |
|---|---|
| `start <id_tag>` | RFID local — autoriza via lista local (se o id_tag estiver nela) ou via `Authorize` remoto, e inicia a sessão. Recusado se o conector estiver reservado para outro id_tag, `Inoperative`, `Faulted`, ou (sem lista local) offline. |
| `stop` | Cliente encerra a sessão localmente (`Reason.ev_disconnected`) |
| `pause` | Carro pausa o carregamento → `SuspendedEV` |
| `resume` | Retoma o carregamento → `Charging` |
| `fault <código>` | Simula falha de hardware → `Faulted`. Códigos: `ground_failure`, `over_current_failure`, `over_voltage`, `connector_lock_failure`, `power_meter_failure`, `weak_signal`, `other_error` |
| `clear` | Limpa a falha ativa, volta a `Available` |
| `datatransfer <vendor_id> [message_id] [data]` | Envia um `DataTransfer` do charger para o CSMS |
| `queue` | Mostra o conteúdo da fila offline e o status de conectividade atual |
| `disconnect` | Derruba a conexão WebSocket na hora (gatilho manual de chaos) |
| `help` | Lista os comandos |

## Reconexão e fila de transações offline

Se a conexão com o CSMS cair (ou ele ainda não estiver de pé), o simulador reconecta automaticamente com backoff exponencial (2s → 4s → 8s... até um teto de 30s).

A mesma instância do simulador **persiste através de reconexões** — só a conexão WebSocket por baixo é trocada. Isso significa que:

- Uma sessão em andamento continua "fisicamente" rodando enquanto offline: SoC sobe, energia acumula, tudo como se o carro continuasse conectado (que é exatamente o que acontece num charger físico real).
- Mensagens que não puderam ser enviadas (`StatusNotification`, `MeterValues`, `StartTransaction`, `StopTransaction`) ficam numa fila local e são entregues ao CSMS **em ordem**, assim que a conexão volta.
- Um `start` local iniciado offline (via lista local — `Authorize` remoto não faz sentido enfileirar, precisa de resposta síncrona) roda a sessão com um **ID de transação temporário negativo**, até o CSMS confirmar um ID real. Qualquer `StopTransaction` enfileirado nesse meio-tempo é corrigido automaticamente para o ID real no momento do flush.
- `Heartbeat` não é enfileirado (não tem valor reenviar um "ainda estou vivo" atrasado) — é simplesmente pulado enquanto offline.

Use o comando de console `queue` para inspecionar o que está pendente a qualquer momento.

**Limitação conhecida**: o protocolo OCPP 1.6 não tem um mecanismo de deduplicação embutido. Se a conexão cair depois do CSMS já ter processado uma mensagem mas antes da confirmação chegar ao simulador, um reenvio no próximo flush pode registrar a mesma transação duas vezes do lado do servidor. Isso é uma limitação real do protocolo, não só deste simulador.

## Instabilidade de rede injetável (chaos)

Para testar a robustez do seu CSMS sem depender de uma queda real de rede:

```bash
# Derruba o WebSocket a cada ~60s (± jitter)
python evchargersim.py --chaos-disconnect-interval 60

# Atraso artificial de 200-2000ms antes de cada mensagem enviada
python evchargersim.py --chaos-latency-min 200 --chaos-latency-max 2000

# 10% das mensagens são simuladas como perdidas na rede
python evchargersim.py --chaos-drop-rate 0.1
```

As flags podem ser combinadas. O comando de console `disconnect` derruba a conexão manualmente a qualquer momento, sem precisar de `--chaos-disconnect-interval`.

Tudo isso é opt-in — desligado por padrão, sem mudar o comportamento de quem não passar essas flags.

## Multi-instância (load balancing)

Cada instância do simulador é independente — rode várias em paralelo (terminais separados, ou um `--config` diferente por instância) para simular um site com múltiplos carregadores.

## Testes

```bash
python -m unittest test_evchargersim.py -v
```

Cobre as funções puras (tapering de corrente, cores/formatação), o roteamento de perfis de carga com múltiplos períodos, `ChangeAvailability`, `ClearChargingProfile`, reserva/lista local, e o ciclo completo de fila offline (incluindo reconciliação de ID local → real e recuperação de uma queda no meio do flush). Não cobre os loops assíncronos diretamente (heartbeat/meter values/console) nem uma integração real contra um WebSocket — os testes substituem `self.call` por um stub e chamam os métodos diretamente.

## Limitações conhecidas

- **Um conector por charge point.** Simular uma estação com 2+ conectores exigiria `ChargerState` por conector e roteamento de `connector_id` em cada handler — não implementado.
- **`ClearChargingProfile`/`SetChargingProfile` não filtram por `chargingProfileId`/`stackLevel`/`connectorId`** — qualquer `SetChargingProfile` recebido substitui o perfil ativo por completo.
- **Sem deduplicação de mensagens reenviadas** pela fila offline (ver seção acima) — limitação do protocolo, não do simulador.
- **`GetDiagnostics`/`UpdateFirmware`/`DataTransfer` não passam pela fila offline** — se offline no momento em que o CSMS tentaria disparar esses fluxos, eles simplesmente não seriam recebidos (mas isso já é esperado: são iniciados pelo CSMS, que só consegue mandar a mensagem se a conexão estiver de pé).
