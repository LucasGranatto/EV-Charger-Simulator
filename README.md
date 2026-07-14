# EVChargerSim — Simulador de Charge Point OCPP 1.6J

Simulador standalone do lado "carro/carregador" de um ponto de carga AC
genérico, usando [mobilityhouse/ocpp](https://github.com/mobilityhouse/ocpp).
Conecta no seu CSMS real via WebSocket OCPP 1.6J, permitindo testar a
lógica do servidor (`SetChargingProfile`, `RemoteStartTransaction`,
balanceamento de carga, reconexão etc.) sem precisar de hardware físico.

---

## Pré-requisitos

| Requisito | Versão |
|---|---|
| Python | 3.10+ |
| ocpp | 2.1.0 |
| websockets | 16.0 |

```bash
pip install ocpp==2.1.0 websockets==16.0
```

---

## Como executar

```bash
python evchargersim.py
```

Isso conecta em `ws://localhost:9000` com o ID padrão `EVCHARGERSIM_01`.

### Opções de linha de comando

```bash
python evchargersim.py [charge_point_id] [--url URL] [--verbose]
```

| Argumento | Descrição |
|---|---|
| `charge_point_id` | ID do charge point (padrão: `EVCHARGERSIM_01`). Posicional e opcional. |
| `--url` | URL base do CSMS, sem o ID no final (padrão: `ws://localhost:9000`). O ID é sempre anexado automaticamente ao conectar. |
| `--verbose` | Mostra `Heartbeat` e `GetConfiguration` no terminal — por padrão ficam em nível `DEBUG` (silenciosos), já que repetem sem trazer informação nova a cada ciclo. `MeterValues` aparece sempre, independente desta flag. |

**Exemplos:**

```bash
python evchargersim.py CARREGADOR_02
python evchargersim.py CARREGADOR_02 --url ws://192.168.15.18:9000
python evchargersim.py --verbose
```

### Simulando múltiplos carregadores (load balancing)

Cada instância roda em seu próprio processo/terminal, com seu próprio ID:

```bash
python evchargersim.py CARREGADOR_01
python evchargersim.py CARREGADOR_02
python evchargersim.py CARREGADOR_03
```

---

## Comandos do console interativo

Durante a execução, digite no terminal:

| Comando | O que faz |
|---|---|
| `start <id_tag>` | Simula motorista passando RFID no totem (`Authorize` → `StartTransaction`), sem precisar de `RemoteStart` vindo do CSMS. |
| `stop` | Simula cliente encerrando a sessão localmente (cabo desconectado / botão no carro) — `Reason.ev_disconnected`. |
| `pause` | Simula o carro pausando o carregamento (→ `SuspendedEV`). |
| `resume` | Retoma o carregamento após um `pause` (→ `Charging`). |
| `fault <código>` | Dispara `StatusNotification` com erro, simulando uma falha de hardware. |
| `clear` | Limpa uma falha ativa, voltando para `Available`. Necessário depois de um `fault` para poder usar `start` de novo. |
| `help` | Lista todos os comandos. |

### Códigos de fault válidos

`ground_failure`, `over_current_failure`, `over_voltage`,
`connector_lock_failure`, `power_meter_failure`, `weak_signal`,
`other_error`

---

## Comportamento simulado

- **Bateria/SoC**: simula um EV de ~50 kWh começando em 20% a cada sessão,
  com *tapering* (queda de corrente conforme a bateria se aproxima de
  100%, mais perceptível acima de ~80% de SoC). Ao atingir 100%, encerra
  a sessão automaticamente.
- **RemoteStartTransaction**: não reautoriza localmente por padrão — isso
  é o comportamento correto da OCPP 1.6 quando `AuthorizeRemoteTxRequests`
  não está habilitado (o autor já é validado pelo backend antes do comando
  remoto ser disparado). Já o comando `start` do console (RFID local)
  *sempre* chama `Authorize.req` primeiro, como um totem físico faria.
- **HeartbeatInterval**: sincronizado com o CSMS via
  `ChangeConfiguration`/`GetConfiguration` — mudanças feitas pelo servidor
  têm efeito imediato no próximo ciclo.
- **Reconexão automática**: se a conexão com o CSMS cair (ou ele ainda não
  estiver no ar), o simulador tenta reconectar com backoff exponencial
  (2s → 4s → 8s ... até um teto de 30s), sem precisar reiniciar o
  processo manualmente.

---

## Aparência do terminal

- Timestamp, ID do charge point e nível de log (INFO/WARNING/ERROR) saem
  em cores diferentes da mensagem em si, para facilitar leitura. Desativa
  automaticamente quando a saída não é um terminal real (ex: redirecionada
  para um arquivo).
- `MeterValues` mostra uma barra de progresso do SoC (`🔋 [██████░░░░] 62%`)
  colorida conforme o estado: verde carregando, amarelo suspenso, cinza
  sem sessão, vermelho em falha.
- Eventos principais (conectado, sessão iniciada/encerrada, fault/clear,
  pause/resume) têm ícones (🔌⚡🛑⚠️✅⏸️▶️) para escanear o log de relance.

---

## Configurações editáveis

No topo do arquivo `evchargersim.py`:

| Constante | Descrição |
|---|---|
| `CONNECTOR_ID` | Conector simulado (padrão: 1). |
| `METER_VALUES_INTERVAL` | Intervalo de `MeterValues`, em segundos (padrão: 30). |
| `HEARTBEAT_INTERVAL` | Intervalo inicial de `Heartbeat`, em segundos (padrão: 120). |
| `DEFAULT_OFFERED_AMPS` | Corrente aplicada ao iniciar sessão, antes do primeiro `SetChargingProfile` (padrão: 16A). |
| `BATTERY_CAPACITY_WH` | Capacidade da bateria simulada (padrão: 50.000 Wh). |
| `INITIAL_SOC_PERCENT` | SoC inicial de cada sessão (padrão: 20%). |

---

## Requisitos futuros / limitações conhecidas

- `SIMULATION_SPEED` está declarado mas não tem efeito ainda — reservado
  para uma futura aceleração do acumulador de energia/SoC.
- Cada processo simula **um único** charge point. Para vários
  carregadores, rode uma instância por terminal (ver seção acima).
