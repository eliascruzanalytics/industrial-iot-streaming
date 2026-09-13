# Documentação dos Consumidores Streaming — Industrial IoT Data Pipeline

Este documento detalha os **5 consumidores de dados streaming** desenvolvidos para a arquitetura Industrial IoT. Cada consumidor opera com um `group_id` Kafka independente para garantir isolamento de falhas, processamento paralelo e especialização de responsabilidades.

---

## Arquitetura e Visão Geral dos Consumidores

```text
                                  KAFKA BROKER
                             (iot-machine-events)
                                       │
         ┌──────────────────┬──────────┴───────────┬──────────────────┐
         │                  │                      │                  │
         ▼                  ▼                      ▼                  ▼
┌─────────────────┐┌─────────────────┐  ┌─────────────────┐┌─────────────────┐
│ AlertConsumer   ││ MetricsConsumer │  │ ArchiveConsumer ││ MaintenanceCons.│
│ (Group: alert)  ││ (Group: metrics)│  │ (Group: archive)││ (Group: maint.) │
└────────┬────────┘└────────┬────────┘  └────────┬────────┘└────────┬────────┘
         │                  │                      │                  │
         ▼                  ▼                      ▼                  ▼
  Alertas Críticos   Métricas Janeladas     Data Lake Bronze   Ciclo de Vida &
 (data/alerts/)     (data/metrics/)        (data/bronze/)     Ordens de Serviço
                                                              (data/maint./)

                               QUARENTENA (DLQ)
                               (data/quarantine/)
                                       │
                                       ▼
                               ┌─────────────────┐
                               │ DlqConsumer     │
                               │ (Monitor DLQ)   │
                               └────────┬────────┘
                                        │
                                        ▼
                                 Métricas de DQ
```

---

## Detalhamento dos 5 Consumidores

### 1. `AlertConsumer` (`consumers/alert_consumer.py`)
- **Grupo Kafka**: `alert-consumer-group`
- **Tópico**: `iot-machine-events`
- **Filtro de Condição**:
  - `temperature > 90.0°C`
  - `vibration > 15.0 mm/s`
  - `pressure > 7.0 bar`
  - `status == 'FAILURE'`
  - `anomaly_level == 'CRITICAL'`
- **Saída**: Registra log de emergência com destaque visual e persiste payload em `data/alerts/alerts.jsonl`.

---

### 2. `MetricsConsumer` (`consumers/metrics_consumer.py`)
- **Grupo Kafka**: `metrics-consumer-group`
- **Tópico**: `iot-machine-events`
- **Função**:
  - Computa médias móveis (temperatura, pressão, vibração).
  - Calcula mínimos e máximos por dispositivo.
  - Mantém a contagem total de eventos e distribuição de status operacionais (`RUNNING`, `STOPPED`, etc.).
- **Saída**: Atualiza periodicamente o arquivo `data/metrics/operational_metrics.json`.

---

### 3. `ArchiveConsumer` (`consumers/archive_consumer.py`)
- **Grupo Kafka**: `archiver-consumer-group`
- **Tópico**: `iot-machine-events`
- **Função**:
  - Consome dados brutos e persiste no Data Lake Bronze.
  - Particiona o armazenamento por ano/mês/dia (`data/bronze/year=YYYY/month=MM/day=DD/raw_telemetry.jsonl`).
- **Saída**: Armazenamento bruto particionado e imutável para auditoria e histórico.

---

### 4. `DlqConsumer` (`consumers/dlq_consumer.py`)
- **Origem dos Dados**: Diretório de Quarentena (`data/quarantine/`) ou tópico DLQ.
- **Função**:
  - Categoriza falhas de validação de Data Quality (`temperature_out_of_range`, `event_id_null`, `invalid_status`).
  - Monitora taxas de erro por dispositivo e código de erro.
  - Dispara alerta quando o volume de erros ultrapassa o limite tolerado.
- **Saída**: Atualiza o relatório `data/quarantine/dlq_summary.json`.

---

### 5. `MaintenanceConsumer` (`consumers/maintenance_consumer.py`)
- **Grupo Kafka**: `maintenance-consumer-group`
- **Tópico**: `iot-machine-events`
- **Função**:
  - Monitora vibração acumulada, ciclos em alta temperatura e estresse mecânico.
  - Calcula o *Health Score* da máquina (100% a 0%).
  - Emite ordens de manutenção preventiva (`WO-MTR-XXX`) antes que ocorra quebra do equipamento.
- **Saída**: Atualiza o arquivo de estado `data/maintenance/equipment_lifecycle.json`.

---

## Como Executar os Consumidores

### Execução de Demonstração Local (Sem dependência de Kafka ativo)
Para testar o funcionamento de todos os consumidores offline com eventos de amostra:

```bash
python consumers/run_all_consumers.py --once
```

Para testar um consumidor específico em modo amostra:
```bash
python consumers/run_all_consumers.py --consumer alert --once
python consumers/run_all_consumers.py --consumer metrics --once
python consumers/run_all_consumers.py --consumer archive --once
python consumers/run_all_consumers.py --consumer dlq --once
python consumers/run_all_consumers.py --consumer maintenance --once
```

### Execução em Tempo Real no Barramento Kafka

Para rodar os 5 consumidores simultaneamente em threads paralelas consumindo do Kafka:
```bash
python consumers/run_all_consumers.py --consumer all
```

Ou execute individualmente em um terminal dedicado:
```bash
python consumers/alert_consumer.py
python consumers/metrics_consumer.py
python consumers/archive_consumer.py
python consumers/dlq_consumer.py
python consumers/maintenance_consumer.py
```

---

## Suíte de Testes Automatizados

Para executar os testes dos 5 consumidores:
```bash
pytest tests/test_consumers.py -v
```
