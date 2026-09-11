# Guia Passo a Passo de Execução — Industrial IoT Streaming Data Pipeline

Este documento contém o tutorial completo e detalhado de como executar, monitorar e testar todo o pipeline de dados streaming na sua máquina local.

---

## 1. Pré-requisitos

Certifique-se de ter os seguintes programas instalados e configurados na sua máquina:

1. **Docker Desktop** (em execução no Windows)
2. **Python 3.12+**
3. **Git**

---

## 2. Preparação do Ambiente Python

Abra o terminal (PowerShell ou VS Code Terminal) na pasta do projeto:

```bash
cd c:\Users\elias.cruz\Documents\PYTHON_PROJECT\io_stream
```

Instale todas as dependências necessárias do projeto rodando:

```bash
pip install -r requirements.txt
```

Isso instalará o **PySpark**, **Paho-MQTT**, **Kafka-Python-NG**, **Psycopg2-Binary** e **Pytest**.

---

## 3. Ordem de Execução (Terminal a Terminal)

Para demonstrar o pipeline completo em tempo real, utilizaremos múltiplos terminais simultâneos.

```text
 Terminal 1          Terminal 2           Terminal 3            Terminal 4           Terminal 5
 (Docker)           (Simulador)            (Bridge)             (PySpark)           (Consultas)
    │                    │                    │                     │                    │
    ▼                    ▼                    ▼                     ▼                    ▼
Subir Infra        Simular Sensores     Encaminhar MQTT        Processar Stream     Consultar SQL
 (Mosquitto/         (Publicar MQTT       mensagens para        (Data Quality,      no PostgreSQL
Kafka/Postgres)       em Mosquitto)          Kafka)              Deduplicação,
                                                                   Refined)
```

---

### Terminal 1 — Iniciar a Infraestrutura (Docker Compose)

No primeiro terminal, suba os serviços de infraestrutura (**Mosquitto MQTT**, **Zookeeper**, **Kafka**, **Kafka UI** e **PostgreSQL**):

```bash
docker compose up -d
```

Verifique se todos os containers estão rodando com sucesso:

```bash
docker compose ps
```

 Interfaces de Observabilidade disponíveis:
- **Kafka UI**: Acesse no navegador `http://localhost:8080`
- **MQTT Broker**: Porta `1883`
- **PostgreSQL**: Porta `5432`

---

### Terminal 2 — Iniciar o Simulador IoT (MQTT Publisher)

No segundo terminal, inicie o simulador de telemetria das máquinas industriais (`MTR-001` até `MTR-005`):

```bash
python simulator/producer.py
```

O simulador começará a publicar eventos JSON a cada 5 segundos no broker Mosquitto sob o tópico:
`industrial/machines/{device_id}/telemetry`

---

### Terminal 3 — Iniciar o Bridge MQTT → Kafka

No terceiro terminal, execute o bridge Python que consome os eventos do Mosquitto e os publica no Apache Kafka:

```bash
python bridge/mqtt_to_kafka.py
```

> **Nota**: Se você estiver utilizando a infraestrutura Docker completa, o bridge também é executado automaticamente como container. Você pode acompanhar seus logs no terminal com:
> ```bash
> docker compose logs -f mqtt-kafka-bridge
> ```

---

### Terminal 4 — Executar o Engine de Processamento (PySpark Structured Streaming)

No quarto terminal, execute o script do PySpark Structured Streaming:

```bash
python streaming/refined.py
```

O PySpark irá:
1. Consumir o tópico Kafka `iot-machine-events`.
2. Validar as regras de **Data Quality**.
3. Enviar eventos inválidos para **Quarantine** (`data/quarantine/`).
4. Aplicar **Deduplicação** por `event_id`.
5. Aplicar **Watermark** por `event_timestamp`.
6. Classificar anomalias (`NORMAL`, `WARNING`, `CRITICAL`).
7. Gravar na camada **Refined** Parquet (`data/refined/`) e no banco **PostgreSQL** (`refined_machine_measurements`).

---

### Terminal 5 — Executar Consultas Analíticas no PostgreSQL

No quinto terminal, consulte os dados analíticos persistidos no PostgreSQL:

```bash
docker exec -it iot_postgres psql -U iot_user -d iot_db -f /database/queries.sql
```

Você verá o resultado de 9 consultas SQL respondendo a perguntas de negócio (máquinas críticas, maiores temperaturas, médias de vibração, distribuição de anomalias).

---

## 4. Como Executar os Testes Automatizados

Para rodar a suíte completa de testes de unidade com o **pytest**:

```bash
pytest
```

Saída esperada:

```text
collected 18 items

tests/test_deduplication.py .                                            [  5%]
tests/test_mqtt_bridge.py .......                                        [ 44%]
tests/test_quality.py .....                                              [ 72%]
tests/test_transformations.py .....                                      [100%]

============================= 18 passed in 0.59s ==============================
```

---

## 5. Como Parar e Limpar o Ambiente

Para parar a simulação:
- Pressione `Ctrl + C` nos terminais do simulador, bridge e PySpark.

Para encerrar a infraestrutura Docker preservando os dados:
```bash
docker compose down
```

Para encerrar e remover todos os volumes/dados salvos:
```bash
docker compose down -v
```
