# Case Local — Industrial IoT Streaming Pipeline

## Objetivo

Criar, na máquina local, um case completo de Engenharia de Dados simulando uma arquitetura de dados IoT em streaming, desde a geração dos eventos até a camada **Refined**.

O projeto deve ser reproduzível com Docker e servir como case profissional para GitHub.

### Fluxo final

```text
Python IoT Simulator
        |
        v
      Kafka
        |
        v
PySpark Structured Streaming
        |
        +--------------------+
        |                    |
        v                    v
     BRONZE             QUARANTINE
        |
        v
     REFINED
        |
        v
  PostgreSQL
```

---

# 1. Objetivos técnicos

O tutorial deve demonstrar:

- geração de eventos IoT em Python;
- Kafka como barramento de eventos;
- Kafka Topics;
- produtor e consumidor;
- PySpark Structured Streaming;
- processamento baseado em event time;
- watermark;
- deduplicação;
- schema explícito;
- Data Quality;
- separação entre eventos válidos e inválidos;
- camada Bronze;
- camada Refined;
- camada Quarantine;
- persistência em PostgreSQL;
- testes automatizados com pytest;
- Docker Compose;
- logging;
- observabilidade básica;
- documentação de arquitetura.

O projeto NÃO deve depender de GCP, Azure ou AWS.

Tudo deve funcionar localmente.

---

# 2. Stack

Utilizar:

- Python 3.11+
- Apache Kafka
- Apache Spark / PySpark
- PostgreSQL
- Docker
- Docker Compose
- pytest

Opcional para evolução:

- Kafka UI
- Grafana
- Prometheus

Para a primeira versão, priorizar simplicidade e funcionamento.

---

# 3. Pré-requisitos

Antes de executar o projeto, verificar:

```bash
docker --version
docker compose version
python --version
git --version
```

No Windows, o projeto deve funcionar utilizando Docker Desktop.

Recomenda-se executar o projeto a partir de:

```text
VS Code
```

---

# 4. Estrutura do projeto

Criar a seguinte estrutura:

```text
industrial-iot-streaming/
│
├── README.md
├── docker-compose.yml
├── .gitignore
├── requirements.txt
│
├── simulator/
│   ├── __init__.py
│   ├── producer.py
│   ├── sensor.py
│   └── config.py
│
├── streaming/
│   ├── __init__.py
│   ├── schemas.py
│   ├── quality.py
│   ├── bronze.py
│   └── refined.py
│
├── database/
│   └── init.sql
│
├── tests/
│   ├── __init__.py
│   ├── test_quality.py
│   ├── test_transformations.py
│   └── test_deduplication.py
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── business_rules.md
│
└── architecture/
    └── architecture.md
```

---

# 5. Criar o simulador IoT

Criar um simulador em Python que represente máquinas industriais.

Utilizar inicialmente cinco dispositivos:

```text
MTR-001
MTR-002
MTR-003
MTR-004
MTR-005
```

Cada dispositivo deve produzir eventos contendo:

```json
{
  "event_id": "evt-001",
  "device_id": "MTR-001",
  "event_timestamp": "2026-09-11T09:30:00Z",
  "temperature": 72.4,
  "pressure": 4.3,
  "vibration": 6.2,
  "rpm": 1740,
  "status": "RUNNING"
}
```

Campos obrigatórios:

```text
event_id
device_id
event_timestamp
temperature
pressure
vibration
rpm
status
```

Adicionar também, se fizer sentido:

```text
machine_type
location
```

---

# 6. Simular comportamento realista

O simulador não deve gerar apenas números completamente aleatórios.

Criar três comportamentos:

## NORMAL

Exemplo:

```text
temperature: 50–75
pressure: 3–5
vibration: 1–6
rpm: 1400–1800
```

## WARNING

Exemplo:

```text
temperature: 75–90
pressure: 5–7
vibration: 6–12
rpm: 1200–1900
```

## CRITICAL

Exemplo:

```text
temperature: > 90
pressure: > 7
vibration: > 15
```

O simulador deve permitir que uma máquina evolua de:

```text
NORMAL
   ↓
WARNING
   ↓
CRITICAL
   ↓
FAILURE
```

Isso cria uma narrativa de negócio para o pipeline.

---

# 7. Publicar no Kafka

Criar um Kafka Topic:

```text
iot-machine-events
```

O Python deve publicar os eventos nesse tópico.

Configurar o produtor com:

- bootstrap server;
- topic;
- serialização JSON;
- tratamento de erros;
- logging.

O produtor deve permitir configurar a frequência de geração.

Exemplo:

```text
1 evento a cada 5 segundos por dispositivo
```

Não criar uma carga exagerada.

O objetivo é demonstrar streaming, não gerar milhões de eventos.

---

# 8. Docker Compose

Criar um `docker-compose.yml` contendo inicialmente:

```text
Kafka
PostgreSQL
```

Adicionar Kafka UI se isso simplificar a demonstração.

O ambiente deve poder ser iniciado com:

```bash
docker compose up -d
```

E encerrado com:

```bash
docker compose down
```

Para remover volumes:

```bash
docker compose down -v
```

Documentar claramente a diferença entre os comandos.

---

# 9. Schema do evento

Criar:

```text
streaming/schemas.py
```

Definir um schema explícito do PySpark.

Evitar inferência automática do schema.

Exemplo conceitual:

```text
event_id          string
device_id         string
event_timestamp   timestamp
temperature       double
pressure          double
vibration         double
rpm               integer
status            string
```

Explicar no README por que schema explícito é importante em pipelines de produção.

---

# 10. Bronze

Criar o processamento:

```text
streaming/bronze.py
```

Fluxo:

```text
Kafka
  ↓
Parse JSON
  ↓
Schema
  ↓
Bronze
```

A Bronze deve preservar os dados recebidos com o mínimo possível de transformação.

Adicionar:

```text
ingestion_timestamp
```

A Bronze deve servir para:

- rastreabilidade;
- auditoria;
- reprocessamento;
- debugging.

Escolher uma estratégia simples e reproduzível para armazenamento local.

Pode utilizar arquivos Parquet como Bronze.

Estrutura:

```text
data/
└── bronze/
```

---

# 11. Data Quality

Criar:

```text
streaming/quality.py
```

Regras mínimas:

```text
event_id IS NOT NULL
device_id IS NOT NULL
event_timestamp IS NOT NULL
temperature IS NOT NULL
pressure >= 0
vibration >= 0
rpm >= 0
status IN ('RUNNING', 'STOPPED', 'MAINTENANCE', 'FAILURE')
```

Classificar cada evento como:

```text
VALID
INVALID
```

---

# 12. Quarantine

Eventos inválidos não devem simplesmente desaparecer.

Criar:

```text
data/
└── quarantine/
```

Armazenar:

```text
event_id
device_id
error_reason
original_payload
quarantine_timestamp
```

Exemplo:

```text
evt-123
MTR-003
temperature_out_of_range
{...payload original...}
```

Explicar no README que Quarantine/DLQ permite investigação e eventual reprocessamento.

---

# 13. Deduplicação

Implementar deduplicação utilizando:

```text
event_id
```

O pipeline deve conseguir lidar com o mesmo evento sendo recebido mais de uma vez.

Criar teste específico para isso.

Explicar no README o conceito de:

```text
idempotência
```

e por que isso é importante em sistemas de streaming.

---

# 14. Event Time e Watermark

Implementar processamento utilizando:

```text
event_timestamp
```

e não apenas o horário em que o evento chegou.

Utilizar watermark do Spark.

Exemplo conceitual:

```python
withWatermark("event_timestamp", "10 minutes")
```

Criar uma pequena demonstração de evento atrasado.

Documentar:

- event time;
- processing time;
- late event;
- watermark.

---

# 15. Refined

Criar:

```text
streaming/refined.py
```

A Refined deve conter dados limpos e enriquecidos.

Tabela lógica:

```text
refined_machine_measurements
```

Campos:

```text
event_id
device_id
event_timestamp
ingestion_timestamp

temperature
pressure
vibration
rpm

machine_status
anomaly_level

temperature_alert
pressure_alert
vibration_alert

processing_timestamp
```

---

# 16. Regras de anomalia

Criar regras de negócio simples.

Exemplo:

```text
temperature > 90
OR vibration > 15
OR pressure > 7
```

Resultado:

```text
CRITICAL
```

Exemplo:

```text
temperature > 75
OR vibration > 6
OR pressure > 5
```

Resultado:

```text
WARNING
```

Caso contrário:

```text
NORMAL
```

Prioridade:

```text
CRITICAL
    >
WARNING
    >
NORMAL
```

Adicionar testes para essas regras.

---

# 17. PostgreSQL

Criar um banco PostgreSQL para consumo dos dados Refined.

Criar tabela:

```sql
refined_machine_measurements
```

Adicionar índices adequados para consultas por:

```text
device_id
event_timestamp
anomaly_level
```

Criar também uma tabela:

```text
refined_machine_current_status
```

Representando o estado mais recente de cada máquina.

---

# 18. Consultas analíticas

Criar:

```text
database/queries.sql
```

Consultas para responder:

1. Quais máquinas estão em estado CRITICAL?

2. Qual máquina possui maior temperatura?

3. Qual máquina possui maior vibração?

4. Quantos eventos foram processados?

5. Quantos eventos foram rejeitados?

6. Qual a quantidade de eventos por status?

7. Qual a quantidade de eventos por nível de anomalia?

8. Qual a temperatura média por máquina?

9. Qual máquina apresentou maior quantidade de alertas?

---

# 19. Testes

Utilizar pytest.

Criar testes para:

```text
Data Quality
Transformações
Deduplicação
Regras de anomalia
```

Exemplos:

```text
test_invalid_temperature
test_missing_device_id
test_valid_event
test_duplicate_event
test_critical_machine
test_warning_machine
test_normal_machine
```

O comando:

```bash
pytest
```

deve executar os testes.

---

# 20. README

Criar um README profissional contendo:

## Título

```text
Industrial IoT Streaming Data Pipeline
```

## Contexto

Explicar o problema:

> Uma indústria possui máquinas equipadas com sensores que enviam eventos continuamente. O objetivo é construir um pipeline capaz de receber, validar, processar e disponibilizar esses eventos na camada Refined, permitindo identificar comportamentos anormais próximos do tempo real.

## Arquitetura

Mostrar:

```text
Python IoT Simulator
        ↓
Kafka
        ↓
PySpark Structured Streaming
        ↓
 ┌──────┴────────┐
 ↓               ↓
Bronze       Quarantine
 ↓
Refined
 ↓
PostgreSQL
```

## Tecnologias

Listar todas as tecnologias.

## Data Flow

Explicar o caminho completo do evento.

## Data Quality

Documentar todas as regras.

## Streaming Concepts

Explicar:

- event time;
- processing time;
- watermark;
- late events;
- deduplication;
- idempotência.

## Como executar

Fornecer comandos exatos.

## Como testar

```bash
pytest
```

## Exemplos

Mostrar exemplos de eventos.

## Resultados

Mostrar consultas e resultados esperados.

## Próximos passos

Adicionar:

- Kafka UI;
- Grafana;
- Prometheus;
- métricas de pipeline;
- alertas;
- CI/CD;
- schema evolution;
- particionamento;
- integração cloud;
- ML para predictive maintenance.

---

# 21. Requisitos de qualidade do código

O código deve:

- ser modular;
- possuir funções pequenas;
- possuir type hints quando fizer sentido;
- possuir logging;
- evitar valores mágicos;
- utilizar variáveis de ambiente para configurações;
- possuir tratamento de exceções;
- possuir comentários apenas quando agregarem contexto;
- evitar código duplicado.

Não colocar senhas diretamente no código.

Utilizar `.env` quando necessário.

Criar `.env.example`.

---

# 22. Configuração

Centralizar configurações como:

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
EVENT_INTERVAL_SECONDS
```

Nunca versionar `.env`.

Adicionar ao `.gitignore`:

```text
.env
__pycache__/
.pytest_cache/
.venv/
data/
*.log
```

---

# 23. Execução esperada

O tutorial deve terminar com um fluxo reproduzível.

## Terminal 1 — infraestrutura

```bash
docker compose up -d
```

## Terminal 2 — simulador

```bash
python simulator/producer.py
```

## Terminal 3 — streaming

Executar o job PySpark.

## Terminal 4 — consultas

Consultar PostgreSQL.

## Testes

```bash
pytest
```

O README deve explicar exatamente a ordem de execução.

---

# 24. Critério de sucesso

Considerar o case concluído quando for possível demonstrar:

```text
[OK] Kafka funcionando
[OK] Eventos IoT sendo produzidos
[OK] Eventos chegando ao Kafka
[OK] Spark consumindo eventos
[OK] Bronze sendo alimentada
[OK] Data Quality funcionando
[OK] Eventos inválidos indo para Quarantine
[OK] Deduplicação funcionando
[OK] Watermark configurado
[OK] Refined sendo alimentada
[OK] PostgreSQL recebendo dados
[OK] Regras de anomalia funcionando
[OK] Testes passando
[OK] README documentado
```

---

# 25. Importante — ordem de desenvolvimento

Não tentar construir tudo simultaneamente.

Seguir obrigatoriamente esta sequência:

### Fase 1

```text
Docker
Kafka
PostgreSQL
```

### Fase 2

```text
Python Simulator
Kafka Producer
```

### Fase 3

```text
PySpark
Kafka Consumer
```

### Fase 4

```text
Bronze
```

### Fase 5

```text
Data Quality
Quarantine
```

### Fase 6

```text
Deduplication
Watermark
```

### Fase 7

```text
Refined
```

### Fase 8

```text
PostgreSQL
```

### Fase 9

```text
pytest
```

### Fase 10

```text
README
Architecture documentation
```

---

# 26. Regras para o Antigravity

Ao implementar este tutorial:

1. Não criar todos os arquivos de uma vez sem testar.
2. Implementar uma fase por vez.
3. Após cada fase, executar os testes ou comandos necessários.
4. Se houver erro, corrigir antes de avançar.
5. Não trocar Kafka por outro sistema de mensageria.
6. Não trocar PySpark por Pandas.
7. Não adicionar GCP, AWS ou Azure.
8. Priorizar uma solução simples, reproduzível e didática.
9. Manter o projeto adequado para publicação no GitHub.
10. Documentar decisões arquiteturais relevantes.
11. Não esconder erros do pipeline.
12. Eventos inválidos devem ser rastreáveis.
13. Evitar dependências desnecessárias.
14. Usar Docker sempre que isso simplificar a reprodução do ambiente.
15. Ao final de cada fase, informar:
   - arquivos criados/alterados;
   - comandos executados;
   - resultado dos testes;
   - próximo passo.

---

# 27. Evolução futura

Depois da V1 funcionando, não implementar automaticamente as extensões.

Criar uma seção de backlog:

```text
[ ] Kafka UI
[ ] Grafana
[ ] Prometheus
[ ] Métricas de throughput
[ ] Pipeline latency
[ ] Dead Letter Queue
[ ] Schema Registry
[ ] Schema Evolution
[ ] CI/CD GitHub Actions
[ ] Terraform
[ ] Docker optimization
[ ] ML predictive maintenance
[ ] Deploy GCP
```

A primeira versão deve permanecer simples e totalmente local.

---

# 28. Resultado final esperado

O projeto deve contar uma história completa:

```text
SENSOR
  │
  │ evento
  ▼
PYTHON
  │
  │ JSON
  ▼
KAFKA
  │
  │ stream
  ▼
PYSPARK
  │
  ├───────────────┐
  │               │
  ▼               ▼
BRONZE        QUARANTINE
  │
  │ valid events
  ▼
DATA QUALITY
  │
  ▼
DEDUPLICATION
  │
  ▼
WATERMARK
  │
  ▼
REFINED
  │
  ▼
POSTGRESQL
  │
  ▼
ANALYTICS
```

O objetivo do case não é apenas mostrar ferramentas.

O objetivo é demonstrar capacidade de projetar e implementar um pipeline de dados de streaming com:

**ingestão + processamento + qualidade + confiabilidade + armazenamento + consumo analítico.**
