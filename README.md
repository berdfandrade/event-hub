# Event Hub

O **Event Hub** é uma API em Python (FastAPI) que **recebe eventos de outros sistemas via webhook e os processa em segundo plano**, de forma assíncrona e confiável.

Em vez de o sistema que envia o evento ficar esperando todo o processamento terminar, o Event Hub responde na hora (`202 Accepted`), guarda o evento e entrega o trabalho para um *worker*. Se algo der errado no processamento, o evento é tentado de novo automaticamente; se continuar falhando, ele fica separado para análise e pode ser reprocessado manualmente.

## Para que serve

Use o Event Hub quando vários serviços precisam avisar uns aos outros que algo aconteceu, por exemplo:

- o serviço de autenticação avisa `user.created` e o Event Hub envia o e-mail de boas-vindas;
- a loja avisa `order.paid` e o Event Hub atualiza o estoque e emite a nota;
- um provedor externo (pagamento, CRM etc.) manda webhooks que precisam ser tratados sem perder nenhum.

O que ele resolve:

| Problema | Como o Event Hub trata |
|---|---|
| Processamento lento travando quem envia o evento | Responde `202` imediatamente e processa depois |
| Falhas temporárias (API fora, timeout) | Retry automático com espera crescente (backoff exponencial) |
| Eventos que nunca vão dar certo | Após N tentativas vão para uma *dead-letter queue* e ficam `failed` |
| Webhooks reenviados em duplicidade | Header `Idempotency-Key` evita processar o mesmo evento duas vezes |
| Worker cai no meio do processamento | O evento volta para a fila quando o worker reinicia |
| "O que aconteceu com o meu evento?" | Cada evento tem status consultável por API |

## Como funciona

```
 Serviço externo                 Event Hub API                     Worker(s)
 ───────────────                 ─────────────                     ─────────
 POST /webhooks/events ───────►  1. grava no MongoDB (pending)
                                 2. coloca o ID na fila (Redis)
       ◄────── 202 Accepted ──   3. responde na hora
                                                     ┌──────────►  4. pega o evento da fila
                                                     │             5. executa os handlers
                                                     │             6. atualiza o status no MongoDB
                                    fila de retries ─┘                ├─ sucesso → completed
                                    (com atraso)    ◄─────────────────┼─ erro    → retrying
                                                                      └─ esgotou → failed (dead-letter)
```

- **MongoDB** guarda o evento completo e seu histórico de status (é a fonte da verdade).
- **Redis** guarda apenas os IDs nas filas: a fila principal, a de retries agendados e a dead-letter.
- **Worker** é um processo separado da API. Você pode rodar quantos quiser para dar conta de mais volume.

### Ciclo de vida de um evento

```
pending ──► processing ──► completed
                │
                ├──► retrying ──► processing ──► ...
                │
                └──► failed  ──(POST /events/{id}/retry)──► pending
```

| Status | Significado |
|---|---|
| `pending` | Recebido e na fila, aguardando um worker |
| `processing` | Um worker está executando os handlers |
| `retrying` | Falhou, mas vai ser tentado de novo após um intervalo |
| `completed` | Todos os handlers executaram com sucesso |
| `failed` | Esgotou as tentativas (ou não pôde ser enfileirado) |

## Rodando localmente

Pré-requisitos: Python 3.12+ e Docker.

```bash
python -m venv .venv && source .venv/bin/activate
```

```bash
pip install -r requirements.txt
```

Crie o `.env` na raiz:

```env
MONGO_URI=mongodb://localhost:27017
MONGO_DATABASE=eventhub
REDIS_URL=redis://localhost:6379/0
```

Suba MongoDB e Redis, depois a API e o worker (em terminais separados):

```bash
docker compose up -d
```

```bash
uvicorn app.main:app --reload
```

```bash
python -m app.workers.event_worker
```

> Em desenvolvimento, dá para rodar o worker dentro da própria API com `RUN_EMBEDDED_WORKER=true uvicorn app.main:app --reload`.

A documentação interativa fica em <http://localhost:8000/docs>.

## Exemplo de uso

Enviar um evento:

```bash
curl -i -X POST http://localhost:8000/webhooks/events \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: pedido-123" \
  -d '{"name": "order.paid", "source": "shop", "payload": {"order_id": 123}}'
```

Resposta (`202 Accepted`, com header `Location: /events/<id>`):

```json
{
  "id": "178f3ad6-0b24-42fa-8589-b6b32effa24b",
  "name": "order.paid",
  "source": "shop",
  "payload": {"order_id": 123},
  "status": "pending",
  "attempts": 0,
  "last_error": null,
  "idempotency_key": "pedido-123",
  "created_at": "2026-11-11T12:55:46.456000Z",
  "updated_at": "2026-11-11T12:55:46.456000Z",
  "processed_at": null
}
```

Acompanhar o evento:

```bash
curl http://localhost:8000/events/178f3ad6-0b24-42fa-8589-b6b32effa24b
```

## Endpoints

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/webhooks/events` | Recebe um evento e responde `202`. Com `Idempotency-Key` repetida, responde `200` com o evento original e não processa de novo |
| `GET` | `/events` | Lista eventos, do mais novo para o mais antigo. Filtros: `status`, `name`, `source`, `limit`, `skip` |
| `GET` | `/events/{id}` | Detalhes e status de um evento |
| `POST` | `/events/{id}/retry` | Reenfileira um evento `failed` (zera as tentativas) |
| `GET` | `/events/stats` | Quantidade de eventos por status e tamanho das filas |
| `GET` | `/health` | Verifica a conexão com MongoDB e Redis |

## Criando handlers

Um *handler* é a função que faz o trabalho de verdade quando um evento chega. Crie um módulo em `app/handlers/`:

```python
# app/handlers/orders.py
from app.schemas.event import Event
from app.services.event.handlers import registry


@registry.on("order.paid")
async def update_stock(event: Event):
    order_id = event.payload["order_id"]
    ...  # se levantar exceção, o evento será tentado de novo
```

E importe-o em `app/handlers/__init__.py`:

```python
from . import default, orders  # noqa: F401
```

Regras:

- Um mesmo evento pode ter vários handlers; eles rodam um depois do outro, primeiro os de `"*"` e depois os específicos, cada grupo na ordem de registro. Se um falhar, os seguintes não rodam nessa tentativa.
- Handlers registrados com `@registry.on("*")` rodam para todos os eventos (o `default.py` usa isso para logar).
- Qualquer exceção ou estouro de tempo (`WORKER_HANDLER_TIMEOUT`) conta como falha e gera retry.
- A entrega é **pelo menos uma vez** (*at-least-once*): em casos raros, como queda do worker no meio do processamento, um handler pode rodar mais de uma vez para o mesmo evento. Escreva handlers que aguentem isso (idempotentes).

## Configuração

Variáveis lidas do `.env` ou do ambiente:

| Variável | Padrão | Descrição |
|---|---|---|
| `MONGO_URI` | — | URI de conexão do MongoDB (obrigatória) |
| `MONGO_DATABASE` | — | Nome do banco (obrigatória) |
| `REDIS_URL` | — | URL do Redis (obrigatória) |
| `EVENT_QUEUE_PREFIX` | `events` | Prefixo das chaves no Redis |
| `WORKER_CONCURRENCY` | `4` | Eventos processados em paralelo por worker |
| `WORKER_MAX_ATTEMPTS` | `5` | Tentativas antes de marcar como `failed` |
| `WORKER_RETRY_BASE_DELAY` | `2` | Espera do primeiro retry, em segundos (dobra a cada tentativa) |
| `WORKER_RETRY_MAX_DELAY` | `300` | Espera máxima entre retries, em segundos |
| `WORKER_HANDLER_TIMEOUT` | `30` | Tempo máximo de cada handler, em segundos |
| `RUN_EMBEDDED_WORKER` | `false` | Roda o worker dentro do processo da API |

## Estrutura do projeto

```
app/
├── main.py                  # app FastAPI, ciclo de vida e /health
├── controllers/
│   ├── webhook.py           # POST /webhooks/events
│   └── events.py            # consulta, estatísticas e retry
├── schemas/event.py         # modelos Event, EventCreate e status
├── services/
│   ├── event/
│   │   ├── event.py         # persistência e transições de status no MongoDB
│   │   ├── event_queue.py   # filas no Redis (principal, retries, dead-letter)
│   │   ├── processor.py     # executa handlers e decide sucesso/retry/falha
│   │   └── handlers.py      # registro de handlers (@registry.on)
│   └── redis/redis.py       # wrapper do cliente Redis
├── handlers/                # seus handlers de eventos
├── workers/event_worker.py  # processo do worker
├── database/                # conexões com MongoDB e Redis
└── core/settings.py         # configurações
tests/                       # testes (pytest + testcontainers)
```

## Testes

Os testes sobem MongoDB e Redis temporários com *testcontainers*, então só precisam do Docker rodando:

```bash
pytest
```

> `tests/database/test_mongodb_connection.py` testa a conexão com o MongoDB configurado no `.env` e só passa com ele rodando.
