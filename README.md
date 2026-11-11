# event-hub

Recebe eventos via webhook e os processa de forma assíncrona.

```
POST /webhooks/events ──► MongoDB (status=pending) ──► Redis (fila) ──► worker ──► handlers
        │                                                                 │
        └── 202 Accepted imediatamente                 completed / retrying (backoff) / failed (dead-letter)
```

## Rodando

```bash
docker compose up -d                       # MongoDB + Redis
uvicorn app.main:app --reload              # API
python -m app.workers.event_worker         # worker (pode rodar várias instâncias)
```

Para desenvolvimento, `RUN_EMBEDDED_WORKER=true` roda o worker dentro do processo da API.

## Endpoints

| Método | Rota | Descrição |
|---|---|---|
| POST | `/webhooks/events` | Recebe evento, responde `202`. Header opcional `Idempotency-Key` (repetição retorna `200` com o evento original) |
| GET | `/events` | Lista (`?status=&name=&source=&limit=&skip=`) |
| GET | `/events/{id}` | Status do evento |
| POST | `/events/{id}/retry` | Reenfileira um evento `failed` |
| GET | `/events/stats` | Contagem por status e tamanho das filas |
| GET | `/health` | Checa MongoDB e Redis |

## Criando handlers

Crie um módulo em `app/handlers/` e importe-o em `app/handlers/__init__.py`:

```python
from app.services.event.handlers import registry

@registry.on("user.created")
async def send_welcome_email(event):
    ...  # levantar exceção => retry com backoff exponencial
```

## Configuração (`.env`)

`MONGO_URI`, `MONGO_DATABASE`, `REDIS_URL` (obrigatórias) e opcionais:
`WORKER_CONCURRENCY` (4), `WORKER_MAX_ATTEMPTS` (5), `WORKER_RETRY_BASE_DELAY` (2s),
`WORKER_RETRY_MAX_DELAY` (300s), `WORKER_HANDLER_TIMEOUT` (30s), `EVENT_QUEUE_PREFIX` (`events`).

## Testes

```bash
pytest   # usa testcontainers (precisa de Docker)
```
