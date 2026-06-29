# Astra Local Stack

This stack starts MatrixOne, Memoria, and the Astra API server from published
Docker images. It is the public, source-free way to run a local Astra backend
for the `astra` CLI or for `astra-gateway` when using the Astra backend.

## Start With Make

From the repository root:

```bash
make stack-env
```

`make stack-env` creates `deployment/astra-stack/.env` and generates local
stack secrets. Fill the required embedding configuration:

```dotenv
MEMORIA_EMBEDDING_API_KEY=...
MEMORIA_EMBEDDING_BASE_URL=...
```

Then start the stack:

```bash
make stack-up
make stack-smoke
```

`make stack-up` fails before starting containers if any required value is empty
or still looks like a placeholder.

## Install The CLI

The stack does not build the CLI from source. Install the public `astra` binary
from this repository's releases:

```bash
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install-astra.sh | sh -s -- --init-models
astra --version
```

## Admin Bootstrap

Use `astra admin register` to create an administrator account. On a fresh
MatrixOne data volume this bootstraps the initial admin. After an admin exists,
admin registration requires an existing admin login.

```bash
astra admin --api-url http://127.0.0.1:17001 register \
  --username admin \
  --email admin@example.com \
  --password '<password>'
```

The installer writes a commented `.models.yaml` template when run with
`--init-models`. Uncomment one model entry, fill real credentials, then load it:

```bash
# Edit .models.yaml: uncomment one model entry and fill real credentials.
astra admin --api-url http://127.0.0.1:17001 model load .models.yaml --update-existing
```

## Start With Docker Compose

If you do not use Make:

```bash
cd deployment/astra-stack
cp .env.example .env
```

Fill these values:

```text
ASTRA_JWT_SECRET
ASTRA_TOKEN_ENCRYPTION_KEY
ASTRA_BRIDGE_SECRET
MEMORIA_MASTER_KEY
MEMORIA_EMBEDDING_API_KEY
MEMORIA_EMBEDDING_BASE_URL
```

Then start:

```bash
docker compose --env-file .env up -d
```

## Services

| Service | Host port | Description |
| --- | --- | --- |
| `api` | `17001` | Astra HTTP API |
| `memoria` | `8100` | Memoria memory service |
| `matrixone` | `26001` | MatrixOne MySQL-compatible endpoint |
| `matrixone` debug | `26060` | MatrixOne debug/health endpoint |

Ports bind to `127.0.0.1` by default. Change the `*_BIND` values in `.env`
only when you intentionally expose them to other machines.

`ASTRA_API_PORT` controls the host-facing published port. The API container
itself always listens on `17001`.

## Images

By default the stack pulls:

```text
matrixorigin/astra:latest
matrixorigin/memoria:latest
matrixorigin/matrixone:latest
```

Override image tags in `.env`, for example:

```dotenv
ASTRA_IMAGE=matrixorigin/astra:0.1.0
MEMORIA_IMAGE=matrixorigin/memoria:latest
MATRIXONE_IMAGE=matrixorigin/matrixone:latest
```

## Operations

```bash
make stack-status
make stack-logs SERVICE=api
make stack-down
make stack-clean
```

`make stack-clean` removes the MatrixOne data volume. Use it when you want to
replay first-admin bootstrap from scratch.
