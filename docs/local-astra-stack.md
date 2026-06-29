# Local Astra Stack

Use the local stack when you want a running Astra API server for the `astra`
CLI or for `astra-gateway`'s Astra backend.

```bash
make stack-env
```

Edit `deployment/astra-stack/.env` and fill:

```dotenv
MEMORIA_EMBEDDING_API_KEY=...
MEMORIA_EMBEDDING_BASE_URL=...
```

Then start and smoke-test the stack:

```bash
make stack-up
make stack-smoke
```

The stack starts:

| Service | Host URL |
| --- | --- |
| Astra API | `http://127.0.0.1:17001` |
| Memoria | `http://127.0.0.1:8100` |
| MatrixOne | `127.0.0.1:26001` |
| MatrixOne debug | `http://127.0.0.1:26060` |

Install the CLI:

```bash
make cli-install INIT_MODELS=1
```

Create the first admin:

```bash
astra admin --api-url http://127.0.0.1:17001 register \
  --username admin \
  --email admin@example.com \
  --password '<password>'
```

Load model configuration:

```bash
# Edit .models.yaml: uncomment one model entry and fill real credentials.
astra admin --api-url http://127.0.0.1:17001 model load .models.yaml --update-existing
```

Stop or remove the stack:

```bash
make stack-down
make stack-clean
```

`make stack-clean` removes the MatrixOne data volume.
