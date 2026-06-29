# Astra Suite

Public distribution and open-source tools for the Astra agent ecosystem.

The stable default install path in this repository is `astra-gateway`, the
open-source chat-platform gateway. Optional Astra backend pieces are also
published here: the `astra` CLI installer, a local Docker Compose stack, and
documentation for connecting gateway to a local Astra API server.

## User Journeys

### Install Astra Gateway

```bash
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh
astra-gateway --version
astra-gateway init
astra-gateway start
```

`scripts/install.sh` installs `astra-gateway` and preserves the existing public
installer contract.

See [docs/gateway.md](docs/gateway.md) and
[crates/astra-gateway/README.md](crates/astra-gateway/README.md).

### Optional: Install The Astra CLI

Install the `astra` CLI when you want to talk to an Astra API server directly
or use gateway's Astra backend:

```bash
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install-astra.sh | sh
astra --version
```

The Astra CLI installer downloads `astra` from this repository's GitHub
Releases. It does not install `astra-server` or `astra-gateway`.

To also write a commented model registry template in the current directory:

```bash
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install-astra.sh | sh -s -- --init-models
```

See [docs/install-astra-cli.md](docs/install-astra-cli.md).

### Optional: Run A Local Astra Stack

Use Docker Compose to start MatrixOne, Memoria, and the Astra API server:

```bash
make stack-env
# edit deployment/astra-stack/.env:
#   MEMORIA_EMBEDDING_API_KEY=...
#   MEMORIA_EMBEDDING_BASE_URL=...
make stack-up
make stack-smoke
```

Then install the CLI, create the first admin, and load model configuration:

```bash
make cli-install INIT_MODELS=1

astra admin --api-url http://127.0.0.1:17001 register \
  --username admin \
  --email admin@example.com \
  --password '<password>'

# edit .models.yaml: uncomment one model entry and fill real credentials
astra admin --api-url http://127.0.0.1:17001 model load .models.yaml --update-existing
```

On a fresh MatrixOne volume, `astra admin register` bootstraps the initial
administrator. After an admin exists, creating another admin requires an
existing admin login.

Default local endpoints:

| Service | Endpoint |
| --- | --- |
| Astra API | `http://127.0.0.1:17001` |
| Memoria | `http://127.0.0.1:8100` |
| MatrixOne | `127.0.0.1:26001` |
| MatrixOne debug | `http://127.0.0.1:26060` |

See [deployment/astra-stack/README.md](deployment/astra-stack/README.md) and
[docs/local-astra-stack.md](docs/local-astra-stack.md).

### Use Astra Gateway From Source

`astra-gateway` bridges chat platforms to agent CLIs such as Claude Code,
Codex, Copilot, Astra, and custom CLIs. It is open source and can run without
an Astra server by using SQLite plus a non-Astra CLI backend.

```bash
cargo build --release -p astra-gateway
./target/release/astra-gateway init
./target/release/astra-gateway start
```

When you want gateway to use Astra, start the local stack and configure the
gateway's Astra profile with `app_server_url: "http://127.0.0.1:17001"`.

See [docs/gateway.md](docs/gateway.md) and
[crates/astra-gateway/README.md](crates/astra-gateway/README.md).

## Repository Structure

```text
astra-suite/
├── crates/
│   ├── astra/                 # HTTP + SSE client library for Astra server
│   └── astra-gateway/         # Chat-platform gateway binary + library
├── deployment/
│   └── astra-stack/           # Public local stack: Astra API + Memoria + MatrixOne
├── docs/                      # Public user journeys and release conventions
├── scripts/
│   ├── install.sh             # Public astra-gateway installer
│   └── install-astra.sh       # Optional astra CLI installer
├── ARCHITECTURE.md
├── CONTRIBUTING.md
├── Makefile
└── LICENSE
```

## Development

```bash
make help           # show all targets
make build          # compile workspace with all targets
make release        # release build for astra-gateway
make check          # format + clippy + test
make test           # fast offline tests
make test-live      # live integrations
make format         # auto-format
make lint           # fmt check + clippy
```

Gateway workflow:

```bash
make gateway-install # install astra-gateway from releases
make init           # generate gateway config + release build
make run            # start gateway
make stop           # stop gateway
make restart        # stop + start
make log            # tail gateway log
```

Local Astra stack workflow:

```bash
make cli-install    # install optional astra CLI from releases
make stack-env      # create deployment/astra-stack/.env and generate secrets
make stack-up       # start MatrixOne + Memoria + Astra API
make stack-status   # show compose status
make stack-logs     # follow logs, optionally SERVICE=api
make stack-down     # stop containers
make stack-clean    # stop containers and remove MatrixOne data
```

## Releases

`astra-gateway` assets are built by this repository. `astra` CLI assets are
uploaded to this repository's releases by the private Astra build pipeline.
Gateway releases keep the existing `v<version>` tag format; CLI releases use
`astra-cli-v<version>` tags.

See [docs/releases.md](docs/releases.md).

## License

[MIT](LICENSE)
