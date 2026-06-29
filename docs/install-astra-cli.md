# Install Astra CLI

`scripts/install.sh` installs the public `astra` CLI from GitHub Releases.
It does not install `astra-gateway` and it does not install `astra-server`.

```bash
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh
astra --version
```

Install a specific version:

```bash
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh -s -- -v v0.1.0
```

Install to a custom directory:

```bash
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh -s -- -d ~/.local/bin
```

Preview the download URL without installing:

```bash
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh -s -- -n
```

Write a commented model registry template while installing:

```bash
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh -s -- --init-models
```

By default this creates `./.models.yaml` and does not overwrite an existing
file. Use `--models-path path/to/.models.yaml` or `ASTRA_MODELS_PATH` to choose
another location.

The installer tries GitHub directly first and falls back through
`ASTRA_GHPROXY`:

```bash
ASTRA_GHPROXY=https://ghfast.top sh scripts/install.sh
```

Release assets use this naming convention:

```text
astra-v<version>-linux-amd64.tar.gz
astra-v<version>-linux-arm64.tar.gz
astra-v<version>-darwin-amd64.tar.gz
astra-v<version>-darwin-arm64.tar.gz
```
