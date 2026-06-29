# Release Assets

`astra-suite` is the public release surface for open Astra user artifacts.

The repository hosts multiple independent components, so releases are
component-scoped. Do not use the repository-wide `/releases/latest` pointer for
installers or self-updaters; it can only point at one component.

## Astra CLI

The private Astra repository builds the `astra` CLI and uploads the assets to
this repository's releases. New CLI releases use tags like:

```text
astra-cli-v<version>
```

The optional Astra CLI installer, `scripts/install-astra.sh`, discovers the
newest release that contains the current platform's CLI asset:

```text
astra-v<version>-linux-amd64.tar.gz
astra-v<version>-linux-arm64.tar.gz
astra-v<version>-darwin-amd64.tar.gz
astra-v<version>-darwin-arm64.tar.gz
```

Each archive has a matching `.sha256` file.

## Astra Gateway

This repository builds and releases `astra-gateway` from source. The public
gateway installer, `scripts/install.sh`, and the gateway self-update command
discover the newest release that contains the current platform's gateway asset.
New gateway releases use tags like:

```text
astra-gateway-v<version>
```

Gateway asset names intentionally keep the existing target-triple naming:

```text
astra-gateway-x86_64-unknown-linux-musl.tar.gz
astra-gateway-aarch64-unknown-linux-musl.tar.gz
astra-gateway-x86_64-apple-darwin.tar.gz
astra-gateway-aarch64-apple-darwin.tar.gz
```

Do not rename these gateway assets without also updating
`astra-gateway update`.

Legacy `v<version>` releases remain supported by the installer/updater when the
expected asset is present, but new releases should use component-scoped tags.
