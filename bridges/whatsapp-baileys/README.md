# WhatsApp Baileys Bridge

Experimental WhatsApp Web sidecar for `astra-gateway`.

It uses Baileys to maintain a WhatsApp Web linked-device session. Inbound
messages are posted to gateway `/inject`; gateway replies are sent back through
this bridge's `/send` endpoint.

This is not the official WhatsApp Business API. Use a disposable/test number.

## Run

```bash
cd bridges/whatsapp-baileys
npm install

export GATEWAY_INJECT_URL=http://127.0.0.1:9090/inject
export BRIDGE_PORT=8787
npm start
```

Scan the printed QR code from the WhatsApp account that should act as the bot.

If direct network access to WhatsApp is blocked, set a proxy:

```bash
export WHATSAPP_PROXY=http://127.0.0.1:7890
# or rely on HTTPS_PROXY / HTTP_PROXY
```

Gateway config:

```yaml
api_port: 9090

platforms:
  whatsapp_web:
    enabled: true
    bridge_url: "http://127.0.0.1:8787"
```

Optional local auth:

```bash
export BRIDGE_AUTH_TOKEN=dev-secret
```

```yaml
platforms:
  whatsapp_web:
    enabled: true
    bridge_url: "http://127.0.0.1:8787"
    auth_token: "dev-secret"
```
