import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState
} from '@whiskeysockets/baileys'
import { HttpsProxyAgent } from 'https-proxy-agent'
import http from 'node:http'
import process from 'node:process'
import P from 'pino'
import qrcode from 'qrcode-terminal'

const bridgePort = Number(process.env.BRIDGE_PORT || '8787')
const gatewayInjectUrl = process.env.GATEWAY_INJECT_URL || 'http://127.0.0.1:9090/inject'
const authDir = process.env.BAILEYS_AUTH_DIR || './auth'
const authToken = process.env.BRIDGE_AUTH_TOKEN || ''
const logLevel = process.env.LOG_LEVEL || 'info'
const proxyUrl =
  process.env.WHATSAPP_PROXY ||
  process.env.HTTPS_PROXY ||
  process.env.https_proxy ||
  process.env.HTTP_PROXY ||
  process.env.http_proxy ||
  ''

const logger = P({ level: logLevel })
let sock = null
const proxyAgent = proxyUrl ? new HttpsProxyAgent(proxyUrl) : undefined

function normalizeJid(to) {
  if (!to) return ''
  if (to.includes('@')) return to
  const digits = String(to).replace(/[^\d]/g, '')
  return digits ? `${digits}@s.whatsapp.net` : ''
}

function senderFromJid(jid) {
  return String(jid || '').split('@')[0]
}

function extractText(message) {
  const m = message?.message
  if (!m) return ''
  return (
    m.conversation ||
    m.extendedTextMessage?.text ||
    m.imageMessage?.caption ||
    m.videoMessage?.caption ||
    ''
  ).trim()
}

async function postJson(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body)
  })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`POST ${url} failed: ${res.status} ${text}`)
  }
}

async function injectInbound(message) {
  const remoteJid = message.key?.remoteJid || ''
  const participant = message.key?.participant || remoteJid
  const group = remoteJid.endsWith('@g.us')
  const text = extractText(message)
  if (!remoteJid || !text || message.key?.fromMe) return

  const chatId = group ? remoteJid : senderFromJid(remoteJid)
  const userId = senderFromJid(participant)
  await postJson(gatewayInjectUrl, {
    platform: 'whatsapp_web',
    chat_id: chatId,
    user_id: userId,
    text,
    group
  })
  logger.info({ chatId, userId, group }, 'injected inbound message')
}

async function startWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState(authDir)
  const { version } = await fetchLatestBaileysVersion()
  sock = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: false,
    agent: proxyAgent,
    fetchAgent: proxyAgent,
    logger
  })

  sock.ev.on('creds.update', saveCreds)
  sock.ev.on('connection.update', ({ connection, lastDisconnect, qr }) => {
    if (qr) {
      qrcode.generate(qr, { small: true })
      logger.info('scan the QR code with the WhatsApp account used as the bot')
    }
    if (connection === 'open') {
      logger.info('whatsapp connected')
    }
    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut
      logger.warn({ statusCode, shouldReconnect }, 'whatsapp connection closed')
      if (shouldReconnect) {
        startWhatsApp().catch((err) => logger.error({ err }, 'reconnect failed'))
      } else {
        logger.error('logged out; remove auth dir and scan again')
      }
    }
  })

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify') return
    for (const message of messages) {
      try {
        await injectInbound(message)
      } catch (err) {
        logger.error({ err }, 'failed to inject inbound message')
      }
    }
  })
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let body = ''
    req.setEncoding('utf8')
    req.on('data', (chunk) => {
      body += chunk
      if (body.length > 1024 * 1024) {
        req.destroy()
        reject(new Error('request body too large'))
      }
    })
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {})
      } catch (err) {
        reject(err)
      }
    })
    req.on('error', reject)
  })
}

function writeJson(res, status, body) {
  const encoded = JSON.stringify(body)
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(encoded)
  })
  res.end(encoded)
}

function authorized(req) {
  if (!authToken) return true
  return req.headers.authorization === `Bearer ${authToken}`
}

function startHttpServer() {
  const server = http.createServer(async (req, res) => {
    if (req.method === 'GET' && req.url === '/health') {
      writeJson(res, 200, { ok: true, connected: Boolean(sock?.user) })
      return
    }
    if (req.method !== 'POST' || req.url !== '/send') {
      writeJson(res, 404, { ok: false, error: 'not found' })
      return
    }
    if (!authorized(req)) {
      writeJson(res, 401, { ok: false, error: 'unauthorized' })
      return
    }
    if (!sock) {
      writeJson(res, 503, { ok: false, error: 'whatsapp socket not ready' })
      return
    }
    try {
      const body = await readJson(req)
      const jid = normalizeJid(body.to)
      const text = String(body.text || '')
      if (!jid || !text) {
        writeJson(res, 400, { ok: false, error: 'to and text are required' })
        return
      }
      await sock.sendMessage(jid, { text })
      writeJson(res, 200, { ok: true })
    } catch (err) {
      logger.error({ err }, 'send failed')
      writeJson(res, 500, { ok: false, error: String(err.message || err) })
    }
  })
  server.listen(bridgePort, '127.0.0.1', () => {
    logger.info({ bridgePort, gatewayInjectUrl, authDir }, 'baileys bridge listening')
  })
}

startHttpServer()
startWhatsApp().catch((err) => {
  logger.error({ err }, 'failed to start whatsapp bridge')
  process.exit(1)
})
