import makeWASocket, {
  DisconnectReason,
  useMultiFileAuthState
} from '@whiskeysockets/baileys'
import { DEFAULT_CONNECTION_CONFIG } from '@whiskeysockets/baileys'
import { HttpsProxyAgent } from 'https-proxy-agent'
import fs from 'node:fs/promises'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'
import process from 'node:process'
import P from 'pino'
import QRCode from 'qrcode'
import qrcode from 'qrcode-terminal'

const runDir = process.env.GATEWAY_RUN_DIR || path.join(os.homedir(), '.astra-gateway')
const socketPath = path.join(runDir, 'whatsapp-baileys.sock')
const authDir = path.join(runDir, 'whatsapp-auth')
const qrDir = path.join(runDir, 'whatsapp-qr')
const logLevel = process.env.LOG_LEVEL || 'silent'
const loginOnce = process.argv.slice(2).includes('login')
const proxyUrl =
  process.env.WHATSAPP_PROXY ||
  process.env.HTTPS_PROXY ||
  process.env.https_proxy ||
  process.env.HTTP_PROXY ||
  process.env.http_proxy ||
  ''
const baileysVersion = (process.env.BAILEYS_VERSION || '')
  .split(',')
  .map((part) => Number(part.trim()))
  .filter((part) => Number.isFinite(part))

const logger = P({ level: logLevel })
let sock = null
const proxyAgent = proxyUrl ? new HttpsProxyAgent(proxyUrl) : undefined
const jsonlClients = new Set()
let qrCount = 0

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

async function pushInbound(message) {
  const event = inboundEvent(message)
  if (!event) return

  if (jsonlClients.size === 0) {
    logger.debug({ chatId: event.chat_id, userId: event.user_id }, 'dropping inbound message: no JSONL client connected')
    return
  }

  broadcastJsonl(event)
  logger.info({ chatId: event.chat_id, userId: event.user_id, group: event.group }, 'pushed inbound message')
}

function inboundEvent(message) {
  const remoteJid = message.key?.remoteJid || ''
  const participant = message.key?.participant || remoteJid
  const group = remoteJid.endsWith('@g.us')
  const text = extractText(message)
  if (!remoteJid || !text || message.key?.fromMe) return null

  return {
    type: 'inbound_message',
    chat_id: remoteJid,
    user_id: senderFromJid(participant),
    text,
    group,
    msg_id: message.key?.id || ''
  }
}

async function writeQrFiles(qr) {
  await fs.mkdir(qrDir, { recursive: true })
  const txtPath = path.join(qrDir, 'qr.txt')
  const pngPath = path.join(qrDir, 'qr.png')
  await fs.writeFile(txtPath, `${qr}\n`, 'utf8')
  await QRCode.toFile(pngPath, qr, {
    type: 'png',
    margin: 2,
    width: 512
  })
  console.error(`QR written to ${pngPath}`)
}

async function startWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState(authDir)
  const version = baileysVersion.length === 3
    ? baileysVersion
    : DEFAULT_CONNECTION_CONFIG.version
  logger.info({ version }, 'using WhatsApp Web version')
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
      qrCount += 1
      console.error(`WhatsApp QR ${qrCount === 1 ? 'received' : 'refreshed'}.`)
      qrcode.generate(qr, { small: true })
      writeQrFiles(qr).catch((err) => logger.error({ err }, 'failed to write QR files'))
      console.error('Scan the QR code with the WhatsApp account used as the bot.')
    }
    if (connection === 'open') {
      broadcastJsonl({ type: 'connection', state: 'open' })
      console.error(loginOnce ? 'WhatsApp connected. Login complete.' : 'WhatsApp connected.')
      if (loginOnce) {
        process.exit(0)
      }
    }
    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut
      logger.warn({ statusCode, shouldReconnect }, 'whatsapp connection closed')
      broadcastJsonl({ type: 'connection', state: 'close', status_code: statusCode, reconnect: shouldReconnect })
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
        await pushInbound(message)
      } catch (err) {
        logger.error({ err }, 'failed to push inbound message')
      }
    }
  })
}

function writeLine(stream, value) {
  stream.write(`${JSON.stringify(value)}\n`)
}

function broadcastJsonl(value) {
  for (const client of jsonlClients) {
    writeLine(client, value)
  }
}

async function handleJsonlCommand(stream, command) {
  const id = command.id
  try {
    if (command.type === 'subscribe') {
      jsonlClients.add(stream)
      writeLine(stream, { type: 'response', id, ok: true, connected: Boolean(sock?.user) })
      return
    }
    if (!sock) {
      writeLine(stream, { type: 'response', id, ok: false, error: 'whatsapp socket not ready' })
      return
    }
    if (command.type === 'send_text') {
      const jid = normalizeJid(command.to)
      const text = String(command.text || '')
      if (!jid || !text) {
        writeLine(stream, { type: 'response', id, ok: false, error: 'to and text are required' })
        return
      }
      const result = await sock.sendMessage(jid, { text })
      writeLine(stream, { type: 'response', id, ok: true, message_id: result?.key?.id || '' })
      return
    }
    if (command.type === 'typing') {
      const jid = normalizeJid(command.to)
      const state = command.state === 'paused' ? 'paused' : 'composing'
      if (!jid) {
        writeLine(stream, { type: 'response', id, ok: false, error: 'to is required' })
        return
      }
      await sock.sendPresenceUpdate(state, jid)
      writeLine(stream, { type: 'response', id, ok: true })
      return
    }
    writeLine(stream, { type: 'response', id, ok: false, error: `unknown command type: ${command.type}` })
  } catch (err) {
    logger.error({ err, commandType: command.type }, 'JSONL command failed')
    writeLine(stream, { type: 'response', id, ok: false, error: String(err.message || err) })
  }
}

async function startJsonlServer() {
  await prepareSocketPath(socketPath)

  const server = net.createServer((stream) => {
    let buffer = ''
    stream.setEncoding('utf8')
    stream.on('data', (chunk) => {
      buffer += chunk
      let index
      while ((index = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, index).trim()
        buffer = buffer.slice(index + 1)
        if (!line) continue
        try {
          const command = JSON.parse(line)
          handleJsonlCommand(stream, command)
        } catch (err) {
          logger.warn({ err }, 'invalid JSONL command')
          writeLine(stream, { type: 'response', ok: false, error: 'invalid json' })
        }
      }
    })
    stream.on('close', () => jsonlClients.delete(stream))
    stream.on('error', (err) => {
      jsonlClients.delete(stream)
      logger.debug({ err }, 'JSONL client error')
    })
  })

  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(socketPath, () => {
      server.off('error', reject)
      logger.info({ socketPath }, 'baileys JSONL socket listening')
      resolve()
    })
  })
}

function canConnectSocket(target) {
  return new Promise((resolve) => {
    const client = net.createConnection(target)
    client.once('connect', () => {
      client.end()
      resolve(true)
    })
    client.once('error', () => resolve(false))
  })
}

async function prepareSocketPath(target) {
  await fs.mkdir(path.dirname(target), { recursive: true })
  const stat = await fs.lstat(target).catch((err) => {
    if (err.code === 'ENOENT') return null
    throw err
  })
  if (!stat) return
  if (!stat.isSocket()) {
    throw new Error(`socket path exists and is not a socket: ${target}`)
  }
  if (await canConnectSocket(target)) {
    throw new Error(`socket already in use: ${target}`)
  }
  await fs.unlink(target)
  logger.debug({ socketPath: target }, 'removed stale socket file')
}

async function main() {
  await startJsonlServer()
  await startWhatsApp()
}

main().catch((err) => {
  console.error(`Failed to start WhatsApp bridge: ${err?.message || err}`)
  process.exit(1)
})
