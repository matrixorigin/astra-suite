# WeCom Webhook for Cron Scheduled Messages

## Background

WeCom AI Bot (`aibot_respond_msg`) can only **respond** to messages in group chats — it requires a `reply_token` from an inbound @mention. Cron-triggered tasks have no `reply_token`, so their results cannot be delivered to groups via the AI Bot respond path.

By configuring `webhook_url`, cron task results are delivered to the group via a WeCom Webhook Bot instead.

## Setup

### 1. Create a Webhook Bot in Your WeCom Group

1. Open the target group chat → Group Settings (top-right corner)
2. Group Bots → Add Bot
3. Enter a name (e.g. "Nightly Report")
4. Copy the Webhook URL after creation

### 2. Add webhook_url to gateway.yaml

```yaml
platforms:
  wecom:
    enabled: true
    bot_id: "your-bot-id"
    secret: "your-secret"
    webhook_url: "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
```

`webhook_url` is optional. When not configured, cron results fall back to `aibot_send_msg`.

### 3. Create a Cron Job

@mention the AI Bot in the group to create a scheduled task:

> @BisectBot add a cron job: every day at 9am, analyze nightly regression results and generate a report

The gateway will parse this and create a cron job. When triggered, the result is posted to the group via the webhook bot.

## How It Works

```
Cron timer fires
  → CronScheduler invokes Claude CLI with the task message
  → Gets analysis result
  → Detects platform == "wecom" && webhook_url is configured
  → HTTP POST to webhook URL (markdown format)
  → Message appears in group (as the webhook bot identity)
```

## Message Format

Messages are sent as WeCom Webhook markdown:

```json
{
  "msgtype": "markdown",
  "markdown": {
    "content": "⏰ **Scheduled task `abc12345`**\n\n<analysis result>"
  }
}
```

Messages exceeding 4000 characters are automatically truncated.

## Mentioning Users

Use `<@userid>` syntax in the message content to @mention specific users:

```json
{
  "msgtype": "markdown",
  "markdown": {
    "content": "<@WeiLu><@SunYuZe> Nightly regression report:\n\n..."
  }
}
```

## Comparison: AI Bot vs Webhook Bot

| Capability | AI Bot (respond) | AI Bot (send) | Webhook Bot |
|---|---|---|---|
| Group reply (reactive) | Yes (needs reply_token) | - | - |
| Group message (proactive) | No | Yes, but cannot @mention | Yes, supports @mention |
| DM (proactive) | - | Yes | No |
| Cron result to group | No | Possible | Recommended |

Both can coexist in the same group — AI Bot handles interactive conversations, Webhook Bot handles scheduled push notifications.

## Fallback Behavior

- **webhook_url configured**: cron results go through webhook (reliable, supports @mention)
- **webhook_url not configured**: cron results attempt `aibot_send_msg` (works but cannot @mention users)
- **Non-wecom platforms**: always use the normal outbound delivery path
