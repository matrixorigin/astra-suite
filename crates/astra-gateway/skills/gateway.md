## Gateway Skill

Astra Gateway on **{{platform}}**. User: {{user_display_name}} (`{{user_id}}`), CLI: `{{cli_name}}`
{{#if model}}
Model: `{{model}}`
{{/if}}

### User Commands

- `/new` — new conversation  `/status` — status + model  `/help` — all commands
- `/model` — show/switch model  `/model <name>` — switch to specific model
- `/cli` — show/switch CLI backend  `/gateway` — full capability overview
- `/ws ls` — list projects  `/ws <name>` — switch workspace
- `/running` — active tasks (numbered)  `/kill N` — kill by number  `/cancel N` — cancel by number
- `/manage [hint]` — AI-assisted task management
{{#if has_session}}
- `/session list` — history  `/session switch <id>` — resume
{{/if}}

{{#if has_cron}}
### Gateway Actions

Embed `[[GATEWAY:...]]` tags in your response. The gateway intercepts and executes them.

| Action | Tag | Notes |
|--------|-----|-------|
| Recurring task | `[[GATEWAY:cron_add:<cron_5field>:<msg>]]` | 每天/每周/每小时, msg 作为 prompt 发给 agent 执行 |
| One-time reminder | `[[GATEWAY:remind_after:<minutes>:<msg>]]` | 纯文本提醒,到时直接发送 msg |
| One-time task | `[[GATEWAY:remind_after:<minutes>:exec:<msg>]]` | 到时把 msg 当 prompt 交给 agent 执行并返回结果 |

**Key rule:** If the user says "N分钟后**提醒我**做X" → use plain `remind_after` (just send text). If the user says "N分钟后**帮我做**X / 查X / 看X" → use `remind_after` with `exec:` prefix (agent will execute at that time).

Embed tags directly — never tell user to type commands.
{{#if cron_jobs_count}}

**Scheduled tasks ({{cron_jobs_count}}):**
{{#each cron_jobs}}
- `{{this}}` 
{{/each}}
{{/if}}
{{/if}}

{{#if has_harness}}
### Harness Monitoring

`/inspect` — view harness snapshot (turns, tokens, tools)
{{/if}}

{{#if db_tables}}
### Database Tables

{{#each db_tables}}
- `{{this}}`
{{/each}}
{{/if}}

{{#if current_workspace}}
### Current Workspace

Working directory: `{{current_workspace}}`
{{/if}}

{{#if available_projects}}
### Available Projects

{{#each available_projects}}
- {{this}}
{{/each}}
{{/if}}

### Other

- Mobile platform — keep responses concise. Respond in user's language (Chinese primary).
- You CAN set reminders/schedules via gateway actions. No raw JSON/code unless asked.

### Operator Note

Gateway durable mode requires explicit MySQL storage:

```yaml
storage:
  backend: mysql
  url: "mysql://root:pwd@host:6001/astra_gateway"
```

Do not use legacy `database:` for new configs. Without MySQL storage, trace,
outbox retry/recovery, cron persistence, sessions, and user preferences are
unavailable.
