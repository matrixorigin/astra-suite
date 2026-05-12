# Astra Suite 测试计划
## 基于最近一周 Bug Fix (2026-05-01 ~ 2026-05-08)

---

## 📋 Bug Fix 总览

### 1. **Cron 定时任务修复** (commit: 1d70fd1)
**影响范围**: `astra-gateway` cron 调度模块  
**修复内容**:
- 🐛 修复调度器时间计算逻辑 (now → today 判断)
- ✨ 新增时区支持 (支持 Asia/Shanghai 等时区配置)
- 🔧 修复 `/cron del` 命令支持 8 字符短 ID 前缀匹配
- 🚀 支持 `*/N` 步进表达式 (如 `*/5 * * * *` 每 5 分钟)

**相关文件**:
- `crates/astra-gateway/src/store/mod.rs` (+160 lines)
- `crates/astra-gateway/src/commands.rs`
- `crates/astra-gateway/src/config.rs`
- `crates/astra-gateway/src/main.rs`

### 2. **Codex CLI 进程阻塞修复** (commit: 6d04338)
**影响范围**: `astra-gateway` 与 Codex CLI 集成  
**修复内容**:
- 🐛 修复 stdin 阻塞问题 (添加 `stdin(Stdio::null())`)
- 🔧 修复 `--sandbox` 参数作用域 (仅 `exec` 初始调用，不用于 `exec resume`)

**相关文件**:
- `crates/astra-gateway/src/cli_bridge.rs`

---

## 🧪 测试计划

### Phase 1: Cron 定时任务功能测试

#### 1.1 基础调度时间计算测试
**目标**: 验证 `next_cron_run_str` 时间计算逻辑正确性

| Test Case ID | 描述 | Cron 表达式 | 预期行为 | 优先级 |
|-------------|------|------------|---------|-------|
| CRON-T-001 | 当前时间之前的日任务应调度到今天 | `30 10 * * *` (10:30) | 当前时间 09:00 → 今天 10:30 | P0 |
| CRON-T-002 | 当前时间之后的日任务应调度到明天 | `30 10 * * *` (10:30) | 当前时间 11:00 → 明天 10:30 | P0 |
| CRON-T-003 | 步进表达式 - 每 5 分钟 | `*/5 * * * *` | 从当前时间后的下个 5 的倍数分钟 | P1 |
| CRON-T-004 | 步进表达式 - 特定小时内每 15 分钟 | `*/15 9 * * *` | 仅在 09:00-09:59 内每 15 分钟 | P1 |
| CRON-T-005 | 工作日过滤 - 周一到周五 | `0 9 * * 1-5` | 跳过周末，调度到下个工作日 | P1 |
| CRON-T-006 | 特定星期几 | `0 14 * * 3` | 调度到下个周三 14:00 | P2 |

**测试步骤**:
```bash
# 单元测试方式 (建议添加)
cargo test -p astra-gateway -- next_cron_run_str --nocapture

# 集成测试方式
# 1. 启动 gateway (配置不同时区)
# 2. 使用 /cron add 创建任务
# 3. 检查返回的 next_run 时间字段
```

#### 1.2 时区支持测试
**目标**: 验证多时区场景下调度时间计算正确

| Test Case ID | 描述 | 时区配置 | Cron 表达式 | 验证点 | 优先级 |
|-------------|------|---------|------------|-------|-------|
| CRON-TZ-001 | Asia/Shanghai 时区 (+8) | `timezone: "Asia/Shanghai"` | `0 9 * * *` | 北京时间 09:00 = UTC 01:00 | P0 |
| CRON-TZ-002 | UTC 时区 | `timezone: "UTC"` | `0 9 * * *` | UTC 09:00 | P0 |
| CRON-TZ-003 | America/New_York (-5/-4) | `timezone: "America/New_York"` | `30 8 * * *` | 纽约时间 08:30 | P2 |
| CRON-TZ-004 | 跨时区夏令时边界 | 各时区 | `0 2 * * *` | DST 切换日处理正确 | P3 |

**测试步骤**:
```yaml
# gateway.yaml 配置示例
timezone: "Asia/Shanghai"

# 测试命令
# 1. 设置时区后重启 gateway
# 2. 创建 cron 任务
# 3. 查看数据库中 next_run 字段 (UTC 时间戳)
# 4. 对比 /cron list 显示的本地时间
```

#### 1.3 /cron del 命令测试
**目标**: 验证短 ID 前缀匹配功能

| Test Case ID | 描述 | 输入 | 预期结果 | 优先级 |
|-------------|------|------|---------|-------|
| CRON-DEL-001 | 完整 ID 删除 | `/cron del 12345678-1234-5678-1234-567812345678` | 删除成功 | P1 |
| CRON-DEL-002 | 8 字符短 ID 删除 | `/cron del 12345678` | 匹配唯一任务并删除 | P0 |
| CRON-DEL-003 | 短 ID 冲突检测 | `/cron del 1234` (多个任务匹配) | 返回错误/列出候选 | P1 |
| CRON-DEL-004 | 不存在的 ID | `/cron del nonexist` | 返回 "未找到" 错误 | P2 |

**测试步骤**:
```bash
# 1. 创建多个 cron 任务
/cron add "0 9 * * *" "test task 1"
/cron add "0 10 * * *" "test task 2"

# 2. 查看任务列表，记录 job_id
/cron list

# 3. 测试短 ID 删除
/cron del <前8位>

# 4. 验证删除结果
/cron list
```

#### 1.4 边界条件测试
**目标**: 验证异常输入处理

| Test Case ID | 描述 | 输入 | 预期结果 | 优先级 |
|-------------|------|------|---------|-------|
| CRON-EDGE-001 | 非法 cron 表达式 | `invalid cron` | fallback 到 +24h | P1 |
| CRON-EDGE-002 | 步进值为 0 | `*/0 * * * *` | fallback 或错误提示 | P2 |
| CRON-EDGE-003 | 步进值 >= 60 | `*/60 * * * *` | fallback 或错误提示 | P2 |
| CRON-EDGE-004 | 月底/年底边界 | `0 23 * * *` (当前 12-31 23:30) | 调度到次年 1-1 23:00 | P1 |

---

### Phase 2: Codex CLI 集成测试

#### 2.1 Stdin 阻塞修复验证
**目标**: 确认不再出现 "Reading additional input from stdin..." 错误

| Test Case ID | 描述 | 命令 | 验证点 | 优先级 |
|-------------|------|------|-------|-------|
| CODEX-STD-001 | 初始 exec 不阻塞 | `codex exec "hello" --sandbox workspace-write --json` | 立即返回响应，无 stdin 提示 | P0 |
| CODEX-STD-002 | 长时间运行任务 | `codex exec "analyze repo" --json` | 进程正常运行完成，不挂起 | P1 |
| CODEX-STD-003 | 并发调用 | 同时执行 3 个 codex exec | 所有进程都正常完成 | P2 |

**测试步骤**:
```bash
# 直接测试
time codex exec "list files in current dir" --sandbox workspace-write --json

# 通过 gateway 测试 (模拟 WeChat 场景)
# 1. 配置 gateway 使用 codex 后端: /cli codex
# 2. 发送消息触发 exec
# 3. 检查响应是否正常、无延迟
```

#### 2.2 --sandbox 参数作用域测试
**目标**: 验证 `--sandbox` 仅用于 `exec` 而非 `exec resume`

| Test Case ID | 描述 | 命令 | 预期结果 | 优先级 |
|-------------|------|------|---------|-------|
| CODEX-SB-001 | exec 初始调用带 --sandbox | `codex exec "prompt" --sandbox workspace-write --json` | 命令成功，sandbox 生效 | P0 |
| CODEX-SB-002 | exec resume 不应带 --sandbox | `codex exec resume <id> "continue" --json` | 命令成功，无 "--sandbox 参数错误" | P0 |
| CODEX-SB-003 | session 持久化后恢复 | 创建 session → 重启 gateway → resume | resume 正常工作 | P1 |

**测试步骤**:
```bash
# 1. 创建新 session
SESSION_ID=$(codex exec "create a plan" --sandbox workspace-write --json | jq -r '.session_id')

# 2. Resume session (不应报错 "unexpected argument '--sandbox'")
codex exec resume $SESSION_ID "continue with step 1" --json

# 3. 检查响应
echo $?  # 应为 0 (成功)
```

#### 2.3 端到端集成测试
**目标**: 验证 gateway → codex 完整流程

| Test Case ID | 描述 | 场景 | 验证点 | 优先级 |
|-------------|------|------|-------|-------|
| CODEX-E2E-001 | WeChat gateway 发送消息 | 企业微信发送 "hello" | 收到 codex 响应 | P0 |
| CODEX-E2E-002 | 切换 CLI 后端 | `/cli codex` → 发送消息 | 使用 codex 处理 | P1 |
| CODEX-E2E-003 | 错误恢复 | codex 进程崩溃 → 重试 | gateway 正确处理错误 | P2 |
| CODEX-E2E-004 | 长消息响应 | 请求大型代码生成 | 完整返回，无截断 | P2 |

---

### Phase 3: 回归测试

#### 3.1 Cron 系统回归
**目标**: 确保 fix 未破坏现有功能

| Test Case ID | 描述 | 验证点 | 优先级 |
|-------------|------|-------|-------|
| REG-CRON-001 | `/cron add` 基本功能 | 创建任务成功 | P0 |
| REG-CRON-002 | `/cron list` 列表显示 | 显示所有任务 + next_run | P0 |
| REG-CRON-003 | 任务实际执行 | 到期任务被执行 | P0 |
| REG-CRON-004 | 执行失败重试 | 失败任务按策略重试 | P1 |
| REG-CRON-005 | 持久化 (需 MySQL) | 重启后任务保留 | P1 |

#### 3.2 CLI Bridge 回归
**目标**: 确保 codex 修复不影响其他 CLI 后端

| Test Case ID | 描述 | CLI 后端 | 验证点 | 优先级 |
|-------------|------|---------|-------|-------|
| REG-CLI-001 | Claude CLI | `claude` | 正常交互 | P0 |
| REG-CLI-002 | Copilot CLI | `copilot` | 正常交互 | P1 |
| REG-CLI-003 | Codex CLI | `codex` | 正常交互 (已在 2.x 测试) | P0 |
| REG-CLI-004 | 切换后端 | `/cli <name>` | 切换成功 | P1 |

---

## 🎯 测试执行策略

### 优先级定义
- **P0** (Critical): 核心功能，必须通过才能发布
- **P1** (High): 重要功能，应在发布前完成
- **P2** (Medium): 增强功能，可后续补充
- **P3** (Low): 边缘场景，根据资源决定

### 测试环境
```yaml
# gateway.yaml 测试配置
timezone: "Asia/Shanghai"  # 测试时区功能

storage:
  backend: sqlite  # 快速测试用 sqlite
  path: "./test.db"

# 或使用 MySQL (测试持久化功能)
# storage:
#   backend: mysql
#   url: "mysql://root:pwd@localhost:3306/astra_test"
```

### 自动化测试建议
```bash
# 单元测试
cargo test -p astra-gateway

# 集成测试
cargo test -p astra-gateway --test integration_runner

# 添加新的测试用例 (建议)
# crates/astra-gateway/tests/cron_timing_tests.rs
# crates/astra-gateway/tests/codex_stdin_tests.rs
```

---

## 📊 测试覆盖率目标

| 模块 | 行覆盖率目标 | 分支覆盖率目标 | 当前状态 |
|------|------------|--------------|---------|
| `store/mod.rs` (cron 逻辑) | 90% | 85% | 需补充测试 |
| `cli_bridge.rs` (codex 集成) | 80% | 75% | 需补充测试 |
| `commands.rs` (cron del) | 85% | 80% | 需补充测试 |

---

## 🐛 已知风险点

### Risk-1: 时区 DST 边界
- **描述**: 夏令时切换时可能出现时间计算错误 (如 2:00 AM 不存在)
- **缓解**: 使用 `chrono` 库的 DST-aware 计算，添加边界测试用例

### Risk-2: Cron 步进表达式边界
- **描述**: `*/N` 当 N 较大或边界条件时可能溢出
- **缓解**: CRON-EDGE-002/003 测试用例覆盖

### Risk-3: 并发调度冲突
- **描述**: 多个定时任务同时到期时可能产生竞争
- **缓解**: 需要添加并发测试用例 (Phase 3 补充)

---

## ✅ 验收标准

### Phase 1 通过标准
- [ ] P0 测试用例 100% 通过
- [ ] P1 测试用例 ≥ 90% 通过
- [ ] 无新增 critical/high severity bugs

### Phase 2 通过标准
- [ ] CODEX-STD-001, CODEX-SB-001/002 通过
- [ ] CODEX-E2E-001 端到端场景验证通过
- [ ] 无进程阻塞或挂起现象

### Phase 3 通过标准
- [ ] REG-CRON-001~003 核心功能回归通过
- [ ] REG-CLI-001~003 CLI 后端正常工作
- [ ] 性能无明显劣化 (cron 调度延迟 < 5s)

---

## 📝 测试执行记录

### 执行日志模板
```markdown
### [日期] Phase X 测试执行

**执行人**: 
**环境**: (OS, Rust version, gateway version)
**配置**: (gateway.yaml 关键配置)

| Test Case ID | 状态 | 备注 |
|-------------|------|------|
| CRON-T-001 | ✅ PASS | |
| CRON-T-002 | ❌ FAIL | 边界条件需修复 |
| ... | | |

**发现的问题**:
1. [问题描述] - [Issue #链接]

**总结**: 
```

---

## 🔄 持续改进建议

1. **添加自动化测试**
   - 为 `next_cron_run_str_with_tz` 添加全面的单元测试
   - 为 CLI bridge stdin 处理添加集成测试

2. **增强监控**
   - 添加 cron 调度延迟指标 (Prometheus metrics)
   - 添加 codex exec 失败率告警

3. **文档完善**
   - 更新 `docs/cron.md` 说明时区配置和步进表达式
   - 更新 `docs/cli-backends.md` 说明 codex sandbox 参数行为

---

**生成时间**: 2026-05-08  
**基于提交**: 1d70fd1, 6d04338  
**下次更新**: 发现新 bug fix 后更新此计划
