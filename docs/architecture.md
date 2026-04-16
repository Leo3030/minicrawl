# Compliant Multi-Egress Architecture

这份设计稿描述的是一个合规版的抽取系统，关注的是稳定性、隔离性和可扩展性，而不是绕过站点限制。

## 目标

- 让同一个 `session profile` 始终绑定同一个 worker
- 让每个 worker 使用固定出口身份，例如固定 NAT/EIP 或固定区域出口
- 对同一个站点实施统一限速
- 让抽取 runtime 和调度层解耦

## 逻辑拓扑

```text
Request
  -> API
  -> Scheduler
  -> Session Registry
  -> Site Rate Limiter
  -> Worker Pool
       -> worker-us-east-1       -> fixed egress-us-east
       -> worker-eu-west-1       -> fixed egress-eu-west
       -> worker-ap-southeast-1  -> fixed egress-ap-southeast
  -> Playwright Runtime
  -> Validation + JSON Output
```

## 设计原则

### 1. 固定出口不是频繁切换出口

合规场景下，更好的做法是让 `session profile -> worker -> fixed egress` 保持稳定。这样做的价值是：

- 网络来源稳定，便于审计
- 会话状态更容易追踪
- 错误和限速可以按站点、按 worker 排查

### 2. 会话是业务对象，不是浏览器实例

`session profile` 至少应该记录：

- `profile_id`
- `site_host`
- `worker_id`
- `browser profile`
- `created_at`
- `last_used_at`

这里的 `browser profile` 在 MVP 中只包含稳定的 `locale / timezone / viewport / userAgent`。后续如果需要登录态，可以继续加 `storage state` 或 cookie 存储。

### 3. 限速按站点做，不按任务做

调度器应该先看 `site_host`，再决定：

- 最小请求间隔
- 最大并发
- 是否需要冷却窗口

MVP 里先实现最简单的 `min interval`。

## 生产版建议

- Worker 放到独立进程或队列消费者
- Session Registry 落到 Postgres 或 Redis
- Site Rate Limiter 落到 Redis，支持跨实例共享
- Worker 的 fixed egress 通过云网络设施提供
- Playwright context 可挂持久化 storage state
- 加 job 表、attempt 表、site policy 表

## 当前代码映射

- `src/orchestrator.ts`: worker、session profile、site limiter
- `src/server.ts`: API 编排和诊断信息输出
- `src/extractor.ts`: Playwright runtime，接收稳定 browser profile
