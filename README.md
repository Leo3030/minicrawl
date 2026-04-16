# Spider MVP

一个 schema-first 的网页抽取原型。

这个项目先用 Playwright 读取页面快照，再让模型生成抽取计划和 JSON Schema。你可以先审阅和修改 schema，确认后再执行正式抽取，而不是一上来就直接抓结果。

当前版本还带了一个轻量调度层，用来演示这些运行时能力：

- `session profile -> worker -> fixed egress label` 的稳定绑定
- 按站点执行最小请求间隔、重试和冷却
- 用本地 JSON 文件或 Postgres 持久化 session、job、site policy
- 返回 job、worker、session 相关诊断信息

## 用它做什么

- 给任意页面先生成一份抽取 schema 草案
- 用自然语言继续修改 schema，比如“删掉 price”或“把 review_count 改成 number”
- 确认 schema 之后再执行抽取
- 对 Amazon 搜索结果页走内置列表抽取流程，支持跨页收集商品

## 快速开始

先安装依赖并准备环境变量：

```bash
pnpm install
cp .env.example .env
pnpm exec playwright install chromium
```

然后启动开发服务：

```bash
pnpm dev
```

打开 `http://localhost:3000`，输入目标网址和抽取目标即可。

如果你想跑编译后的版本：

```bash
pnpm build
pnpm start
```

## 配置环境变量

`.env.example` 里已经给了默认值。

- `MINIMAX_API_KEY`: 必填，模型调用使用的 API key
- `MINIMAX_BASE_URL`: 默认 `https://api.minimaxi.com`
- `MINIMAX_MODEL`: 默认 `MiniMax-M2.7`
- `OPENAI_API_KEY`: 可作为 `MINIMAX_API_KEY` 的兜底读取值
- `PORT`: 默认 `3000`
- `DATABASE_URL`: 可选。配置后使用 Postgres 持久化；不配则写入本地 JSON 文件
- `RUNTIME_STORE_PATH`: 本地持久化文件路径，默认 `data/runtime-store.json`
- `SITE_MIN_INTERVAL_MS`: 同站点最小请求间隔，默认 `1500`
- `SITE_MAX_ATTEMPTS`: 同站点默认最大尝试次数，默认 `2`
- `SITE_RETRY_BACKOFF_MS`: 重试回退时间，默认 `1200`
- `SITE_COOLDOWN_MS`: 多次失败后的冷却时间，默认 `5000`

## 先审 schema，再抽数据

Web UI 现在是一个两阶段流程：

1. 提交 URL 和抽取目标，服务端调用 `POST /api/schema/draft`
2. 前端展示 schema 草案，你可以继续发自然语言修改，服务端调用 `POST /api/schema/revise`
3. 确认 schema 后，前端调用 `POST /api/extract`
4. 抽取结果按确认后的 schema 返回

如果 URL 是 Amazon 搜索结果页，也就是域名包含 `amazon.` 且路径以 `/s` 开头，服务端会直接使用内置列表计划，而不是让模型现生成一份 plan。

## API 概览

### `POST /api/schema/draft`

根据页面快照生成 schema 草案。

请求体示例：

```json
{
  "url": "https://example.com",
  "goal": "提取标题、摘要和主图"
}
```

成功响应会返回：

- `plan`: 当前抽取计划
- `schema`: 由计划生成的 JSON Schema
- `reply`: 给前端显示的引导文案

### `POST /api/schema/revise`

按自然语言指令修改现有计划。

请求体示例：

```json
{
  "url": "https://example.com",
  "goal": "提取标题、摘要和主图",
  "message": "删掉主图，再新增 author 字段",
  "plan": {}
}
```

其中 `plan` 必须是上一步返回的合法计划对象。

### `POST /api/extract`

按确认后的计划执行抽取。

请求体示例：

```json
{
  "url": "https://www.amazon.com/s?k=keyboard",
  "goal": "提取商品标题、价格、评分和评论数",
  "sessionProfileId": "amazon-us-main",
  "maxItems": 40,
  "reviewsPerItem": 3,
  "plan": {}
}
```

字段说明：

- `sessionProfileId`: 可选。不传时按站点自动生成
- `maxItems`: 可选。对 Amazon 搜索结果页可跨分页继续收集，最多抓到指定条数
- `reviewsPerItem`: 可选。只对需要进入详情页补充评论信息的场景生效
- `plan`: 可选但推荐传入。传入后会以你确认的计划为准

响应示例：

```json
{
  "ok": true,
  "totalCount": 24,
  "result": []
}
```

### `GET /api/jobs`

返回最近的 job 列表。可用 `limit` 查询参数控制数量。

### `GET /api/jobs/:jobId`

返回单个 job 的详情、尝试历史、校验信息和结果。

### `GET /api/system/topology`

返回当前运行时拓扑，包括：

- worker 列表
- session profile 绑定
- site policy
- 最近 job
- 当前持久化驱动

### `GET /api/system/policies`

返回当前站点策略。

### `POST /api/system/policies`

覆盖指定站点的策略。

请求体示例：

```json
{
  "siteHost": "example.com",
  "minIntervalMs": 2000,
  "maxAttempts": 3,
  "retryBackoffMs": 1500,
  "cooldownMs": 8000
}
```

## Amazon 搜索结果页的特殊行为

Amazon 搜索结果页是当前项目里最完整的列表抽取路径。

- 自动识别搜索结果卡片，过滤噪音块和重复商品
- `maxItems` 大于当前页结果数时，会继续翻分页
- 当计划里包含详情字段时，会进入商品详情页补充字段
- `reviewsPerItem` 用来控制每个商品补抓多少条评论

这个逻辑是针对搜索页定制的，不是通用站点的分页抽取框架。

## 持久化和调度

默认情况下，运行时数据写到 `data/runtime-store.json`。如果配置了 `DATABASE_URL`，服务启动时会自动初始化这些表：

- `session_profiles`
- `site_policies`
- `jobs`

运行时会维护：

- 稳定的 session profile
- 固定 worker 和 egress label 绑定
- 按站点的限速、重试和冷却
- 最近 job 的状态和尝试历史

## 项目结构

```text
src/server.ts         Express API 和静态页面入口
src/extractor.ts      Playwright 抽取和 Amazon 特化逻辑
src/openai.ts         计划生成和 schema 修订
src/orchestrator.ts   session、worker、rate limit、job 调度
src/runtime-store.ts  JSON 文件 / Postgres 持久化
src/schema.ts         JSON Schema 构建和结果校验
public/               最小前端
docs/architecture.md  合规版多出口架构说明
```

## 当前范围和限制

- 目前是单服务进程，不是独立 worker 集群
- 通用站点主要覆盖单页抽取
- Amazon 搜索页支持分页列表抽取，但逻辑是站点特化的
- schema 修订依赖模型返回稳定结构化结果
- 没有登录态管理、点击流 DSL、分布式队列和 migration 管理

## 进一步扩展

- 加 schema version 和站点 profile 缓存
- 把失败回修做成只修 selector，不重做整份计划
- 补登录态、分页、点击流能力
- 把当前的 Postgres 初始化拆成独立 migration
- 把内存调度替换成独立 worker 进程或队列消费者

## 相关文档

- [合规版多出口架构说明](./docs/architecture.md)
