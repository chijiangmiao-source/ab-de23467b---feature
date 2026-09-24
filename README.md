# calibsvc — 晶圆量测设备标定参数服务

按设备维护随批次区间（半开区间 `[lower, upper)`）变化的标定参数，并支持在
特定批次与温度组合上发布温区覆盖层（半开批次区间 × 半开温度区间的矩形）。
每次发布（无论基础标定还是温区覆盖层）生成一个递增修订号；新区间/矩形只替换
上一修订的重叠部分，保留残段/残片，并把内容相同且共享完整边界的相邻区域合并。
所有历史修订不可变、可随时复算；每个修订同时快照当时的基础批次标定与全部温区
覆盖层。未命中温区的温度继续使用原有基础标定。

## 快速开始

```bash
# 构建并启动 db + app + verify（verify 跑完构建检查、单元测试和 HTTP 冒烟后退出）
docker compose up --build --exit-code-from verify verify

# 只启动服务（宿主机端口可用 HOST_PORT 配置，默认 8080）
HOST_PORT=9090 docker compose up --build app
```

`verify` 服务只在 `app` 健康（数据库可达、迁移完成）后启动，执行一次后按结果退出：
全部通过退出码 0，任何一步失败退出码 1。

## API

### 健康检查

```
GET /health  → 200 {"status":"ok"}   # 数据库不可达时 503
```

### 发布标定

```
POST /v1/equipments/{equipment}/calibrations
{
  "operation_id":  "op-2026-0001",   // 操作标识（幂等键，设备内唯一）
  "seen_revision": 3,                // 调用方所见修订号（乐观并发控制）
  "lower": 50, "upper": 150,         // 半开批次区间 [lower, upper)
  "content": { "recipe": "R1", "gain": 1.5 }   // 任意 JSON 标定内容
}

→ 201
{
  "equipment": "EQ-1",
  "revision": 4,                     // 新的递增修订号
  "segments": [                      // 该修订生效的完整区间快照
    {"lower": 0,   "upper": 50,  "content": {...}},
    {"lower": 50,  "upper": 150, "content": {...}},
    {"lower": 150, "upper": 300, "content": {...}}
  ]
}
```

- 每次成功发布修订号 +1，新快照替换重叠部分、保留残段、合并内容相同的相邻段。
- 同一 `operation_id` + 相同参数重试：返回首次结果（201 + 首次响应体，
  响应头 `X-Idempotent-Replay: true`），不产生新修订。
- 同一 `operation_id` + 不同参数：`409 OPERATION_CONFLICT`。
- `seen_revision` 与当前修订不一致：`409 STALE_REVISION`（并发发布同一所见
  修订时只有一个能成功）。
- 内容按规范化 JSON 比较（键序、空白不敏感），用于相邻段合并与幂等哈希。

### 发布温区覆盖层

```
POST /v1/equipments/{equipment}/temperature-overlays
{
  "operation_id":  "op-2026-0042",         // 操作标识（幂等键，设备内唯一，两类发布共用命名空间）
  "seen_revision": 3,                     // 调用方所见修订号（乐观并发控制）
  "batch_lower": 50, "batch_upper": 150,  // 半开批次区间 [batch_lower, batch_upper)
  "temp_lower":  60, "temp_upper":  90,   // 半开温度区间 [temp_lower, temp_upper)
  "content": { "recipe": "R1-HOT", "gain": 1.7 }
}

→ 201
{
  "equipment": "EQ-1",
  "revision": 4,                          // 同样递增修订号
  "rectangles": [                         // 该修订生效的完整温区矩形快照
    {"batch_lower":0,   "batch_upper":50,  "temp_lower":0,  "temp_upper":100, "content":{...}},
    {"batch_lower":50,  "batch_upper":150, "temp_lower":0,  "temp_upper":60,  "content":{...}},
    {"batch_lower":50,  "batch_upper":150, "temp_lower":60, "temp_upper":90,  "content":{...}},
    ...
  ]
}
```

- 发布矩形与既有矩形的交集被替换；被交矩形切成至多四个残片（左右整高残片 +
  上下温度残片），互不重叠；随后反复合并内容相同、共享完整边（并集仍为矩形）
  的相邻矩形；仅部分相接（L 形）不合并。
- 温区覆盖层叠加在基础标定之上；未命中任何矩形的点回退到该批次的基础标定。
- 每次发布（基础或温区）都把基础段和温区矩形整体快照到新修订，因此任一历史
  修订都能同时复算“当时的基础标定 + 当时全部温区”。
- 幂等、`OPERATION_CONFLICT`、`STALE_REVISION` 语义与基础发布一致；同一
  `operation_id` 在两个入口间复用也算异参冲突。

### 查询生效标定

```
# 一维（既有，语义与响应结构不变；忽略所有温区覆盖层）
GET /v1/equipments/{equipment}/calibration?batch=75[&revision=2]

# 二维（带 temperature）：命中温区返回覆盖层内容，否则回退基础标定
GET /v1/equipments/{equipment}/calibration?batch=75&temperature=80[&revision=2]
```

二维响应：

```json
{
  "equipment": "EQ-1",
  "batch": 75,
  "temperature": 80,
  "revision": 2,
  "source": "overlay",            // "overlay" 或 "base"（回退）
  "lower": 50, "upper": 150,      // 命中矩形（或回退基础段）的批次区间
  "temp_lower": 60, "temp_upper": 90,  // 回退基础时省略为 0
  "content": { ... }
}
```

半开边界：温度恰好等于某矩形上界（或批次等于矩形上界）时不算命中，回退基础
标定；基础标定也不覆盖的批次返回 `BATCH_NOT_COVERED`。

### 错误码（稳定）

| HTTP | code                  | 含义                                   |
|------|-----------------------|----------------------------------------|
| 400  | `INVALID_JSON`        | 请求体不是合法 JSON / 含未知字段       |
| 400  | `INVALID_PARAMETER`   | 缺少必填参数或参数非法                 |
| 400  | `INVALID_INTERVAL`    | 区间非法（半开区间下界 >= 上界；批次或温度任一维）|
| 400  | `INVALID_CONTENT`     | content 不是合法 JSON                  |
| 404  | `EQUIPMENT_NOT_FOUND` | 设备不存在                             |
| 404  | `REVISION_NOT_FOUND`  | 修订号不存在（超出当前修订）           |
| 404  | `BATCH_NOT_COVERED`   | 批次未被任何区间覆盖                   |
| 409  | `STALE_REVISION`      | 所见修订已过期（并发冲突）             |
| 409  | `OPERATION_CONFLICT`  | 操作标识被不同参数复用                 |
| 503  | `UNHEALTHY`           | 健康检查时数据库不可达                 |

## 设计

**数据模型**（PostgreSQL，`internal/store`）

- `heads(equipment, revision)`：每台设备的当前修订号。
- `segments(equipment, revision, lower, upper, content)`：每个修订的完整基础
  批次区间快照，只插不改不删——数据库触发器拒绝任何 `UPDATE`/`DELETE`，历史
  修订永远可复算。
- `overlay_rects(equipment, revision, batch_lo, batch_hi, temp_lo, temp_hi, content)`：
  每个修订的完整温区矩形快照（批次×温度），同样只插不改不删，带不可变触发器。
  无论哪一类发布，新修订都同时写入 `segments` 与 `overlay_rects` 两层快照
  （未变化的一层原样复制），所以每个修订都完整保留当时的基础标定与全部温区。
- `operations(equipment, operation_id, kind, request_hash, revision, response)`：
  幂等账本，`kind` 区分 `base`/`overlay`，记录首次响应；同样受不可变触发器
  保护。`operation_id` 在两类发布之间共用同一命名空间。

**发布事务**（单事务，失败即整体回滚，不留部分切分）

1. `INSERT ... ON CONFLICT DO NOTHING` 建 head 行，随后 `SELECT ... FOR UPDATE`
   锁定——同一设备的两类发布由此串行化。
2. 先查幂等账本：同操作（kind 与载荷哈希一致）→ 直接重放首次结果；异参或跨
   入口复用 → `OPERATION_CONFLICT`。（先于修订检查，保证过期修订的合法重试
   仍返回首次结果。）
3. 校验 `seen_revision == head`，否则 `STALE_REVISION`。
4. 读取当前修订的基础段与温区矩形，按发布种类用 `internal/split.Apply` 或
   `internal/rectsplit.Apply` 计算新快照，两层整批写入新修订并推进 head，记录
   幂等账本，提交。任一步失败（含矩形切分/校验失败）整个事务回滚。

**区间切分**（`internal/split`，纯函数，表驱动单元测试覆盖）

新区间 `[L,U)` 作用于有序不重叠段集：重叠段留下 `[s.L,L)` 与 `[U,s.U)` 残段，
中段被新段替换，最后对相邻同内容段做合并；输出保持有序、不重叠、无退化段。

**矩形切分**（`internal/rectsplit`，纯函数，表驱动单元测试覆盖）

新矩形 `[BL,BH)×[TL,TH)` 作用于互不重叠的矩形集：与每个既有矩形求交，被交
矩形保留至多四个轴对齐残片（左/右整高片 + 低温/高温片），交集被新矩形替换；
随后反复合并内容相同、共享完整边（两矩形并集仍为矩形）的相邻矩形，仅部分边
相接（L 形）不合并。输出与输入矩形顺序无关，保持有序、两两不重叠（可相接）、
无退化矩形；`At` 用于点命中，半开边界天然实现未命中回退。

## 验证（compose 中的 verify 服务）

`verify` 一次性运行并据结果退出：

- 构建检查：`go build ./...`、`go vet ./...`
- 单元测试：`go test ./...`（区间切分、矩形切分、JSON 规范化；设置
  `CALIB_TEST_DATABASE_URL` 时追加 store 层 PostgreSQL 集成测试）
- HTTP 冒烟（对运行中的服务）：
  - 区间切分与覆盖：中间批次补发、残段保留、同内容合并、未覆盖批次报错
  - 历史不变：旧修订一维/二维查询结果不随后续发布改变（API 层 + 数据库触发器层）
  - 幂等重放：同操作同内容（含并发）返回首次结果，异参复用与跨入口复用报
    `OPERATION_CONFLICT`
  - 并发过期冲突：8 个并发同一所见修订，恰好 1 个成功、其余
    `STALE_REVISION`，head 恰好前进 1
  - 温区覆盖层：十字交叠切成四残片、半开温度边界未命中回退基础标定、同内容
    完整边相邻矩形合并、历史修订按批次+温度复算唯一内容
  - 温区幂等/冲突/并发：同载体重试返回首次修订（含并发重试与 head 移动后
    仍可重放）、异参/跨类型拒绝稳定报错、8 路并发恰好一胜、失败不留状态
  - 一维接口回归：带 `temperature` 走二维（含历史复算与回退），省略则与改造前
    完全一致；两个入口的参数校验与稳定错误码
  - 参数校验与稳定错误码；失败事务不留任何状态

## 配置

| 变量           | 作用                         | 默认        |
|----------------|------------------------------|-------------|
| `DATABASE_URL` | PostgreSQL DSN（app 必填）   | —           |
| `PORT`         | app 容器内监听端口           | `8080`      |
| `HOST_PORT`    | 宿主机映射端口（compose）    | `8080`      |
| `APP_URL`      | verify 访问的服务地址        | 见 compose  |
| `CALIB_TEST_DATABASE_URL` | store 集成测试 DSN（verify 容器） | 见 compose |

## 项目结构

```
cmd/server/        服务入口（健康路径、优雅退出）
cmd/verify/        一次性验收：构建检查 + 单元测试 + HTTP 冒烟
internal/split/    基础标定的一维区间切分纯函数（单元测试）
internal/rectsplit/ 温区覆盖层的二维矩形切分/合并纯函数（单元测试）
internal/canonjson/ JSON 规范化（内容相等性）
internal/store/    PostgreSQL 持久化与两类发布事务（集成测试）
internal/httpapi/  HTTP 路由、参数校验、错误映射
Dockerfile         runtime（服务）与 verify（验收）两个构建目标
docker-compose.yml db + app + verify 编排，健康依赖与可配置端口
```
