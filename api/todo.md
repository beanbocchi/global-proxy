# TODO — `api/` (post-migrations)

**Chốt nguyên tắc:** KHÔNG service nào connect Postgres trực tiếp — gateway/worker
đều qua **PostgREST (HTTP)**, async qua **NATS JetStream**. Chỉ Supabase stack giữ
credential Postgres.

**Decision (chốt):** **NATS JetStream**, KHÔNG Kafka — cho job backbone + analytics.
Lý do: solo-dev/single-host, 1 binary nhẹ có durable stream + replay; analytics
đi NATS → Go sink → ClickHouse (`cmd/usagesink`). → Cần reconcile `docs/projects/db-migration`
(D3 "Kafka now" → NATS; xem mục "Còn lại").

**Delivery (chốt):** transport at-least-once (NATS, **no DLQ**); worker = **at-most-once**
qua register-first (`register_message` = `INSERT ON CONFLICT DO NOTHING`). Crash/lỗi
ko ghi được = mất luôn message (chấp nhận, vì destination chưa idempotent).

## Đã xong (session này)

- [x] Gateway **no DB** — chỉ publish bus, fast-return 202 (publish ack = persisted).
- [x] Worker `processed_message` = **at-most-once ledger** (`id`/`status`/`updated_at`),
      `register_message`/`mark_done`/`mark_error` RPC (`pkg/idempotency`: `Guard` + `Store`
      backends postgres/mem). Bỏ lease/`locked_until`/`claim_message`/redis.
- [x] Bus per-message ack: `Handler` trả `[]error`, 1 message độc không nak cả batch.
- [x] NATS bus + **handler pool** (`WithConcurrency`). **No DLQ** (gỡ max-deliver/`bus.Attempt`).
- [x] `pkg/guard`: outbound (per-host breaker + bulkhead) + inbound (per-IP rate
      limit + whitelist hook), chung generic `registry[T]`.
- [x] `pkg/postgrest` scan-into-dest API + `RPC`.

## Resolved (câu hỏi cũ)

- [x] **Outcome path** — worker ghi outcome vào `processed_message.status`
      (done/error) qua RPC, không còn `job` table. Side-effect (PB webhook) là việc
      riêng; chốt **at-most-once** (destination chưa idempotent → thà mất hơn double).
- [x] **Worker đọc gì từ global plane?** — KHÔNG. Payload self-contained trong bus
      message; worker chỉ claim + mark. Không PostgREST read.

## Còn lại

- [x] **Reconcile canonical docs** (transport): Kafka → NATS JetStream khắp
      `planning.md` (D3/F3/F4/F6/table/phases), `execution-checklist.md` (P0-B/P1-D/Q1),
      `tdd.md` (P2/P6 + toàn bộ diagram/table). Jobs/payments split ghi ở D3/F3/P6/P0-B.
- [ ] **tdd.md deep pass:** nhiều job-flow diagram vẫn model "outbox → NATS" cho
      *jobs* (vd §matrix L1285, async flow L1180). Theo model mới jobs = gateway publish
      thẳng (no outbox); chỉ payments giữ outbox. Cần sửa các diagram job-flow đó.
- [ ] **tdd.md P2** mở rộng nguyên tắc: "no backend *service* connects global
      Postgres directly — gateway/worker via PostgREST" (P2 hiện chỉ nói *clients*).
- [ ] Rate-limit `RPS`/`Burst` hardcode (`internal/gateway/http.go`) → đưa vào config.
- [ ] guard inbound registry **không evict** → spoofed-IP phình map (DoS). Thêm
      TTL/LRU hoặc WAF trước khi mở internet.
- [ ] breaker `ReadyToTrip` = consecutive → cân nhắc failure-ratio over window
      (partial outage 200/5xx xen kẽ không trip; bulkhead là backstop tạm).
- [ ] **`MarkDone`/`MarkError` lỗi đang bị nuốt (`_ =`)** trong `Guard.Run`. Hệ quả:
      DB down sau khi `fn` chạy → outcome KO ghi được, message vẫn ack (at-most-once)
      → row kẹt `pending` mãi, hoặc fn xong mà status ko lên `done`. Hiện ko DLQ, ko
      alert → mất thầm lặng. Cần (khi redesign): ít nhất log/metric, cân nhắc retry
      riêng cho mark (idempotent) hoặc reaper quét `pending` cũ.
- [ ] Side-effect thật (mock đang echo) → PB webhook + destination idempotency key.
- [ ] **Cache read-through cho `/rest/v1` proxy** — allowlist THEO TABLE (KHÔNG cache mọi GET).
      Cache: catalog public (`stores, addons, plans, banner, blog, clusters, constant,
      currency_rates, missions, rank_rewards, discounts`); chỉ anon (no `Authorization`,
      `apikey` rỗng/anon — tránh leak RLS); 2xx; TTL ngắn per-table. KHÔNG cache
      subscription/wallet/usage/user/* (per-user, authoritative — `tdd.md` §2.7.2 L739/765,
      SSE `no-store` L460). Dùng `cache.Client` (`cachememory`), wire ở
      `internal/gateway/postgrest.go`. (Đã chốt design; chưa code.)
- [x] **SSE broadcast** — `model.TopicSSE` typed (`SSEEvent` + `SSEType` const:
      notification, ...) trên bus. Gateway subscribe topic → `sse.Hub` fan-out
      tới client đang kết nối (`GET /sse`), wire format theo `tdd.md` §2.4.1.
      Caveat: 1 group cố định → đúng cho single-instance; multi-replica cần group
      unique/fanout. Recipient lấy từ query `?user=` (TODO: từ auth).
