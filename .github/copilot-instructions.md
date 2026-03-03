# GitHub Copilot Instructions for pawn-queue

## Project Overview

**pawn-queue** is a pure-Python async pub/sub queue library that uses S3-compatible object storage (Hetzner, MinIO, R2, AWS, etc.) as a message broker. No external dependencies like Redis or RabbitMQ needed.

### Core Concepts
- **Topics** → S3 prefixes (e.g., `orders/`)
- **Messages** → JSON objects in `{topic}/messages/{timestamp}-{uuid}.json`
- **Leases** → Concurrent claim records in `{topic}/leases/{uuid}.lease`
- **Consumers** → Poll or listen for messages; renew leases periodically
- **Producers** → Publish messages to topics

## Architecture & Patterns

### Concurrency Strategies

The project implements **two** lease acquisition strategies:

1. **`conditional_write`** (AWS S3, Cloudflare R2)
   - Uses `If-None-Match: *` header for atomic writes
   - Exactly one consumer wins; losers get `412 PreconditionFailed`
   - No jitter needed

2. **`csprng_verify`** (Hetzner, Ceph, MinIO, fallback)
   - Cryptographic compare-and-swap using S3 ETags
   - 4-step process: Write → Jitter → Verify (with retries) → Post-verify confirmation
   - **Critical edge case:** Late-arriving competitor writes during verify window
   - **Mitigation:** Step 4 (post-verify confirmation) sleeps `jitter_min_ms` then does final HEAD

### Late-Write Edge Case (IMPORTANT)

**Problem:** A competing consumer's slow-network PUT can arrive at S3 *after* we've passed all verify rounds (step 3), overwriting our lease. This gives both consumers the false impression they won.

**Solution:** Step 4 (post-verify confirmation) in `_acquire_csprng()`:
```python
# After verify_retries pass, sleep jitter_min_ms again, then final HEAD
await asyncio.sleep(cv.jitter_min_ms / 1000.0)
meta = await self._client.head_object(key)
final_etag = meta.get("ETag", "").strip('"').lower()
if final_etag != lease.expected_etag.strip('"').lower():
    raise LeaseConflictError(message_id)
```

When generating code for lease acquisition:
- Always include Step 4 in `csprng_verify`
- Respect `jitter_min_ms` (default 100 ms, must exceed 2× expected RTT to S3)
- Never skip the final ETag re-check

## Code Organization

```
pawn_queue/
├── client.py          # S3 API wrapper (put, get, head, delete, copy, list)
├── config.py          # Pydantic config validation + env var overlays
├── consumer.py        # Consumer: poll() + listen() modes + lease refresh
├── core.py            # PawnQueue: main entry point, topics, registry
├── exceptions.py      # LeaseConflictError, PublishError, etc.
├── lease.py           # LeaseManager + both acquire strategies [KEY FILE]
├── message.py         # Message dataclass (ack/nack handlers)
├── producer.py        # Producer: publish messages
├── registry.py        # Producer/consumer registration in S3
├── topic.py           # TopicManager: create + list topics
└── utils.py           # Helpers: nonce, MD5, timestamps, S3 key builders

e2e/
├── conftest.py        # Session-scoped PawnQueue fixture
├── test_e2e_concurrency.py    # [FLAKINESS TEST] 20 msgs × 3 consumers
├── test_e2e_compat_probe.py   # Strategy detection + 10-way race
└── ...                # Other e2e suites (produce/consume, registry, dead-letter)

tests/
└── ...                # Unit tests with moto (in-memory S3)
```

## Key Implementation Details

### Consumer Lease Refresh

Active leases are extended in the background (`_lease_refresher` task):
- Runs every `polling.lease_refresh_interval_seconds` (default: 10 s)
- Re-PUTs the lease with new `expires_at` timestamp
- Keeps the *same nonce* so ETag remains stable for `csprng_verify`

### Janitor Task

Deletes expired leases to restore message visibility after consumer crashes:
- Runs at the start of each `poll()` call
- Checks `expires_at` timestamp in each lease object
- Idempotent (safe to run frequently)

### Idempotency

- Producer/consumer registration is idempotent by name → returns same UUID on re-registration
- Topic creation is idempotent (PUT `.topic` marker)
- `ack()`/`nack()` multiple calls are safe (no-ops)
- Message handlers should still be idempotent (defensive; duplicate delivery theoretically possible under extreme conditions)

## Testing Guidelines

### Unit Tests (`tests/`)
- Use `moto`'s `ThreadedMotoServer` for in-memory S3
- No real cloud credentials needed
- Fast (~1 second)

### E2E Tests (`e2e/`)
- Requires real S3 credentials (Hetzner, AWS, etc.)
- Each session writes under `pawnqueue-e2e-{uuid}/` prefix
- All objects deleted after test run (no cleanup needed)
- **Critical test:** `test_e2e_concurrency.py` — 20 messages × 3 concurrent consumers with both strategies

**When writing concurrency tests:**
- Do not set `jitter_min_ms < 100` (too aggressive for remote S3)
- Must run both `csprng_verify` and `conditional_write` variants (parametrized)
- Assert `duplicates == []` (failure = protocol bug)

## Configuration & Env Vars

### Config File (`config.yaml`)
```yaml
s3:
  endpoint_url: "https://fsn1.your-objectstorage.com"
  bucket_name: "my-queue"
  aws_access_key_id: "..."
  aws_secret_access_key: "..."
  region_name: "eu-central-1"
  use_ssl: true

polling:
  interval_seconds: 5
  max_messages_per_poll: 10
  visibility_timeout_seconds: 30
  lease_refresh_interval_seconds: 10
  jitter_max_ms: 200  # [deprecated for csprng_verify; has own jitter config]

concurrency:
  strategy: "auto"  # auto | conditional_write | csprng_verify
  csprng_verify:
    jitter_min_ms: 100           # ⚠️ CRITICAL: must exceed 2× network RTT
    jitter_max_ms: 400
    verify_retries: 2
    verify_retry_delay_ms: 150

registry:
  heartbeat_interval_seconds: 60
```

### Environment Variables (override config)
```bash
PAWNQUEUE_S3_ENDPOINT_URL=https://...
PAWNQUEUE_S3_BUCKET_NAME=...
PAWNQUEUE_S3_ACCESS_KEY=...
PAWNQUEUE_S3_SECRET_KEY=...
PAWNQUEUE_S3_REGION=...
```

## Common Pitfalls

1. **Forgetting post-verify confirmation in `_acquire_csprng`**
   - Always sleep `jitter_min_ms` before the final HEAD
   - This catches late-arriving competitor writes

2. **Setting `jitter_min_ms` too low**
   - Must be ≥ 2× network one-way latency to S3
   - Default 100 ms works for European data centres; tune for your region

3. **Not using idempotent consumer handlers**
   - While unlikely, duplicate delivery is theoretically possible
   - Always check "have I already processed this?" before side effects

4. **Modifying the nonce during lease refresh**
   - Keep the same nonce when refreshing to maintain stable ETag
   - See `LeaseManager.refresh()` method

5. **Forgetting to strip quotes from ETag**
   - S3 returns ETags like `"abc123"` (with quotes)
   - Always `.strip('"')` before comparison

## Common Changes

### Adding a new S3 operation
1. Add method to `S3Client` in `client.py`
2. Use `self._client.OPERATION()` (aioboto3 wrapper)
3. Handle `ClientError` appropriately
4. Document in docstring (especially any leasing implications)

### Tuning concurrency for a new backend
1. Check if backend supports `If-None-Match: *` (test via `probe_conditional_write`)
2. If not, `csprng_verify` is used
3. Measure one-way network latency to your S3 endpoint
4. Set `jitter_min_ms = 2 × measured_latency_ms` (minimum 100 ms)

### Adding a new test scenario
1. Use existing e2e fixture `pawnqueue` (session-scoped PawnQueue instance)
2. Use `topic` fixture to auto-create a unique, isolated topic
3. Run both strategies if testing concurrency (parametrize)
4. Check for duplicates and message loss

## Session-Scoped Event Loop (pytest-asyncio)

E2E tests share a single session event loop (set in `e2e/pytest.ini`):
```ini
[pytest]
asyncio_mode = auto
asyncio_default_fixture_loop_scope = session
asyncio_default_test_loop_scope = session
timeout = 120
```

This is **critical**: session-scoped async fixtures (like `pawnqueue`) must run in the session loop; individual tests must not get their own loops (causes "Future attached to a different loop" errors).

## Recent Fixes & Lessons Learned

### Issue: Flaky duplicate delivery in concurrency tests
**Root cause:** Post-verify window vulnerability in `csprng_verify`

**Fix applied:**
1. Added Step 4 (post-verify confirmation) to `_acquire_csprng()` in `lease.py`
2. Raised default `jitter_min_ms` in tests from 50 → 150 ms (closer to real-world RTT)
3. Updated README with detailed explanation of the late-write edge case

**Lesson:** Distributed consensus protocols over eventually-consistent storage require careful timing windows. Always allow for slow-network arrivals after local verification passes.

## References

- **Main lease algorithm:** See `pawn_queue/lease.py` lines 200–280
- **Consumer polling logic:** See `pawn_queue/consumer.py` lines 70–130
- **Concurrency strategy selection:** See `pawn_queue/core.py` (setup sequence)
- **Architecture diagrams:** See `README.md` (Mermaid diagrams)
- **E2E test isolation:** See `e2e/conftest.py` (run_prefix fixture)

---

**Questions?** Check the README or post an issue describing the scenario. Include:
- Backend (Hetzner, AWS, MinIO, etc.)
- Python version
- Full error traceback (if applicable)
- Config values (masking credentials)
