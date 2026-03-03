# Contributing to pawn-queue

Thanks for your interest in contributing! Here's how to get started.

## Development Setup

### Clone and Install

```bash
git clone https://github.com/yourusername/pawn-queue.git
cd pawn-queue

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

### Code Quality

We use `black` for formatting and `ruff` for linting:

```bash
# Format code
black pawn_queue tests

# Run linter
ruff check pawn_queue tests

# Or fix issues automatically
ruff check --fix pawn_queue tests
```

### Running Tests

#### Unit Tests (fast, no S3 needed)

```bash
pytest tests/ -v
```

#### E2E Tests (requires S3 credentials)

Set environment variables or create `e2e/config.yaml`:

```bash
export PAWNQUEUE_S3_ENDPOINT_URL=https://your-s3-endpoint.com
export PAWNQUEUE_S3_BUCKET_NAME=your-test-bucket
export PAWNQUEUE_S3_ACCESS_KEY=your-access-key
export PAWNQUEUE_S3_SECRET_KEY=your-secret-key
export PAWNQUEUE_S3_REGION=your-region

pytest e2e/ -v --timeout=120
```

## Making Changes

1. **Create a new branch** for your feature/fix:
   ```bash
   git checkout -b feature/my-feature
   ```

2. **Make your changes** and write tests:
   - Add unit tests in `tests/`
   - Add E2E tests in `e2e/` if testing S3 integration

3. **Run tests** to ensure nothing breaks:
   ```bash
   pytest tests/ -v        # unit tests
   pytest e2e/ -v          # e2e tests (if you modified core logic)
   ```

4. **Format and lint:**
   ```bash
   black pawn_queue tests
   ruff check --fix pawn_queue tests
   ```

5. **Commit with clear messages:**
   ```bash
   git commit -m "feat: add new feature" -m "Detailed description here"
   ```

6. **Push and create a Pull Request** against the `main` branch.

## Releasing to PyPI

### Prerequisites

- Admin access to the GitHub repository
- PyPI account with publish permissions
- `PYPI_API_TOKEN` secret configured in GitHub Settings → Secrets

### Release Process

1. **Update version** in `pyproject.toml`:
   ```toml
   [project]
   version = "0.2.0"  # Increment from 0.1.0
   ```

2. **Update `CHANGELOG.md`** (if you have one) with release notes.

3. **Commit and push:**
   ```bash
   git add pyproject.toml CHANGELOG.md
   git commit -m "chore: bump version to 0.2.0"
   git push origin main
   ```

4. **Create a GitHub Release:**
   - Go to your repo → Releases → "Create a new release"
   - Tag: `v0.2.0` (must match `pyproject.toml` version)
   - Title: `Release 0.2.0`
   - Description: Summary of changes
   - Click "Publish release"

   The GitHub Actions workflow will:
   - ✅ Verify the version matches the tag
   - ✅ Run all unit tests
   - ✅ Build distributions (wheel + source)
   - ✅ Upload to PyPI
   - ✅ Attach assets to the GitHub Release

5. **Verify the release:**
   ```bash
   pip install --upgrade pawn-queue
   python -c "from pawn_queue import PawnQueue; print(__version__)"
   ```

### Testing a Release with TestPyPI

Before major releases, you can test with TestPyPI:

1. **Go to GitHub → Actions → "Publish to PyPI"**
2. **Click "Run workflow"**
3. **Select environment: `testpypi`**
4. **Click "Run workflow"**

Then test locally:

```bash
pip install --index-url https://test.pypi.org/simple/ pawn-queue
```

## Architecture Notes

- **`pawn_queue/config.py`** — Configuration models (Pydantic v2)
- **`pawn_queue/core.py`** — Main `PawnQueue` class and `PawnQueueBuilder`
- **`pawn_queue/client.py`** — S3 API wrapper (aioboto3)
- **`pawn_queue/lease.py`** — Lease acquisition (both strategies)
- **`pawn_queue/consumer.py`** — Consumer implementation (poll + listen)
- **`pawn_queue/producer.py`** — Producer implementation
- **`tests/`** — Unit tests (using moto in-memory S3)
- **`e2e/`** — E2E tests (against real S3)

## Key Concepts

- **Concurrency strategies:** `conditional_write` (AWS/R2) vs `csprng_verify` (Hetzner/MinIO)
- **Lease management:** 4-step protocol for `csprng_verify` to prevent late-write edge cases
- **Consumer refresh:** Background task renews leases periodically
- **Janitor:** Cleanup of expired leases before polling
- **Message lifecycle:** Publish → Poll/Listen → Ack/Nack → Delete/Dead-letter

See `README.md` for detailed architecture and design decisions.

## Questions or Issues?

- **Open an issue** for bugs or feature requests
- **Start a discussion** for questions or ideas
- **Check existing issues** to see if it's already been discussed

---

Happy contributing! 🎉
