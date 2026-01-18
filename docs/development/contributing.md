# Contributing

> TODO: Dev setup & guidelines

## Development Setup

```bash
# Clone repository
git clone git@github.com:microboxlabs/python-stream-processor.git
cd python-stream-processor

# Install dependencies
uv sync --all-extras

# Run tests
uv run pytest
```

## Code Style

- Black for formatting
- Ruff for linting
- MyPy for type checking

```bash
uv run black src tests
uv run ruff check src tests
uv run mypy src
```

## Branch Naming

- `feat/description` - New features
- `fix/description` - Bug fixes
- `based/{issue-number}-description` - Issue-linked branches

## Commit Messages

Follow conventional commits:
- `feat:` - New feature
- `fix:` - Bug fix
- `refactor:` - Code refactoring
- `docs:` - Documentation
- `test:` - Tests
