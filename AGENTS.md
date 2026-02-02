# AGENTS.md

## Purpose - This file guides agentic coding assistants working in this
repository. - Keep changes minimal, focused, and aligned with existing style.

## Repository Overview - Project: shard-memo (Python >= 3.10). - Core package:
`shard_memo/`. - Tests: `tests/`. - Examples: `examples/`. - Benchmarks:
`benchmarks/`.

## Build / Lint / Test Commands

### Install (editable + tests) - `pip install -e .` - `pip install -e ".[test]"`

### Build (sdist/wheel) - `python -m build` - Hatchling is configured in
`pyproject.toml`.

### Lint / Type Check - No linting or type-check tools are configured in
`pyproject.toml`. - If adding one, prefer `ruff` + `mypy` with minimal rules.

### Test Suite - Full suite: `pytest` - Single test file: `pytest
tests/test_chunk_memo.py` - Single test by node id: `pytest
tests/test_chunk_memo.py::test_basic_cache_reuse` - With keyword filter: `pytest
-k "cache_reuse"` - Markers: `flaky` marker is defined in `pyproject.toml`.

### Example / Manual Runs - Example script: `python examples/basic.py`

## Cursor / Copilot Rules - No `.cursor/rules/`, `.cursorrules`, or
`.github/copilot-instructions.md` found.

## Code Style Guidelines

### General - Python 3.10+ features are allowed (e.g., `|` unions). - Prefer
explicit, readable code over clever constructs. - Keep functions small and
single-purpose. - Use `pathlib.Path` for filesystem paths. - Breaking API is
totally fine. The code is not 'live' yet.

### Imports - Standard library imports first, then third-party, then local. -
Use absolute imports for package modules. - Keep imports minimal and unused
imports removed. - Group imports with a blank line between groups.

### Formatting - Follow PEP 8 with 4-space indentation. - Keep line lengths
reasonable; wrap long f-strings and dicts. - Use trailing commas in multiline
literals for clean diffs.

### Typing - Use type hints for public functions and methods. - Prefer builtin
collections (`list`, `dict`, `tuple`) with generics. - Use `Any` only when
necessary; narrow when possible. - When a callable signature is important,
specify it with `Callable`.

### Naming - Functions/variables: `snake_case`. - Classes: `PascalCase`. -
Constants: `UPPER_SNAKE_CASE`. - Use descriptive names (e.g., `cache_chunk_spec`,
`exec_chunk_size`).

### Public APIs - The public API is exported from `shard_memo/__init__.py`. -
Keep backwards compatibility in public names and signatures. - Update README
examples if public API changes.

### Error Handling - Validate inputs early; raise `ValueError` for bad
arguments. - Include clear, actionable error messages. - Prefer raising over
silent fallback behavior.

### Concurrency / Multiprocessing - The exec function must be top-level
(pickleable) for `ProcessPoolExecutor`. - Avoid capturing non-pickleable objects
in closures. - Keep worker payloads serializable.

### Serialization / Hashing - Hashing uses stable serialization; preserve
deterministic ordering. - Avoid adding non-deterministic data to chunk hashes.

### Data Structures - Chunk keys are tuples of tuples; maintain immutability. -
Keep memo chunk specs as `dict[str, int | dict]` style.

### Tests - Use `pytest` with clear, descriptive test names. -
Prefer small deterministic tests with temporary directories. - Use
`tempfile.TemporaryDirectory()` to isolate filesystem usage. - Avoid long
sleeps; tests should be fast and stable.

### Logging / Output - The library uses `verbose` flags and avoids heavy
logging. - Keep prints out of library code; tests may print for diagnostics.

### Documentation - README is the primary usage doc; update it if behavior
changes. - Keep docstrings concise and focused on usage.

## File-Specific Guidance

### `shard_memo/core.py` - Core logic and API entry points. - Preserve method
signatures on `ChunkCache` unless necessary. - Maintain deterministic ordering
and chunk behavior.

### `tests/test_chunk_memo.py` - Tests are written in a straightforward,
functional style. - Follow existing patterns for new tests.

### `examples/` - Keep examples minimal and runnable without extra setup.

## When Adding New Tools - If you introduce a formatter/linter, document
commands here. - Keep configuration in `pyproject.toml` when possible.

## Safety / Hygiene - Don’t delete user data or cached files unless tests
create them. - Avoid destructive git operations; preserve unrelated changes.
