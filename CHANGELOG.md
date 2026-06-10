# Changelog

## [Unreleased]

### Added
- `A2AAgentProtocol`: HiveMind agent protocol plugin bridging NL queries to A2A servers
- Vendored `A2AClient`: agent-card discovery, blocking `tasks/send`, SSE `tasks/sendSubscribe`
- Session/context mapping: HiveMind `session_id` → A2A `sessionId`
- Error-safe: always yields a human-readable string before the `None` sentinel
- Unit tests (pytest-httpx mocks): 27 tests
- E2e tests (uvicorn FastAPI mock server): 11 tests
- CI via `gh-automations@dev` shared workflows
