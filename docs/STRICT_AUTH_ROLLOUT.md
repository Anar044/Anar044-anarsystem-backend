# Backend strict authentication (staged)

The current VPS stays in legacy mode until plugin clients and API proxy are configured. On the **new self-hosted server**, run strict mode before exposing the service.

Required secret environment variables:
- SH_BACKEND_AUTH_MODE=strict
- SH_BACKEND_PROXY_TOKEN: random 32+ character secret used only between API gateway and backend
- SH_PLUGIN_AUTH_KEYS: JSON map of plugin IDs to token (random 32+ characters), trusted departmentId, and canonical serverUrl, e.g. {"my-plugin":{"token":"replace-with-secure-secret","departmentId":"dept-uuid","serverUrl":"http://rms.example:8080"}}.

Behavior:
1. /api/plugin/* and /api/iiko/* accept X-SH-Backend-Token only from the trusted API proxy. Do not send this token to browsers.
2. Socket.IO clients joining /plugin-websocket must supply socket.handshake.auth.token along with pluginId, departmentId, and serverUrl. **A compatible restaurant plugin build is required before enabling strict mode.**
3. Authenticated plugin identity is immutable during the connection. Unscoped requests to plugin data/order-history/request are rejected in strict mode.
4. Browser tenant RBAC and workspace permissions must still be verified by Smart Horeca API. An internal token does not authorize end users.
5. Run backend behind a trusted reverse proxy bound to localhost, and protect /health at firewall level. Origin/CORS headers are not authentication.
6. Keep per-plugin tokens private, rotate on compromise, and never commit them to Git.
7. An on-prem production deployment requires follow-up review of all endpoint routing and persisted tenant isolation.

Verification: node --check server-core.js && node --check security.js && node --test test/security.test.js

Legacy mode is kept only to avoid disconnecting current live plugins. Never expose legacy mode as a public multi-tenant backend.
