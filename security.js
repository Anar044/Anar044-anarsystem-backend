"use strict";
const crypto = require("node:crypto");
function safeEqual(left, right) {
  if (typeof left !== "string" || typeof right !== "string" || !left || !right) return false;
  const first = crypto.createHash("sha256").update(left).digest();
  const second = crypto.createHash("sha256").update(right).digest();
  return crypto.timingSafeEqual(first, second);
}
const text = value => String(value == null ? "" : value).trim();
const origin = value => text(value).replace(/\/+$/, "").toLowerCase();

function createBackendSecurity(env = process.env) {
  const mode = text(env.SH_BACKEND_AUTH_MODE || "legacy").toLowerCase();
  if (mode !== "legacy" && mode !== "strict") throw Error("Invalid SH_BACKEND_AUTH_MODE");
  const strict = mode === "strict";
  const proxyToken = text(env.SH_BACKEND_PROXY_TOKEN);
  let bindings = Object.create(null);
  if (strict) {
    if (proxyToken.length < 32) throw Error("SH_BACKEND_PROXY_TOKEN must contain at least 32 characters");
    try {
      const raw = JSON.parse(env.SH_PLUGIN_AUTH_KEYS || "{}");
      if (!raw || Array.isArray(raw) || typeof raw !== "object") throw Error("Expected object");
      bindings = raw;
    } catch (_) {
      throw Error("SH_PLUGIN_AUTH_KEYS must be a JSON object");
    }
    if (!Object.keys(bindings).length) throw Error("SH_PLUGIN_AUTH_KEYS must contain registered plugins");
    for (const [id, rule] of Object.entries(bindings)) {
      if (!id || !rule || typeof rule !== "object" ||
          text(rule.token).length < 32 || !text(rule.departmentId) || !text(rule.serverUrl)) {
        throw Error("Invalid SH_PLUGIN_AUTH_KEYS plugin identity configuration");
      }
    }
  }

  function authorizeProxy(req, res, next) {
    if (!strict) return next();
    const token = req.get ? req.get("X-SH-Backend-Token") : req.headers?.["x-sh-backend-token"];
    if (!safeEqual(token, proxyToken)) {
      res.setHeader("Cache-Control", "no-store");
      return res.status(401).json({ success: false, code: "SH_BACKEND_PROXY_UNAUTHORIZED" });
    }
    next();
  }

  function authorizePlugin(socket, next) {
    if (!strict) return next();
    const auth = socket.handshake?.auth || {};
    const query = socket.handshake?.query || {};
    const pluginId = text(auth.pluginId || query.pluginId);
    const rule = Object.hasOwn(bindings, pluginId) ? bindings[pluginId] : null;
    const providedDept = text(auth.departmentId || query.departmentId);
    const providedServer = origin(auth.serverUrl || query.serverUrl);
    if (!rule || !safeEqual(auth.token, rule.token) ||
        (providedDept && providedDept !== text(rule.departmentId)) ||
        (providedServer && providedServer !== origin(rule.serverUrl))) {
      return next(new Error("PLUGIN_UNAUTHORIZED"));
    }
    socket.data ||= {};
    socket.data.shPluginIdentity = Object.freeze({
      pluginId, departmentId: text(rule.departmentId), serverUrl: text(rule.serverUrl)
    });
    next();
  }

  function isTrustedPluginMessage(identity, message) {
    if (!strict) return true;
    if (!identity || !message || typeof message !== "object") return false;
    return (!message.pluginId || text(message.pluginId) === identity.pluginId) &&
      (!message.departmentId || text(message.departmentId) === identity.departmentId) &&
      (!message.serverUrl || origin(message.serverUrl) === origin(identity.serverUrl));
  }
  return { strict, authorizeProxy, authorizePlugin, isTrustedPluginMessage };
}
module.exports = { createBackendSecurity, safeEqual };
