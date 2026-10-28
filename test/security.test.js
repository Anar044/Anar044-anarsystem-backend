const test = require("node:test");
const assert = require("node:assert/strict");
const { createBackendSecurity, safeEqual } = require("../security");
const T = "a".repeat(48), P = "b".repeat(48);
const settings = {
  SH_BACKEND_AUTH_MODE: "strict",
  SH_BACKEND_PROXY_TOKEN: T,
  SH_PLUGIN_AUTH_KEYS: JSON.stringify({
    "plugin-a": { token: P, departmentId: "department-a", serverUrl: "http://rms-a.example:8080" }
  })
};
const gateway = () => createBackendSecurity(settings);
const socket = (token, extras = {}) => ({
  handshake: { auth: { pluginId: "plugin-a", token, ...extras }, query: {} }, data: {}
});
function callGuard(guard, token) {
  let status = 200, nextCalled = false, payload = null;
  const req = { get: name => name === "X-SH-Backend-Token" ? token : undefined };
  const res = { setHeader() {}, status(x) { status = x; return this; }, json(x) { payload = x; return this; } };
  guard(req, res, () => { nextCalled = true; });
  return { status, nextCalled, payload };
}
test("comparison rejects missing and mismatched credentials", () => {
  assert.equal(safeEqual(T, T), true);
  assert.equal(safeEqual(T, P), false);
  assert.equal(safeEqual("", ""), false);
});
test("legacy mode preserves current deployment until strict rollout", () => {
  const security = createBackendSecurity({});
  assert.equal(security.strict, false);
  assert.equal(callGuard(security.authorizeProxy, "").nextCalled, true);
});
test("strict mode refuses missing or incomplete keys", () => {
  assert.throws(() => createBackendSecurity({SH_BACKEND_AUTH_MODE:"strict"}), /SH_BACKEND_PROXY_TOKEN/);
  assert.throws(() => createBackendSecurity({SH_BACKEND_AUTH_MODE:"strict",SH_BACKEND_PROXY_TOKEN:T}), /SH_PLUGIN_AUTH_KEYS/);
  assert.throws(() => createBackendSecurity({...settings,SH_PLUGIN_AUTH_KEYS:"{}"}), /SH_PLUGIN_AUTH_KEYS/);
});
test("backend API accepts only the configured proxy secret", () => {
  const auth = gateway().authorizeProxy;
  assert.equal(callGuard(auth, null).status, 401);
  assert.equal(callGuard(auth, P).status, 401);
  assert.equal(callGuard(auth, T).nextCalled, true);
});
test("socket requires matching plugin key and registered department", () => {
  const auth = gateway().authorizePlugin;
  const bad = socket("incorrect");
  let error; auth(bad, e => {error=e;}); assert.equal(error?.message,"PLUGIN_UNAUTHORIZED");
  const forged = socket(P,{departmentId:"department-b"});
  error = undefined; auth(forged, e => {error=e;}); assert.equal(error?.message,"PLUGIN_UNAUTHORIZED");
  const good = socket(P, { departmentId:"department-a" });
  error = undefined; auth(good, e => {error=e;});
  assert.equal(error,undefined);
  assert.equal(good.data.shPluginIdentity.departmentId,"department-a");
});
test("event cannot change identity after handshake", () => {
  const rules = gateway(), verified={pluginId:"plugin-a",departmentId:"department-a",serverUrl:"http://rms-a.example:8080"};
  assert.equal(rules.isTrustedPluginMessage(verified,{pluginEventType:"addingDiscount"}),true);
  assert.equal(rules.isTrustedPluginMessage(verified,{departmentId:"department-b"}),false);
  assert.equal(rules.isTrustedPluginMessage(verified,{pluginId:"plugin-b"}),false);
  assert.equal(rules.isTrustedPluginMessage(verified,{serverUrl:"http://other.example"}),false);
});
