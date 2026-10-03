const fs = require("fs");
const path = require("path");

const stateFile = path.join(__dirname, "..", "plugin-auth-enrollment.json");
let state = { version: 1, allowed: {} };

try {
    if (fs.existsSync(stateFile)) {
        state = JSON.parse(fs.readFileSync(stateFile, "utf8")) || state;
    }
} catch (e) {
    console.error("Failed to read plugin auth enrollment state:", e.message);
    process.exit(1);
}

state.enabledUntil = new Date(0).toISOString();
state.closedAt = new Date().toISOString();

const tmp = `${stateFile}.tmp`;
fs.writeFileSync(tmp, JSON.stringify(state, null, 2), { encoding: "utf8", mode: 0o600 });
fs.renameSync(tmp, stateFile);
try { fs.chmodSync(stateFile, 0o600); } catch (_) {}

console.log("Plugin auto-enrollment closed.");
