const fs = require("fs");
const http = require("http");
const path = require("path");

const hoursRaw = Number(process.argv[2] || 24);
const hours = Number.isFinite(hoursRaw) ? Math.max(1, Math.min(168, hoursRaw)) : 24;
const stateFile = path.join(__dirname, "..", "plugin-auth-enrollment.json");

function getPluginData() {
    return new Promise((resolve, reject) => {
        const req = http.get("http://127.0.0.1:3000/api/plugin/data", res => {
            let body = "";
            res.setEncoding("utf8");
            res.on("data", chunk => body += chunk);
            res.on("end", () => {
                if (res.statusCode < 200 || res.statusCode >= 300) {
                    return reject(new Error(`plugin data returned HTTP ${res.statusCode}: ${body}`));
                }
                try { resolve(JSON.parse(body)); }
                catch (e) { reject(e); }
            });
        });
        req.setTimeout(5000, () => req.destroy(new Error("plugin data request timed out")));
        req.on("error", reject);
    });
}

(async () => {
    const data = await getPluginData();
    const online = Array.isArray(data.plugins)
        ? data.plugins.filter(p => p && p.online && p.pluginId && p.departmentId)
        : [];

    if (!online.length) {
        throw new Error("No online plugins found. Keep the old plugins connected, then run this script again.");
    }

    const now = new Date();
    const enabledUntil = new Date(now.getTime() + hours * 60 * 60 * 1000);
    const allowed = {};

    for (const p of online) {
        allowed[String(p.pluginId)] = {
            pluginId: String(p.pluginId),
            pluginName: p.pluginName || "",
            departmentId: String(p.departmentId),
            departmentName: p.departmentName || "",
            groupId: p.groupId ? String(p.groupId) : "",
            groupName: p.groupName || "",
            approvedAt: now.toISOString(),
            previousAuthMode: p.authMode || "legacy"
        };
    }

    const state = {
        version: 1,
        createdAt: now.toISOString(),
        enabledUntil: enabledUntil.toISOString(),
        allowed
    };

    const tmp = `${stateFile}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(state, null, 2), { encoding: "utf8", mode: 0o600 });
    fs.renameSync(tmp, stateFile);
    try { fs.chmodSync(stateFile, 0o600); } catch (_) {}

    console.log(`Plugin auto-enrollment opened for ${online.length} currently online plugin(s).`);
    console.log(`Expires: ${enabledUntil.toISOString()}`);
    for (const p of online) {
        console.log(` - ${p.pluginName || p.pluginId} | ${p.departmentName || p.departmentId} | ${p.pluginId}`);
    }
})().catch(err => {
    console.error("Failed to open plugin auto-enrollment:", err.message);
    process.exit(1);
});
