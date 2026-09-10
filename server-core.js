const express = require("express");
const cors = require("cors");
const http = require("http");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { Server } = require("socket.io");

const app = express();
const httpServer = http.createServer(app);
const PORT = 3000;
const REQUEST_TIMEOUT_MS = 30000;
const HISTORY_FILE = path.join(__dirname, "order-history.json");
const MAX_EVENTS_PER_ORDER = 1000;

app.use(cors());
app.use(express.json({ limit: "20mb" }));

const io = new Server(httpServer, {
    path: "/plugin-websocket/socket.io",
    cors: { origin: "*", methods: ["GET", "POST"] },
    transports: ["polling", "websocket"],
    pingInterval: 25000,
    pingTimeout: 60000,
    connectTimeout: 120000
});

const pluginIO = io.of("/plugin-websocket");
const plugins = new Map();
const pendingRequests = new Map();

let historyStore = {};
try {
    if (fs.existsSync(HISTORY_FILE)) historyStore = JSON.parse(fs.readFileSync(HISTORY_FILE, "utf8")) || {};
} catch (e) {
    console.error("ORDER HISTORY LOAD FAILED", e.message);
}

let historyTimer = null;
function saveHistory() {
    if (historyTimer) return;
    historyTimer = setTimeout(() => {
        historyTimer = null;
        try {
            const tmp = `${HISTORY_FILE}.tmp`;
            fs.writeFileSync(tmp, JSON.stringify(historyStore), "utf8");
            fs.renameSync(tmp, HISTORY_FILE);
        } catch (e) {
            console.error("ORDER HISTORY SAVE FAILED", e.message);
        }
    }, 500);
}

function now() { return new Date().toISOString(); }
function requestId() { return crypto.randomUUID(); }
function normalize(value) {
    if (typeof value !== "string") return value;
    try { return JSON.parse(value); } catch (_) { return value; }
}
function normalizeMessage(value) {
    value = normalize(value);
    if (Array.isArray(value) && value.length === 1) value = value[0];
    return normalize(value);
}

const requestTypeByAction = {
    get_sales: "summaryOfRestaurant",
    get_orders: "currentShiftOrdersList",
    get_order: "order",
    get_payments: "summaryOfRestaurant",
    get_products: "topTenMealsByRevenue",
    get_employees: "revenueByWaiters"
};

function canonicalServerUrl(value) {
    let text = String(value || "").trim();
    if (!text) return "";
    text = text.replace(/\/+$/, "");
    if (!/^https?:\/\//i.test(text)) text = `http://${text}`;
    try {
        const url = new URL(text);
        return `${url.protocol.toLowerCase()}//${url.host.toLowerCase()}`;
    } catch (_) {
        return text.toLowerCase();
    }
}

function requestedDepartmentIds(body = {}) {
    const raw = body.departmentIds ?? body.departmentId ?? body.departments;
    if (Array.isArray(raw)) return raw.map(String).map(x => x.trim()).filter(Boolean);
    if (raw == null || raw === "") return [];
    return String(raw).split(",").map(x => x.trim()).filter(Boolean);
}

function pluginMatchesBinding(plugin, body = {}) {
    const departmentIds = requestedDepartmentIds(body);
    const requestedServer = canonicalServerUrl(body.serverUrl || body.iikoServerUrl || "");

    if (departmentIds.length) {
        if (!plugin.departmentId) return false;
        if (!departmentIds.some(id => String(plugin.departmentId) === id)) return false;
    }

    if (requestedServer) {
        if (!plugin.serverUrl) return false;
        if (canonicalServerUrl(plugin.serverUrl) !== requestedServer) return false;
    }

    return true;
}

function findPlugin(body = {}) {
    const hasBinding = requestedDepartmentIds(body).length > 0 || Boolean(body.serverUrl || body.iikoServerUrl);

    if (body.socketId && plugins.has(body.socketId)) {
        const plugin = plugins.get(body.socketId);
        return !hasBinding || pluginMatchesBinding(plugin, body) ? plugin : null;
    }

    for (const plugin of plugins.values()) {
        if (body.pluginId && String(plugin.pluginId) === String(body.pluginId)) {
            if (!hasBinding || pluginMatchesBinding(plugin, body)) return plugin;
            return null;
        }
    }

    for (const plugin of plugins.values()) {
        if (hasBinding && !pluginMatchesBinding(plugin, body)) continue;
        if (body.departmentId && String(plugin.departmentId) === String(body.departmentId)) return plugin;
        if (body.groupId && String(plugin.groupId) === String(body.groupId)) return plugin;
    }

    if (!hasBinding && plugins.size === 1) return plugins.values().next().value;
    return null;
}

function orderNumber(data) {
    if (!data || typeof data !== "object") return null;
    return data.orderNum ?? data.orderNumber ?? data.OrderNum ?? data.Number ?? data.number ?? null;
}

function valueByNames(data, names) {
    if (!data || typeof data !== "object") return null;
    const wanted = names.map(x => String(x).toLowerCase());
    for (const [key, value] of Object.entries(data)) {
        if (wanted.includes(key.toLowerCase()) && value !== null && value !== undefined && value !== "") return value;
    }
    return null;
}

function deepValueByNames(data, names, depth = 0) {
    if (!data || typeof data !== "object" || depth > 12) return null;
    const wanted = names.map(x => String(x).toLowerCase());
    for (const [key, value] of Object.entries(data)) {
        if (wanted.includes(key.toLowerCase()) && value !== null && value !== undefined && value !== "") return value;
    }
    for (const child of Object.values(data)) {
        const found = deepValueByNames(child, names, depth + 1);
        if (found !== null && found !== undefined && found !== "") return found;
    }
    return null;
}

function mergeOrderEvent(plugin, event) {
    const data = event?.data;
    const number = orderNumber(data);
    if (number === null || number === undefined || number === "") return;
    const key = String(number);
    const old = plugin.orderDetails.get(key) || {};
    plugin.orderDetails.set(key, {
        ...old,
        orderNum: number,
        tables: deepValueByNames(data, ["tables", "orderTables", "table", "tableName"]) ?? old.tables ?? null,
        floor: deepValueByNames(data, ["floor", "floorName", "restaurantSection", "section", "hall"]) ?? old.floor ?? null,
        waiter: deepValueByNames(data, ["waiter", "waiterName", "waiterFullName", "employee", "employeeName", "employeeFullName"]) ?? old.waiter ?? null,
        cashier: deepValueByNames(data, ["cashier", "cashierName", "cashierFullName"]) ?? old.cashier ?? null,
        revenue: deepValueByNames(data, ["revenue", "resultSum", "orderSum", "sum", "total"]) ?? old.revenue ?? null,
        payments: deepValueByNames(data, ["payments", "payment", "paymentType", "paymentTypeName", "paymentMethod", "paymentName"]) ?? old.payments ?? null,
        openTime: deepValueByNames(data, ["openTime", "orderOpenDate", "openedAt", "openingTime"]) ?? old.openTime ?? null,
        billTime: deepValueByNames(data, ["billTime", "orderBillTime", "precheckTime", "precheckAt"]) ?? old.billTime ?? null,
        closeTime: deepValueByNames(data, ["closeTime", "orderCloseTime", "closedAt", "closingTime"]) ?? old.closeTime ?? null,
        lastEventType: event.pluginEventType ?? old.lastEventType ?? null,
        lastEventAt: now()
    });
}

function restoreOrderDetails(plugin, saved) {
    if (!saved || typeof saved !== "object") return;
    for (const list of Object.values(saved)) {
        if (!Array.isArray(list)) continue;
        for (const entry of list) {
            if (!entry || !entry.data) continue;
            mergeOrderEvent(plugin, { pluginEventType: entry.pluginEventType || null, data: entry.data });
        }
    }
}

function recordHistory(plugin, event) {
    const data = event?.data;
    const number = orderNumber(data);
    if (number === null || number === undefined || number === "") return;
    const pk = String(plugin.pluginId || "unknown");
    const ok = String(number);
    if (!historyStore[pk] || typeof historyStore[pk] !== "object") historyStore[pk] = {};
    if (!Array.isArray(historyStore[pk][ok])) historyStore[pk][ok] = [];
    const list = historyStore[pk][ok];
    const uuid = event.uuid || null;
    if (uuid && list.some(x => x.uuid === uuid)) return;
    list.push({ uuid, pluginEventType: event.pluginEventType || null, receivedAt: now(), data });
    if (list.length > MAX_EVENTS_PER_ORDER) list.splice(0, list.length - MAX_EVENTS_PER_ORDER);
    plugin.orderHistory.set(ok, list);
    saveHistory();
}

function firstScalar(value) {
    if (value === null || value === undefined || value === "") return null;
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return value;
    if (Array.isArray(value)) {
        for (const item of value) { const found = firstScalar(item); if (found !== null) return found; }
        return null;
    }
    if (typeof value === "object") {
        for (const key of ["name", "Name", "title", "Title", "productName", "ProductName", "itemName", "ItemName", "dishName", "DishName", "value", "Value"]) {
            if (Object.prototype.hasOwnProperty.call(value, key)) { const found = firstScalar(value[key]); if (found !== null) return found; }
        }
    }
    return null;
}

function collectItemCandidates(value, out = [], depth = 0) {
    if (depth > 8 || value === null || value === undefined) return out;
    if (Array.isArray(value)) { for (const item of value) collectItemCandidates(item, out, depth + 1); return out; }
    if (typeof value !== "object") return out;
    const name = firstScalar(value.itemName ?? value.ItemName ?? value.productName ?? value.ProductName ?? value.dishName ?? value.DishName ?? value.name ?? value.Name ?? value.title ?? value.Title);
    const quantity = value.quantity ?? value.Quantity ?? value.amount ?? value.Amount ?? value.count ?? value.Count ?? value.itemAmount ?? value.ItemAmount;
    const price = value.price ?? value.Price ?? value.unitPrice ?? value.UnitPrice;
    const sum = value.sum ?? value.Sum ?? value.total ?? value.Total ?? value.revenue ?? value.Revenue ?? value.resultSum ?? value.ResultSum;
    if (name !== null && (quantity !== undefined || price !== undefined || sum !== undefined || value.item || value.Item || value.product || value.Product)) out.push({ name, quantity, price, sum });
    for (const [key, child] of Object.entries(value)) {
        if (["orderNum", "OrderNum", "orderNumber", "OrderNumber"].includes(key)) continue;
        collectItemCandidates(child, out, depth + 1);
    }
    return out;
}

function historyItems(plugin, number) {
    const pk = String(plugin.pluginId || "unknown");
    const list = historyStore[pk]?.[String(number)] || plugin.orderHistory.get(String(number)) || [];
    const result = [];
    const seen = new Set();
    for (const entry of list) {
        const type = String(entry.pluginEventType || "").toLowerCase();
        const data = entry.data || {};
        if (/discount|surcharge|delete|remove|item|dish/.test(type)) {
            for (const item of collectItemCandidates(data)) {
                const key = JSON.stringify(item);
                if (!seen.has(key)) { seen.add(key); result.push(item); }
            }
        }
    }
    return result;
}

function enrichOrders(data, orderDetails) {
    const root = normalize(data);
    if (!root || typeof root !== "object") return root;
    const copy = JSON.parse(JSON.stringify(root));
    const groups = Array.isArray(copy?.terminalsGroups) ? copy.terminalsGroups : [];
    for (const group of groups) {
        for (const section of (Array.isArray(group?.restaurantSections) ? group.restaurantSections : [])) {
            for (const key of ["orders", "deliveries"]) {
                if (!Array.isArray(section[key])) continue;
                section[key] = section[key].map(order => {
                    const number = orderNumber(order);
                    if (number == null) return order;
                    const details = orderDetails.get(String(number));
                    return details ? { ...order, ...details } : order;
                });
            }
            if (Array.isArray(section.reserves)) {
                section.reserves = section.reserves.map(reserve => {
                    const order = reserve?.reserveOrder;
                    const number = orderNumber(order);
                    if (number == null) return reserve;
                    const details = orderDetails.get(String(number));
                    return details ? { ...reserve, reserveOrder: { ...order, ...details } } : reserve;
                });
            }
        }
    }
    return copy;
}

app.get("/health", (req, res) => res.json({ success: true, service: "anarsystem-backend", plugins: plugins.size }));

app.post("/api/plugin/request", async (req, res) => {
    const body = req.body || {};
    const action = body.action;
    if (!action) return res.status(400).json({ success: false, error: "action is required" });
    const plugin = findPlugin(body);
    if (!plugin) return res.status(503).json({ success: false, error: "No connected plugin found for the requested iiko Server / Department", connectedPlugins: plugins.size });
    const id = requestId();
    const request = { ...body, requestId: id, requestType: body.requestType || requestTypeByAction[action] || action };
    await new Promise(resolve => {
        const finish = (status, payload) => { clearTimeout(timer); pendingRequests.delete(id); resolve(res.status(status).json(payload)); };
        const timer = setTimeout(() => finish(504, { success: false, error: "Plugin request timeout", requestId: id, action }), REQUEST_TIMEOUT_MS);
        pendingRequests.set(id, { requestId: id, action, pluginSocketId: plugin.socketId, createdAt: now(), finish });
        try { plugin.socket.emit("server_to_plugin", request); }
        catch (e) { finish(500, { success: false, error: e.message, requestId: id, action }); }
    });
});

app.get("/api/plugin/order-history", (req, res) => {
    const number = req.query.orderNum;
    if (number === undefined || number === null || number === "") return res.status(400).json({ success: false, error: "orderNum is required" });
    const plugin = findPlugin(req.query);
    if (!plugin) return res.status(503).json({ success: false, error: "No connected plugin found for the requested iiko Server / Department", connectedPlugins: plugins.size });
    const pk = String(plugin.pluginId || "unknown");
    const history = historyStore[pk]?.[String(number)] || plugin.orderHistory.get(String(number)) || [];
    const items = historyItems(plugin, number);
    res.json({ success: true, pluginId: plugin.pluginId, orderNum: String(number), count: history.length, history, items });
});

pluginIO.on("connection", socket => {
    const q = socket.handshake.query || {};
    const a = socket.handshake.auth || {};
    const plugin = {
        socket,
        socketId: socket.id,
        pluginId: q.pluginId || a.pluginId || null,
        pluginName: q.pluginName || a.pluginName || null,
        departmentId: q.departmentId || a.departmentId || null,
        departmentName: q.departmentName || a.departmentName || null,
        groupId: q.groupId || a.groupId || null,
        groupName: q.groupName || a.groupName || null,
        version: q.version || a.version || null,
        currencyCode: q.currencyCode || a.currencyCode || null,
        serverUrl: a.serverUrl || q.serverUrl || null,
        connectedAt: now(),
        lastEventAt: null,
        lastResponseAt: null,
        lastEvent: null,
        orderDetails: new Map(),
        orderHistory: new Map()
    };
    const saved = historyStore[String(plugin.pluginId || "unknown")];
    if (saved) for (const [key, list] of Object.entries(saved)) if (Array.isArray(list)) plugin.orderHistory.set(key, list);
    restoreOrderDetails(plugin, saved);
    plugins.set(socket.id, plugin);
    console.log("PLUGIN CONNECTED", socket.id, plugin.pluginId, plugin.pluginName, plugin.departmentId, plugin.serverUrl);

    socket.on("plugin_to_server", raw => {
        const message = normalizeMessage(raw);
        plugin.lastResponseAt = now();
        plugin.lastEventAt = now();
        if (message && typeof message === "object") {
            plugin.pluginId = message.pluginId || plugin.pluginId;
            plugin.pluginName = message.pluginName || plugin.pluginName;
            plugin.departmentId = message.departmentId || plugin.departmentId;
            plugin.departmentName = message.departmentName || plugin.departmentName;
            plugin.groupId = message.groupId || plugin.groupId;
            plugin.groupName = message.groupName || plugin.groupName;
            plugin.version = message.version || plugin.version;
            plugin.currencyCode = message.currencyCode || plugin.currencyCode;
            plugin.serverUrl = message.serverUrl || plugin.serverUrl;
        }
        const id = message?.requestId || message?.data?.requestId || message?.result?.requestId;
        if (!id) return;
        const pending = pendingRequests.get(id);
        if (!pending) return;
        let data = message.data !== undefined ? message.data : null;
        if (pending.action === "get_orders" && data) data = enrichOrders(data, plugin.orderDetails);
        pending.finish(200, { success: message.success !== false, requestId: id, action: pending.action, data, error: message.error || null });
    });

    socket.on("plugin_to_server_event", event => {
        plugin.lastEventAt = now();
        plugin.lastEvent = event;
        mergeOrderEvent(plugin, event);
        recordHistory(plugin, event);
    });

    socket.on("disconnect", reason => {
        console.log("PLUGIN DISCONNECTED", socket.id, plugin.pluginId, reason);
        for (const pending of pendingRequests.values()) {
            if (pending.pluginSocketId === socket.id) pending.finish(503, { success: false, error: "Plugin disconnected", requestId: pending.requestId, action: pending.action });
        }
        plugins.delete(socket.id);
    });
});

app.get("/api/plugin/data", (req, res) => {
    const hasBinding = requestedDepartmentIds(req.query || {}).length > 0 || Boolean(req.query?.serverUrl || req.query?.iikoServerUrl);
    const visible = hasBinding
        ? Array.from(plugins.values()).filter(plugin => pluginMatchesBinding(plugin, req.query || {}))
        : Array.from(plugins.values());
    res.json({
        success: true,
        count: visible.length,
        plugins: visible.map(p => ({
            pluginId: p.pluginId,
            pluginName: p.pluginName,
            departmentId: p.departmentId,
            departmentName: p.departmentName,
            groupId: p.groupId,
            groupName: p.groupName,
            version: p.version,
            serverUrl: p.serverUrl,
            lastEventAt: p.lastEventAt,
            data: p.lastEvent || null
        }))
    });
});

httpServer.listen(PORT, "127.0.0.1", () => {
    console.log("ANARSYSTEM API", process.version, "port", PORT, "Socket.IO", "/plugin-websocket/socket.io", "namespace", "/plugin-websocket");
});
