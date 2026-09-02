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

const requestTypeByAction = {
    get_sales: "summaryOfRestaurant",
    get_orders: "currentShiftOrdersList",
    get_payments: "summaryOfRestaurant",
    get_products: "topTenMealsByRevenue",
    get_employees: "revenueByWaiters"
};

function findPlugin(body = {}) {
    if (body.socketId && plugins.has(body.socketId)) return plugins.get(body.socketId);
    for (const plugin of plugins.values()) {
        if (body.pluginId && String(plugin.pluginId) === String(body.pluginId)) return plugin;
        if (body.departmentId && String(plugin.departmentId) === String(body.departmentId)) return plugin;
        if (body.groupId && String(plugin.groupId) === String(body.groupId)) return plugin;
    }
    return plugins.size === 1 ? plugins.values().next().value : null;
}

function orderNumber(data) {
    if (!data || typeof data !== "object") return null;
    return data.orderNum ?? data.orderNumber ?? data.OrderNum ?? data.Number ?? data.number ?? null;
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
        tables: data.tables ?? old.tables ?? null,
        floor: data.floor ?? old.floor ?? null,
        waiter: data.waiter ?? old.waiter ?? null,
        cashier: data.cashier ?? old.cashier ?? null,
        revenue: data.revenue ?? old.revenue ?? null,
        openTime: data.openTime ?? old.openTime ?? null,
        billTime: data.billTime ?? old.billTime ?? null,
        closeTime: data.closeTime ?? old.closeTime ?? null,
        lastEventType: event.pluginEventType ?? old.lastEventType ?? null,
        lastEventAt: now()
    });
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

    list.push({
        uuid,
        pluginEventType: event.pluginEventType || null,
        receivedAt: now(),
        data
    });
    if (list.length > MAX_EVENTS_PER_ORDER) list.splice(0, list.length - MAX_EVENTS_PER_ORDER);
    plugin.orderHistory.set(ok, list);
    saveHistory();
}

function enrichOrders(value, details) {
    if (Array.isArray(value)) return value.map(x => enrichOrders(x, details));
    if (!value || typeof value !== "object") return value;

    const result = { ...value };
    const number = orderNumber(result);
    if (number !== null && number !== undefined && number !== "") {
        const extra = details.get(String(number));
        if (extra) {
            if (result.waiter == null && result.Waiter == null) result.waiter = extra.waiter;
            if (result.cashier == null && result.Cashier == null) result.cashier = extra.cashier;
            if (result.floor == null && result.Floor == null) result.floor = extra.floor;
            if (result.tables == null && result.Tables == null && result.orderTables == null && result.OrderTables == null) result.tables = extra.tables;
            if (result.revenue == null && result.Revenue == null && result.orderExpectedRevenue == null && result.OrderExpectedRevenue == null) result.revenue = extra.revenue;
            if (result.openTime == null && result.OpenTime == null && result.orderOpenDate == null && result.OrderOpenDate == null) result.openTime = extra.openTime;
            if (result.billTime == null && result.BillTime == null && result.orderBillTime == null && result.OrderBillTime == null) result.billTime = extra.billTime;
            if (result.closeTime == null && result.CloseTime == null && result.orderCloseTime == null && result.OrderCloseTime == null) result.closeTime = extra.closeTime;
        }
    }
    for (const [key, child] of Object.entries(result)) result[key] = enrichOrders(child, details);
    return result;
}

app.get("/api/health", (req, res) => res.json({
    success: true,
    service: "AnarSystem API",
    server: "Oracle VPS",
    node: process.version,
    time: now(),
    socketIo: true,
    socketIoPath: "/plugin-websocket/socket.io",
    socketIoNamespace: "/plugin-websocket",
    connectedPlugins: plugins.size,
    pendingRequests: pendingRequests.size
}));

app.post("/api/iiko/connect", (req, res) => res.json({
    success: true,
    message: "AnarSystem API работает",
    received: { ip: req.body.ip || null, port: req.body.port || null, login: req.body.login || null }
}));

app.get("/api/plugin/status", (req, res) => {
    res.json({
        success: true,
        count: plugins.size,
        plugins: Array.from(plugins.values()).map(p => ({
            socketId: p.socketId,
            pluginId: p.pluginId,
            pluginName: p.pluginName,
            departmentId: p.departmentId,
            departmentName: p.departmentName,
            groupId: p.groupId,
            groupName: p.groupName,
            version: p.version,
            currencyCode: p.currencyCode,
            serverUrl: p.serverUrl,
            connectedAt: p.connectedAt,
            lastEventAt: p.lastEventAt,
            lastResponseAt: p.lastResponseAt
        }))
    });
});

app.post("/api/plugin/request", (req, res) => {
    const body = req.body || {};
    const action = body.action;
    const type = requestTypeByAction[action];
    if (!action || !type) return res.status(400).json({ success: false, error: "Unsupported plugin action", action });

    const plugin = findPlugin(body);
    if (!plugin) return res.status(503).json({ success: false, error: "No connected plugin found", connectedPlugins: plugins.size });

    const id = requestId();
    const request = {
        chatId: "",
        requestId: id,
        requestType: type,
        requestDetail: JSON.stringify(body.params || {})
    };

    return new Promise(resolve => {
        let done = false;
        const finish = (status, payload) => {
            if (done) return;
            done = true;
            clearTimeout(timer);
            pendingRequests.delete(id);
            resolve(res.status(status).json(payload));
        };
        const timer = setTimeout(() => finish(504, {
            success: false,
            error: "Plugin request timeout",
            requestId: id,
            action
        }), REQUEST_TIMEOUT_MS);

        pendingRequests.set(id, {
            requestId: id,
            action,
            pluginSocketId: plugin.socketId,
            createdAt: now(),
            finish
        });

        try {
            plugin.socket.emit("server_to_plugin", request);
        } catch (e) {
            finish(500, { success: false, error: e.message, requestId: id, action });
        }
    });
});

app.get("/api/plugin/order-history", (req, res) => {
    const number = req.query.orderNum;
    if (number === undefined || number === null || number === "") return res.status(400).json({ success: false, error: "orderNum is required" });
    const plugin = findPlugin(req.query);
    if (!plugin) return res.status(503).json({ success: false, error: "No connected plugin found", connectedPlugins: plugins.size });

    const pk = String(plugin.pluginId || "unknown");
    const history = historyStore[pk]?.[String(number)] || plugin.orderHistory.get(String(number)) || [];
    res.json({ success: true, pluginId: plugin.pluginId, orderNum: String(number), count: history.length, history });
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
        serverUrl: a.serverUrl || null,
        connectedAt: now(),
        lastEventAt: null,
        lastResponseAt: null,
        lastEvent: null,
        orderDetails: new Map(),
        orderHistory: new Map()
    };

    const saved = historyStore[String(plugin.pluginId || "unknown")];
    if (saved) for (const [key, list] of Object.entries(saved)) if (Array.isArray(list)) plugin.orderHistory.set(key, list);

    plugins.set(socket.id, plugin);
    console.log("PLUGIN CONNECTED", socket.id, plugin.pluginId, plugin.pluginName);

    socket.on("plugin_to_server", raw => {
        const message = normalize(raw);
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

        const id = message?.requestId;
        if (!id) return;
        const pending = pendingRequests.get(id);
        if (!pending) return;

        let data = message.data !== undefined ? message.data : null;
        if (pending.action === "get_orders" && data) data = enrichOrders(data, plugin.orderDetails);

        pending.finish(200, {
            success: message.success !== false,
            requestId: id,
            action: pending.action,
            data,
            error: message.error || null
        });
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
            if (pending.pluginSocketId === socket.id) pending.finish(503, {
                success: false,
                error: "Plugin disconnected",
                requestId: pending.requestId,
                action: pending.action
            });
        }
        plugins.delete(socket.id);
    });
});

app.get("/api/plugin/data", (req, res) => {
    res.json({
        success: true,
        count: plugins.size,
        plugins: Array.from(plugins.values()).map(p => ({
            pluginId: p.pluginId,
            pluginName: p.pluginName,
            departmentId: p.departmentId,
            departmentName: p.departmentName,
            groupId: p.groupId,
            groupName: p.groupName,
            version: p.version,
            lastEventAt: p.lastEventAt,
            data: p.lastEvent || null
        }))
    });
});

httpServer.listen(PORT, "127.0.0.1", () => {
    console.log("ANARSYSTEM API", process.version, "port", PORT, "Socket.IO", "/plugin-websocket/socket.io", "namespace", "/plugin-websocket");
});
