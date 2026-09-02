const express = require("express");
const cors = require("cors");
const http = require("http");
const crypto = require("crypto");
const zlib = require("zlib");
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
let historyWriteTimer = null;

function now() { return new Date().toISOString(); }
function generateRequestId() { return crypto.randomUUID(); }

function loadHistoryStore() {
    try {
        if (!fs.existsSync(HISTORY_FILE)) return {};
        const parsed = JSON.parse(fs.readFileSync(HISTORY_FILE, "utf8"));
        return parsed && typeof parsed === "object" ? parsed : {};
    } catch (error) {
        console.error("ORDER HISTORY LOAD FAILED", error.message);
        return {};
    }
}

const historyStore = loadHistoryStore();

function scheduleHistorySave() {
    if (historyWriteTimer) return;
    historyWriteTimer = setTimeout(() => {
        historyWriteTimer = null;
        try {
            const temp = `${HISTORY_FILE}.tmp`;
            fs.writeFileSync(temp, JSON.stringify(historyStore), "utf8");
            fs.renameSync(temp, HISTORY_FILE);
        } catch (error) {
            console.error("ORDER HISTORY SAVE FAILED", error.message);
        }
    }, 500);
}

function logJson(title, data) {
    console.log("\n" + title);
    try { console.log(JSON.stringify(data, null, 2)); }
    catch (error) { console.log(String(data)); }
}

function normalizeMessage(message) {
    if (typeof message !== "string") return message;
    try { return JSON.parse(message); }
    catch (error) { return message; }
}

function decodePluginFullData(raw) {
    if (raw && typeof raw === "object" && !Buffer.isBuffer(raw) && !ArrayBuffer.isView(raw) && !Array.isArray(raw)) return raw;

    let buffer = null;
    if (Buffer.isBuffer(raw)) buffer = raw;
    else if (raw instanceof Uint8Array) buffer = Buffer.from(raw);
    else if (Array.isArray(raw) && raw.every(x => Number.isInteger(x) && x >= 0 && x <= 255)) buffer = Buffer.from(raw);
    else if (typeof raw === "string") {
        try {
            const parsed = JSON.parse(raw);
            if (parsed && typeof parsed === "object") return parsed;
        } catch (_) {}
        try { buffer = Buffer.from(raw, "base64"); } catch (_) {}
    }

    if (!buffer || !buffer.length) return null;

    const attempts = [
        () => zlib.gunzipSync(buffer),
        () => zlib.unzipSync(buffer),
        () => buffer
    ];

    for (const decode of attempts) {
        try {
            const text = decode().toString("utf8");
            try { return JSON.parse(text); } catch (_) {}
        } catch (_) {}
    }
    return null;
}

function findPlugin(body) {
    const requestedSocketId = body.socketId || null;
    const requestedPluginId = body.pluginId || null;
    const requestedDepartmentId = body.departmentId || null;
    const requestedGroupId = body.groupId || null;
    if (requestedSocketId) return plugins.get(requestedSocketId) || null;
    if (requestedPluginId) for (const plugin of plugins.values()) if (String(plugin.pluginId) === String(requestedPluginId)) return plugin;
    if (requestedDepartmentId) for (const plugin of plugins.values()) if (String(plugin.departmentId) === String(requestedDepartmentId)) return plugin;
    if (requestedGroupId) for (const plugin of plugins.values()) if (String(plugin.groupId) === String(requestedGroupId)) return plugin;
    if (plugins.size === 1) return plugins.values().next().value;
    return null;
}

const requestTypeByAction = {
    get_sales: "summaryOfRestaurant",
    get_orders: "getFullDataReport",
    get_payments: "summaryOfRestaurant",
    get_products: "topTenMealsByRevenue",
    get_employees: "revenueByWaiters"
};

function mergeOrderEvent(plugin, event) {
    if (!event || typeof event !== "object") return;
    const data = event.data;
    if (!data || typeof data !== "object") return;
    const number = data.orderNum ?? data.orderNumber ?? data.number;
    if (number === null || number === undefined || number === "") return;

    const key = String(number);
    const previous = plugin.orderDetails.get(key) || {};
    plugin.orderDetails.set(key, {
        ...previous,
        orderNum: number,
        tables: data.tables ?? previous.tables ?? null,
        floor: data.floor ?? previous.floor ?? null,
        waiter: data.waiter ?? previous.waiter ?? null,
        cashier: data.cashier ?? previous.cashier ?? null,
        revenue: data.revenue ?? previous.revenue ?? null,
        openTime: data.openTime ?? previous.openTime ?? null,
        billTime: data.billTime ?? previous.billTime ?? null,
        closeTime: data.closeTime ?? previous.closeTime ?? null,
        lastEventType: event.pluginEventType ?? previous.lastEventType ?? null,
        lastEventAt: now()
    });
}

function recordOrderHistory(plugin, event) {
    if (!event || typeof event !== "object") return;
    const data = event.data;
    if (!data || typeof data !== "object") return;
    const number = data.orderNum ?? data.orderNumber ?? data.number;
    if (number === null || number === undefined || number === "") return;

    const pluginKey = String(plugin.pluginId || "unknown");
    const orderKey = String(number);
    if (!historyStore[pluginKey] || typeof historyStore[pluginKey] !== "object") historyStore[pluginKey] = {};
    if (!Array.isArray(historyStore[pluginKey][orderKey])) historyStore[pluginKey][orderKey] = [];

    const list = historyStore[pluginKey][orderKey];
    const uuid = event.uuid || null;
    if (uuid && list.some(item => item.uuid === uuid)) return;

    list.push({
        uuid,
        pluginEventType: event.pluginEventType || null,
        receivedAt: now(),
        data
    });
    if (list.length > MAX_EVENTS_PER_ORDER) list.splice(0, list.length - MAX_EVENTS_PER_ORDER);
    scheduleHistorySave();
}

function attachPluginHistory(plugin) {
    const pluginKey = String(plugin.pluginId || "unknown");
    const saved = historyStore[pluginKey];
    plugin.orderHistory = new Map();
    if (!saved || typeof saved !== "object") return;
    for (const [orderKey, events] of Object.entries(saved)) {
        if (Array.isArray(events)) plugin.orderHistory.set(orderKey, events);
    }
}

function enrichOrders(value, orderDetails) {
    if (!value || typeof value !== "object") return value;
    if (Array.isArray(value)) return value.map(item => enrichOrders(item, orderDetails));

    const result = { ...value };
    const number = result.orderNum ?? result.OrderNum ?? result.orderNumber ?? result.OrderNumber ?? result.number ?? result.Number;
    if (number !== null && number !== undefined && number !== "") {
        const extra = orderDetails.get(String(number));
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

    for (const [key, child] of Object.entries(result)) result[key] = enrichOrders(child, orderDetails);
    return result;
}

function findPendingOrderRequest(pluginSocketId) {
    let found = null;
    for (const pending of pendingRequests.values()) {
        if (pending.action !== "get_orders") continue;
        if (pending.pluginSocketId !== pluginSocketId) continue;
        if (!found || pending.createdAt < found.createdAt) found = pending;
    }
    return found;
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
    const result = [];
    for (const [socketId, plugin] of plugins.entries()) result.push({
        socketId, pluginId: plugin.pluginId, pluginName: plugin.pluginName,
        departmentId: plugin.departmentId, departmentName: plugin.departmentName,
        groupId: plugin.groupId, groupName: plugin.groupName, version: plugin.version,
        currencyCode: plugin.currencyCode, serverUrl: plugin.serverUrl,
        connectedAt: plugin.connectedAt, lastEventAt: plugin.lastEventAt, lastResponseAt: plugin.lastResponseAt
    });
    res.json({ success: true, count: result.length, plugins: result });
});

app.post("/api/plugin/request", async (req, res) => {
    const body = req.body || {};
    const action = body.action;
    if (!action) return res.status(400).json({ success: false, error: "action is required" });
    const requestType = requestTypeByAction[action];
    if (!requestType) return res.status(400).json({ success: false, error: "Unsupported plugin action", action, supportedActions: Object.keys(requestTypeByAction) });

    const plugin = findPlugin(body);
    if (!plugin) return res.status(503).json({ success: false, error: "No connected plugin found", connectedPlugins: plugins.size });

    const requestId = generateRequestId();
    const request = { chatId: "", requestId, requestType, requestDetail: JSON.stringify(body.params || {}) };
    logJson("========== SITE -> PLUGIN REQUEST ==========", { socketId: plugin.socketId, pluginId: plugin.pluginId, action, request });

    return new Promise((resolve) => {
        let finished = false;
        const finish = (statusCode, payload) => {
            if (finished) return;
            finished = true;
            clearTimeout(timer);
            pendingRequests.delete(requestId);
            resolve(res.status(statusCode).json(payload));
        };
        const timer = setTimeout(() => finish(504, { success: false, error: "Plugin request timeout", requestId, action }), REQUEST_TIMEOUT_MS);
        pendingRequests.set(requestId, { requestId, finish, timer, action, pluginSocketId: plugin.socketId, createdAt: now() });

        try {
            plugin.socket.emit("server_to_plugin", request);
            console.log("REQUEST SENT TO PLUGIN", requestId, requestType, action);
        } catch (error) {
            finish(500, { success: false, error: "Failed to send request to plugin", requestId, details: error.message });
        }
    });
});

app.get("/api/plugin/order-history", (req, res) => {
    const orderNum = req.query.orderNum;
    if (orderNum === undefined || orderNum === null || orderNum === "") return res.status(400).json({ success: false, error: "orderNum is required" });

    const plugin = findPlugin({
        socketId: req.query.socketId,
        pluginId: req.query.pluginId,
        departmentId: req.query.departmentId,
        groupId: req.query.groupId
    });
    if (!plugin) return res.status(503).json({ success: false, error: "No connected plugin found", connectedPlugins: plugins.size });

    const pluginKey = String(plugin.pluginId || "unknown");
    const saved = historyStore[pluginKey]?.[String(orderNum)];
    const live = plugin.orderHistory?.get(String(orderNum));
    const history = Array.isArray(live) ? live : Array.isArray(saved) ? saved : [];

    res.json({
        success: true,
        pluginId: plugin.pluginId,
        orderNum: String(orderNum),
        count: history.length,
        history
    });
});

pluginIO.on("connection", (socket) => {
    const query = socket.handshake.query || {};
    const auth = socket.handshake.auth || {};
    const plugin = {
        socket,
        socketId: socket.id,
        pluginId: query.pluginId || auth.pluginId || null,
        pluginName: query.pluginName || auth.pluginName || null,
        departmentId: query.departmentId || auth.departmentId || null,
        departmentName: query.departmentName || auth.departmentName || null,
        groupId: query.groupId || auth.groupId || null,
        groupName: query.groupName || auth.groupName || null,
        version: query.version || auth.version || null,
        currencyCode: query.currencyCode || auth.currencyCode || null,
        serverUrl: auth.serverUrl || null,
        connectedAt: now(),
        lastEventAt: null,
        lastResponseAt: null,
        lastEvent: null,
        orderDetails: new Map(),
        orderHistory: new Map()
    };
    attachPluginHistory(plugin);
    plugins.set(socket.id, plugin);

    console.log("PLUGIN CONNECTED", socket.id, plugin.pluginId, plugin.pluginName);

    socket.on("plugin_to_server", (rawMessage) => {
        const message = normalizeMessage(rawMessage);
        logJson("========== plugin_to_server ==========", message);
        plugin.lastResponseAt = now();
        plugin.lastEventAt = now();
        if (message && typeof message === "object" && !Array.isArray(message)) {
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

        const requestId = message && typeof message === "object" ? message.requestId : null;
        if (!requestId) return;
        const pending = pendingRequests.get(requestId);
        if (!pending) return;

        let data = message.data !== undefined ? message.data : null;
        if (pending.action === "get_orders" && data) data = enrichOrders(data, plugin.orderDetails);

        pending.finish(200, {
            success: message.success !== false,
            requestId,
            action: pending.action,
            data,
            error: message.error || null
        });
    });

    socket.on("plugin_to_server_event", (event) => {
        logJson("========== plugin_to_server_event ==========", event);
        plugin.lastEventAt = now();
        plugin.lastEvent = event;
        mergeOrderEvent(plugin, event);
        recordOrderHistory(plugin, event);
        const number = event?.data?.orderNum ?? event?.data?.orderNumber ?? event?.data?.number;
        if (number !== null && number !== undefined && number !== "") {
            const pluginKey = String(plugin.pluginId || "unknown");
            const saved = historyStore[pluginKey]?.[String(number)];
            if (Array.isArray(saved)) plugin.orderHistory.set(String(number), saved);
        }
    });

    socket.on("plugin_to_server_full", (rawData, callback) => {
        logJson("========== plugin_to_server_full ==========", {
            type: Buffer.isBuffer(rawData) ? "Buffer" : typeof rawData,
            bytes: Buffer.isBuffer(rawData) ? rawData.length : undefined
        });
        plugin.lastEventAt = now();

        const decoded = decodePluginFullData(rawData);
        if (!decoded) {
            console.log("FULL REPORT DECODE FAILED");
            if (typeof callback === "function") callback({ success: false, error: "Failed to decode full plugin report" });
            return;
        }

        const pending = findPendingOrderRequest(socket.id);
        if (pending) {
            const data = enrichOrders(decoded.Data ?? decoded.data ?? decoded, plugin.orderDetails);
            const responseRequestId = pending.requestId;
            pending.finish(200, {
                success: true,
                requestId: responseRequestId,
                action: "get_orders",
                data,
                error: null
            });
        }

        if (typeof callback === "function") callback({ success: true, receivedAt: now() });
    });

    socket.on("plugin_ping", (data, callback) => {
        if (typeof callback === "function") callback({ success: true, serverTime: now(), received: data || null });
    });

    socket.on("disconnect", (reason) => {
        console.log("PLUGIN DISCONNECTED", socket.id, plugin.pluginId, reason);
        for (const [requestId, pending] of pendingRequests.entries()) {
            if (pending.pluginSocketId !== socket.id) continue;
            pending.finish(503, { success: false, error: "Plugin disconnected", requestId, action: pending.action });
        }
        plugins.delete(socket.id);
    });
});

app.get("/api/plugin/data", (req, res) => {
    const result = Array.from(plugins.values()).map((plugin) => ({
        pluginId: plugin.pluginId, pluginName: plugin.pluginName,
        departmentId: plugin.departmentId, departmentName: plugin.departmentName,
        groupId: plugin.groupId, groupName: plugin.groupName, version: plugin.version,
        lastEventAt: plugin.lastEventAt, data: plugin.lastEvent || null
    }));
    res.json({ success: true, count: result.length, plugins: result });
});

httpServer.listen(PORT, "127.0.0.1", () => {
    console.log("ANARSYSTEM API", process.version, "port", PORT, "Socket.IO", "/plugin-websocket/socket.io", "namespace", "/plugin-websocket");
});
