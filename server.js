const express = require("express");
const cors = require("cors");
const http = require("http");
const crypto = require("crypto");
const { Server } = require("socket.io");

const app = express();
const httpServer = http.createServer(app);
const PORT = 3000;
const REQUEST_TIMEOUT_MS = 30000;

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

function now() { return new Date().toISOString(); }
function generateRequestId() { return crypto.randomUUID(); }

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

// Orders are requested from the full plugin report.
const requestTypeByAction = {
    get_sales: "summaryOfRestaurant",
    get_orders: "getFullDataReport",
    get_payments: "summaryOfRestaurant",
    get_products: "topTenMealsByRevenue",
    get_employees: "revenueByWaiters"
};

// RAM cache only. No database and no plugin changes.
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
        pendingRequests.set(requestId, { finish, timer, action, pluginSocketId: plugin.socketId, createdAt: now() });

        try {
            plugin.socket.emit("server_to_plugin", request);
            console.log("REQUEST SENT TO PLUGIN", requestId, requestType, action);
        } catch (error) {
            finish(500, { success: false, error: "Failed to send request to plugin", requestId, details: error.message });
        }
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
        orderDetails: new Map()
    };
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
        // Enrich the full/current order tree with fields received from live plugin events.
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
    });

    socket.on("plugin_to_server_full", (data, callback) => {
        logJson("========== plugin_to_server_full ==========", data);
        plugin.lastEventAt = now();
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