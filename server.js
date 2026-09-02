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

// HorecaControlPlugin connects to this Socket.IO namespace/path.
const io = new Server(httpServer, {
    path: "/plugin-websocket/socket.io",
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    },
    transports: ["polling", "websocket"],
    pingInterval: 25000,
    pingTimeout: 60000,
    connectTimeout: 120000
});

const pluginIO = io.of("/plugin-websocket");

// socket.id -> plugin information
const plugins = new Map();

// requestId -> pending HTTP request
const pendingRequests = new Map();

function now() {
    return new Date().toISOString();
}

function generateRequestId() {
    return crypto.randomUUID();
}

function logJson(title, data) {
    console.log("\n" + title);
    try {
        console.log(JSON.stringify(data, null, 2));
    } catch (error) {
        console.log(String(data));
    }
}

function normalizeMessage(message) {
    if (typeof message !== "string") {
        return message;
    }

    try {
        return JSON.parse(message);
    } catch (error) {
        return message;
    }
}

function findPlugin(body) {
    const requestedSocketId = body.socketId || null;
    const requestedPluginId = body.pluginId || null;
    const requestedDepartmentId = body.departmentId || null;
    const requestedGroupId = body.groupId || null;

    if (requestedSocketId) {
        return plugins.get(requestedSocketId) || null;
    }

    if (requestedPluginId) {
        for (const plugin of plugins.values()) {
            if (String(plugin.pluginId) === String(requestedPluginId)) {
                return plugin;
            }
        }
    }

    if (requestedDepartmentId) {
        for (const plugin of plugins.values()) {
            if (String(plugin.departmentId) === String(requestedDepartmentId)) {
                return plugin;
            }
        }
    }

    if (requestedGroupId) {
        for (const plugin of plugins.values()) {
            if (String(plugin.groupId) === String(requestedGroupId)) {
                return plugin;
            }
        }
    }

    if (plugins.size === 1) {
        return plugins.values().next().value;
    }

    return null;
}

// UI action -> EnumRequestType used by HorecaControlPlugin.
const requestTypeByAction = {
    get_sales: "summaryOfRestaurant",
    get_orders: "currentShiftOrdersList",
    get_payments: "summaryOfRestaurant",
    get_products: "topTenMealsByRevenue",
    get_employees: "revenueByWaiters"
};

// ============================================================
// HEALTH
// ============================================================

app.get("/api/health", (req, res) => {
    res.json({
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
    });
});

// ============================================================
// IIKO CONNECT TEST
// ============================================================

app.post("/api/iiko/connect", (req, res) => {
    res.json({
        success: true,
        message: "AnarSystem API работает",
        received: {
            ip: req.body.ip || null,
            port: req.body.port || null,
            login: req.body.login || null
        }
    });
});

// ============================================================
// PLUGIN STATUS
// ============================================================

app.get("/api/plugin/status", (req, res) => {
    const result = [];

    for (const [socketId, plugin] of plugins.entries()) {
        result.push({
            socketId,
            pluginId: plugin.pluginId,
            pluginName: plugin.pluginName,
            departmentId: plugin.departmentId,
            departmentName: plugin.departmentName,
            groupId: plugin.groupId,
            groupName: plugin.groupName,
            version: plugin.version,
            currencyCode: plugin.currencyCode,
            serverUrl: plugin.serverUrl,
            connectedAt: plugin.connectedAt,
            lastEventAt: plugin.lastEventAt,
            lastResponseAt: plugin.lastResponseAt
        });
    }

    res.json({
        success: true,
        count: result.length,
        plugins: result
    });
});

// ============================================================
// SITE -> PLUGIN REQUEST
// ============================================================

app.post("/api/plugin/request", async (req, res) => {
    const body = req.body || {};
    const action = body.action;

    if (!action) {
        return res.status(400).json({
            success: false,
            error: "action is required"
        });
    }

    const requestType = requestTypeByAction[action];

    if (!requestType) {
        return res.status(400).json({
            success: false,
            error: "Unsupported plugin action",
            action,
            supportedActions: Object.keys(requestTypeByAction)
        });
    }

    const plugin = findPlugin(body);

    if (!plugin) {
        return res.status(503).json({
            success: false,
            error: "No connected plugin found",
            connectedPlugins: plugins.size
        });
    }

    const requestId = generateRequestId();

    // IMPORTANT: HorecaControlPlugin listens for "server_to_plugin"
    // and expects PluginEventData fields, especially requestType.
    const request = {
        chatId: "",
        requestId,
        requestType,
        requestDetail: JSON.stringify(body.params || {})
    };

    logJson("========== SITE -> PLUGIN REQUEST ==========", {
        socketId: plugin.socketId,
        pluginId: plugin.pluginId,
        action,
        request
    });

    return new Promise((resolve) => {
        let finished = false;

        const finish = (statusCode, payload) => {
            if (finished) {
                return;
            }

            finished = true;
            clearTimeout(timer);
            pendingRequests.delete(requestId);
            resolve(res.status(statusCode).json(payload));
        };

        const timer = setTimeout(() => {
            console.log("\n========== PLUGIN REQUEST TIMEOUT ==========");
            console.log("Request ID:", requestId);
            console.log("Action:", action);

            finish(504, {
                success: false,
                error: "Plugin request timeout",
                requestId,
                action
            });
        }, REQUEST_TIMEOUT_MS);

        pendingRequests.set(requestId, {
            finish,
            timer,
            action,
            pluginSocketId: plugin.socketId,
            createdAt: now()
        });

        try {
            // This is the event name used by HorecaControlPlugin.
            plugin.socket.emit("server_to_plugin", request);

            console.log("\n========== REQUEST SENT TO PLUGIN ==========");
            console.log("Socket:", plugin.socketId);
            console.log("Plugin ID:", plugin.pluginId);
            console.log("Request ID:", requestId);
            console.log("Request type:", requestType);
            console.log("Action:", action);
        } catch (error) {
            finish(500, {
                success: false,
                error: "Failed to send request to plugin",
                requestId,
                details: error.message
            });
        }
    });
});

// ============================================================
// SOCKET.IO CONNECTION
// ============================================================

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
        lastEvent: null
    };

    plugins.set(socket.id, plugin);

    console.log("\n=================================");
    console.log("PLUGIN CONNECTED");
    console.log("=================================");
    console.log("Socket:", socket.id);
    console.log("Plugin ID:", plugin.pluginId);
    console.log("Plugin Name:", plugin.pluginName);
    console.log("Department ID:", plugin.departmentId);
    console.log("Department Name:", plugin.departmentName);
    console.log("Group ID:", plugin.groupId);
    console.log("Group Name:", plugin.groupName);
    console.log("Version:", plugin.version);
    console.log("Server URL:", plugin.serverUrl);
    console.log("Time:", now());

    // ========================================================
    // PLUGIN -> SERVER
    // ========================================================
    // Plugin.SendMessage(...) emits "plugin_to_server".
    // This is also where request/response messages arrive.

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

        const requestId =
            message && typeof message === "object"
                ? message.requestId
                : null;

        if (!requestId) {
            return;
        }

        const pending = pendingRequests.get(requestId);

        if (!pending) {
            console.log("No pending request for:", requestId);
            return;
        }

        console.log("\n========== REQUEST COMPLETED ==========");
        console.log("Request ID:", requestId);
        console.log("Action:", pending.action);

        pending.finish(200, {
            success: message.success !== false,
            requestId,
            action: pending.action,
            data: message.data !== undefined ? message.data : null,
            error: message.error || null
        });
    });

    // ========================================================
    // PLUGIN EVENT
    // ========================================================

    socket.on("plugin_to_server_event", (event) => {
        logJson("========== plugin_to_server_event ==========", event);

        plugin.lastEventAt = now();

        // Latest plugin packet is kept only in RAM.
        plugin.lastEvent = event;
    });

    // ========================================================
    // FULL DATA
    // ========================================================

    socket.on("plugin_to_server_full", (data, callback) => {
        logJson("========== plugin_to_server_full ==========", data);
        plugin.lastEventAt = now();

        if (typeof callback === "function") {
            callback({
                success: true,
                receivedAt: now()
            });
        }
    });

    // ========================================================
    // PING
    // ========================================================

    socket.on("plugin_ping", (data, callback) => {
        if (typeof callback === "function") {
            callback({
                success: true,
                serverTime: now(),
                received: data || null
            });
        }
    });

    // ========================================================
    // DISCONNECT
    // ========================================================

    socket.on("disconnect", (reason) => {
        console.log("\n=================================");
        console.log("PLUGIN DISCONNECTED");
        console.log("Socket:", socket.id);
        console.log("Plugin ID:", plugin.pluginId);
        console.log("Reason:", reason);
        console.log("Time:", now());

        for (const [requestId, pending] of pendingRequests.entries()) {
            if (pending.pluginSocketId !== socket.id) {
                continue;
            }

            pending.finish(503, {
                success: false,
                error: "Plugin disconnected",
                requestId,
                action: pending.action
            });
        }

        plugins.delete(socket.id);
    });
});

// ============================================================
// CURRENT PLUGIN DATA — RAM ONLY
// ============================================================

app.get("/api/plugin/data", (req, res) => {
    const result = Array.from(plugins.values()).map((plugin) => ({
        pluginId: plugin.pluginId,
        pluginName: plugin.pluginName,
        departmentId: plugin.departmentId,
        departmentName: plugin.departmentName,
        groupId: plugin.groupId,
        groupName: plugin.groupName,
        version: plugin.version,
        lastEventAt: plugin.lastEventAt,
        data: plugin.lastEvent || null
    }));

    res.json({
        success: true,
        count: result.length,
        plugins: result
    });
});

// ============================================================
// START SERVER
// ============================================================

httpServer.listen(PORT, "127.0.0.1", () => {
    console.log("\n=================================");
    console.log("ANARSYSTEM API");
    console.log("=================================");
    console.log("Node.js:", process.version);
    console.log("Port:", PORT);
    console.log("HTTP: 127.0.0.1:" + PORT);
    console.log("Socket.IO path:", "/plugin-websocket/socket.io");
    console.log("Socket.IO namespace:", "/plugin-websocket");
    console.log("Request API:", "/api/plugin/request");
    console.log("Server started");
    console.log("=================================");
});