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

function historicalOrderDetails(plugin, number) {
    const pk = String(plugin.pluginId || "unknown");
    const list = historyStore[pk]?.[String(number)] || plugin.orderHistory.get(String(number)) || [];
    if (!Array.isArray(list) || !list.length) return null;

    const result = {};
    const fields = {
        tables: ["tables", "orderTables", "table", "tableName"],
        floor: ["floor", "floorName", "restaurantSection"],
        waiter: ["waiter", "waiterName", "waiterFullName"],
        cashier: ["cashier", "cashierName", "cashierFullName"],
        revenue: ["revenue", "resultSum", "orderSum", "sum", "total"],
        openTime: ["openTime", "orderOpenDate", "openedAt", "openingTime"],
        billTime: ["billTime", "orderBillTime", "precheckTime", "precheckAt"],
        closeTime: ["closeTime", "orderCloseTime", "closedAt", "closingTime"],
        payments: ["payments", "payment", "paymentType", "paymentTypeName", "paymentMethod", "paymentName"]
    };

    for (let i = list.length - 1; i >= 0; i--) {
        const data = list[i]?.data;
        if (!data || typeof data !== "object") continue;
        for (const [target, names] of Object.entries(fields)) {
            if (result[target] == null) {
                const value = deepValueByNames(data, names);
                if (value != null) result[target] = value;
            }
        }
        if (Object.keys(result).length === Object.keys(fields).length) break;
    }

    return Object.keys(result).length ? result : null;
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
            mergeOrderEvent(plugin, {
                pluginEventType: entry.pluginEventType || null,
                data: entry.data
            });
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

function firstScalar(value) {
    if (value === null || value === undefined || value === "") return null;
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return value;
    if (Array.isArray(value)) {
        for (const item of value) {
            const found = firstScalar(item);
            if (found !== null) return found;
        }
        return null;
    }
    if (typeof value === "object") {
        for (const key of ["name", "Name", "title", "Title", "productName", "ProductName", "itemName", "ItemName", "dishName", "DishName", "value", "Value"]) {
            if (Object.prototype.hasOwnProperty.call(value, key)) {
                const found = firstScalar(value[key]);
                if (found !== null) return found;
            }
        }
    }
    return null;
}

function collectItemCandidates(value, out = [], depth = 0) {
    if (depth > 8 || value === null || value === undefined) return out;
    if (Array.isArray(value)) {
        for (const item of value) collectItemCandidates(item, out, depth + 1);
        return out;
    }
    if (typeof value !== "object") return out;

    const name = firstScalar(value.itemName ?? value.ItemName ?? value.productName ?? value.ProductName ?? value.dishName ?? value.DishName ?? value.name ?? value.Name ?? value.title ?? value.Title);
    const quantity = value.quantity ?? value.Quantity ?? value.amount ?? value.Amount ?? value.count ?? value.Count ?? value.itemAmount ?? value.ItemAmount;
    const price = value.price ?? value.Price ?? value.unitPrice ?? value.UnitPrice;
    const sum = value.sum ?? value.Sum ?? value.total ?? value.Total ?? value.revenue ?? value.Revenue ?? value.resultSum ?? value.ResultSum;

    if (name !== null && (quantity !== undefined || price !== undefined || sum !== undefined || value.item || value.Item || value.product || value.Product)) {
        out.push({ name, quantity, price, sum });
    }

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
        const candidates = [];

        const directValues = [
            data.item, data.Item, data.orderItem, data.OrderItem,
            data.product, data.Product, data.menuItem, data.MenuItem,
            data.items, data.Items, data.orderItems, data.OrderItems,
            data.deletedItem, data.DeletedItem, data.addedItem, data.AddedItem
        ];
        for (const value of directValues) collectItemCandidates(value, candidates);
        collectItemCandidates(data, candidates);

        const isItemEvent = /(add|added|item|product|dish|delete|deletion|remove|removed|printed)/.test(type);
        if (!isItemEvent && !candidates.length) continue;

        for (const item of candidates) {
            const name = String(item.name ?? "").trim();
            if (!name) continue;
            const qty = item.quantity === undefined || item.quantity === null || item.quantity === "" ? 1 : item.quantity;
            const key = `${entry.uuid || entry.receivedAt}|${name}|${qty}|${item.sum ?? ""}`;
            if (seen.has(key)) continue;
            seen.add(key);

            const deleted = /(delete|deletion|remove|removed|storno|void|cancel)/.test(type);
            result.push({
                name,
                quantity: qty,
                price: item.price ?? null,
                sum: item.sum ?? null,
                status: deleted ? "Удалено" : "Добавлено",
                eventType: entry.pluginEventType || null,
                eventAt: entry.receivedAt
            });
        }
    }

    return result;
}

function enrichOrders(value, details, plugin) {
    if (Array.isArray(value)) return value.map(x => enrichOrders(x, details, plugin));
    if (!value || typeof value !== "object") return value;

    const result = { ...value };
    const number = orderNumber(result);
    if (number !== null && number !== undefined && number !== "") {
        const extra = details.get(String(number)) || historicalOrderDetails(plugin, number);
        if (extra) {
            if (result.waiter == null && result.Waiter == null) result.waiter = extra.waiter;
            if (result.cashier == null && result.Cashier == null) result.cashier = extra.cashier;
            if (result.floor == null && result.Floor == null) result.floor = extra.floor;
            if (result.tables == null && result.Tables == null && result.orderTables == null && result.OrderTables == null) result.tables = extra.tables;
            if (result.revenue == null && result.Revenue == null && result.orderExpectedRevenue == null && result.OrderExpectedRevenue == null) result.revenue = extra.revenue;
            if (result.payments == null && result.Payments == null) result.payments = extra.payments;
            if (result.paymentType == null && result.PaymentType == null && typeof extra.payments === "string") result.paymentType = extra.payments;
            if (result.openTime == null && result.OpenTime == null && result.orderOpenDate == null && result.OrderOpenDate == null) result.openTime = extra.openTime;
            if (result.billTime == null && result.BillTime == null && result.orderBillTime == null && result.OrderBillTime == null) result.billTime = extra.billTime;
            if (result.closeTime == null && result.CloseTime == null && result.orderCloseTime == null && result.OrderCloseTime == null) result.closeTime = extra.closeTime;
        }

        const hasItems = ["items", "Items", "orderItems", "OrderItems", "products", "Products"].some(key => Array.isArray(result[key]) && result[key].length);
        if (!hasItems && plugin) {
            const reconstructed = historyItems(plugin, number);
            if (reconstructed.length) result.items = reconstructed;
        }
    }
    for (const [key, child] of Object.entries(result)) result[key] = enrichOrders(child, details, plugin);
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
    let requestDetail = JSON.stringify(body.params || {});
    if (action === "get_order") {
        const number = body.params?.orderNum ?? body.params?.orderNumber ?? body.orderNum ?? body.orderNumber;
        requestDetail = String(number ?? "");
    }

    const request = {
        chatId: "",
        requestId: id,
        requestType: type,
        requestDetail
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
    restoreOrderDetails(plugin, saved);

    plugins.set(socket.id, plugin);
    console.log("PLUGIN CONNECTED", socket.id, plugin.pluginId, plugin.pluginName);

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
        if (pending.action === "get_orders" && data) data = enrichOrders(data, plugin.orderDetails, plugin);

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
