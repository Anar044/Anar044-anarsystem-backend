"use strict";

// VPS -> Smart Horeca authenticated webhook. Disabled until BOTH variables
// SH_PUSH_EVENT_WEBHOOK_URL and SH_PUSH_EVENT_WEBHOOK_TOKEN are configured.
// Does not alter the Socket.IO / order-history response paths.
const crypto = require("crypto");
const MAX_RETRIES = 4;
const MAX_PENDING = 256;
const RETRY_MS = [1000, 3000, 9000, 27000];
const SUCCESS_TTL_MS = 48 * 60 * 60 * 1000;
const clean = value => String(value ?? "").trim();

function safeWebhookUrl(raw) {
    const input = clean(raw);
    if (!input) return null;
    let url;
    try { url = new URL(input); } catch { return null; }
    // Restrict outgoing secrets to our Cloudflare Pages project only.
    if (url.protocol !== "https:" || url.username || url.password ||
        !(/^(?:[a-z0-9-]+\.)?smarthoreca\.pages\.dev$/i.test(url.hostname)) ||
        url.pathname !== "/api/plugin/ingest" || url.search || url.hash)
        return null;
    return url.toString();
}

function isAlertCandidate(type) {
    const t = clean(type).toLowerCase().replace(/[\s_.-]/g, "");
    // These are candidates only: Cloudflare does final strict classification,
    // including isDiscount to distinguish a surcharge from a discount.
    return /discount|cancelguestbill|guestbillcancel|cancelprecheck|precheckcancel|deletionofprinteditem|deleteprinteditem|deletedprinteditem|deleteorderitem|removeorderitem|storno|stornir|receiptvoid|voidreceipt|receiptrefund|fiscalreceiptcancel/.test(t);
}

function forwardedEvent(plugin, event) {
    if (!event || typeof event !== "object" || !isAlertCandidate(event.pluginEventType))
        return null;
    const id = clean(event.uuid || event.eventId || event.id);
    const pluginId = clean(plugin?.pluginId);
    const departmentId = clean(plugin?.departmentId);
    // This server supports legacy unverified Socket.IO connections. Only a
    // confirmed HMAC-authenticated socket may generate owner notifications.
    // Do NOT trust a plugin-provided "authenticated" field inside an event.
    if (plugin?.authenticated !== true || !["shared-v1", "device-v1"].includes(
        clean(plugin?.authKeyScope))) return null;
    // Without a stable source event ID, deduplication cannot be guaranteed.
    if (!id || !pluginId || !departmentId || !/^[0-9a-f-]{36}$/i.test(departmentId))
        return null;
    const data = event.data && typeof event.data === "object" &&
        !Array.isArray(event.data) ? event.data : {};
    const occurredAt = clean(event.timestamp || event.createdAt || event.occurredAt);
    return {
        event: clean(event.pluginEventType),
        eventId: id,
        timestamp: occurredAt && Number.isFinite(Date.parse(occurredAt))
            ? occurredAt : new Date().toISOString(),
        departmentId,
        departmentName: clean(plugin.departmentName),
        pluginId,
        pluginVersion: clean(plugin.version),
        serverUrl: clean(plugin.serverUrl),
        currencyCode: clean(plugin.currencyCode),
        groupId: clean(plugin.groupId),
        groupName: clean(plugin.groupName),
        data
    };
}

function createOwnerEventForwarder({
    env = process.env,
    fetchImpl = globalThis.fetch,
    schedule = setTimeout,
    clock = Date.now,
    logger = console
} = {}) {
    const target = safeWebhookUrl(env.SH_PUSH_EVENT_WEBHOOK_URL);
    const token = clean(env.SH_PUSH_EVENT_WEBHOOK_TOKEN);
    const enabled = Boolean(target && token && typeof fetchImpl === "function");
    const pending = new Set();
    const delivered = new Map();

    const enqueue = (plugin, event) => {
        if (!enabled) return { enabled: false };
        const body = forwardedEvent(plugin, event);
        if (!body) return { enabled: true, eligible: false };
        const key = crypto.createHash("sha256").update(JSON.stringify([
            body.departmentId, body.pluginId, body.eventId
        ])).digest("hex");
        const now = clock();
        for (const [k, time] of delivered) {
            if (time <= now) delivered.delete(k);
        }
        if (pending.has(key) || delivered.has(key)) return { enabled: true, duplicate: true };
        if (pending.size >= MAX_PENDING) {
            logger.error("OWNER ALERT BRIDGE: pending queue full");
            return { enabled: true, queued: false, code: "QUEUE_FULL" };
        }
        pending.add(key);
        const deliver = async attempt => {
            try {
                const controller = new AbortController();
                const timeout = schedule(() => controller.abort(), 8000);
                let response;
                try {
                    response = await fetchImpl(target, {
                        method: "POST",
                        headers: {
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                            "X-Plugin-Token": token
                        },
                        body: JSON.stringify(body),
                        signal: controller.signal
                    });
                } finally {
                    clearTimeout(timeout);
                }
                if (response.status === 202) {
                    pending.delete(key);
                    delivered.set(key, clock() + SUCCESS_TTL_MS);
                    return;
                }
                // 4xx implies invalid configuration, identity or payload.
                // Retrying without changing the configuration cannot fix it.
                if (response.status >= 400 && response.status < 500)
                    throw Object.assign(new Error("non-retryable"), { permanent: true, code: response.status });
                throw Object.assign(new Error("webhook delivery failed"), { code: response.status });
            } catch (error) {
                if (error.permanent || attempt + 1 >= MAX_RETRIES) {
                    pending.delete(key);
                    logger.error("OWNER ALERT BRIDGE: delivery failed", {
                        status: typeof error.code === "number" ? error.code : "network",
                        attempt: attempt + 1
                    });
                    return;
                }
                schedule(() => { void deliver(attempt + 1); }, RETRY_MS[attempt]);
            }
        };
        void deliver(0);
        return { enabled: true, queued: true };
    };
    return { enabled, enqueue };
}

module.exports = { createOwnerEventForwarder, forwardedEvent, isAlertCandidate, safeWebhookUrl };
