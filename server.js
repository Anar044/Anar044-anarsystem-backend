const express = require("express");
const { fields, query, connect } = require("./iiko-olap");

// Keep the existing backend untouched in server-core.js.
// This wrapper only adds the missing direct iiko routes before the core registers its routes.
const originalPost = express.application.post;
let iikoRoutesInstalled = false;

express.application.post = function patchedPost(path, ...handlers) {
    if (path === "/api/iiko/connect" && !iikoRoutesInstalled) {
        iikoRoutesInstalled = true;

        originalPost.call(this, "/api/iiko/olap", async (req, res) => {
            const body = req.body || {};
            const action = String(body.action || "query").trim().toLowerCase();

            try {
                if (action === "fields") {
                    const result = await fields(body);
                    return res.status(200).json({
                        success: true,
                        action: "fields",
                        reportType: result.reportType,
                        count: result.fields.length,
                        fields: result.fields,
                        raw: result.raw
                    });
                }

                if (action === "query") {
                    const result = await query(body);
                    return res.status(result.status).json(result.payload);
                }

                return res.status(400).json({
                    success: false,
                    type: "UNKNOWN_ACTION",
                    message: `Неизвестное действие OLAP: ${action}`,
                    availableActions: ["fields", "query"]
                });
            } catch (error) {
                console.error("[OLAP] ERROR", error);
                return res.status(502).json({
                    success: false,
                    type: "FUNCTION_ERROR",
                    message: error?.message || "Ошибка OLAP"
                });
            }
        });

        originalPost.call(this, "/api/iiko/connect", async (req, res) => {
            try {
                const identity = await connect(req.body || {});
                return res.status(200).json({
                    success: true,
                    message: "iiko Server подключён. Реальная авторизация выполнена.",
                    ...identity
                });
            } catch (error) {
                console.error("[IIKO CONNECT] ERROR", error);
                return res.status(502).json({
                    success: false,
                    message: error?.message || "Ошибка подключения к iiko Server"
                });
            }
        });

        // Do not register the old stub route from server-core.js.
        return this;
    }

    return originalPost.call(this, path, ...handlers);
};

require("./server-core.js");
