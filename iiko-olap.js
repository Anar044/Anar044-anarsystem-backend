const crypto = require("crypto");

function sha1(text) {
    return crypto.createHash("sha1").update(String(text), "utf8").digest("hex");
}

function clean(value) {
    return String(value ?? "").trim();
}

function credentials(body = {}) {
    const ip = clean(body.ip);
    const port = clean(body.port);
    const login = clean(body.login);
    const password = String(body.password ?? "");
    if (!ip || !port || !login || !password) {
        throw new Error("Заполните IP, порт, логин и пароль iiko");
    }
    return { ip, port, login, password };
}

function normalizeField(name, meta, index) {
    if (typeof meta === "string") {
        return {
            name: clean(name || meta),
            field: clean(name || meta),
            key: clean(name || meta),
            id: clean(name || meta),
            title: clean(name || meta),
            type: "unknown",
            isMeasure: false,
            aggregationAllowed: false,
            groupingAllowed: true,
            filteringAllowed: true,
            tags: [],
            index
        };
    }
    if (!meta || typeof meta !== "object") return null;
    const fieldName = clean(
        name || meta.technicalName || meta.technical_name || meta.field ||
        meta.key || meta.code || meta.id || meta.name
    );
    if (!fieldName) return null;
    const aggregationAllowed =
        meta.aggregationAllowed === true || meta.allowAggregation === true ||
        meta.canAggregate === true || meta.isMeasure === true || meta.measure === true;
    return {
        ...meta,
        name: fieldName,
        field: fieldName,
        key: fieldName,
        id: fieldName,
        technicalName: fieldName,
        title: clean(meta.title || meta.caption || meta.label || meta.displayName || meta.name || fieldName),
        type: clean(meta.type || meta.dataType || meta.kind || "unknown"),
        isMeasure: meta.isMeasure === true || meta.measure === true || aggregationAllowed,
        aggregationAllowed,
        groupingAllowed: meta.groupingAllowed !== false,
        filteringAllowed: meta.filteringAllowed !== false,
        tags: Array.isArray(meta.tags) ? meta.tags : [],
        index
    };
}

function normalizeColumns(raw) {
    const result = [];
    const add = (name, meta) => {
        const field = normalizeField(name, meta, result.length);
        if (field) result.push(field);
    };

    if (Array.isArray(raw)) {
        raw.forEach(item => {
            if (typeof item === "string") add(item, item);
            else if (item) add(item.technicalName || item.field || item.key || item.code || item.id || item.name, item);
        });
    } else if (raw && typeof raw === "object") {
        for (const key of ["fields", "columns", "dimensions", "measures"]) {
            const list = raw[key];
            if (!Array.isArray(list)) continue;
            list.forEach(item => {
                if (typeof item === "string") add(item, item);
                else if (item) add(item.technicalName || item.field || item.key || item.code || item.id || item.name, item);
            });
        }
        for (const [key, value] of Object.entries(raw)) {
            if (["fields", "columns", "dimensions", "measures", "data", "items"].includes(key)) continue;
            if (value && typeof value === "object" && !Array.isArray(value)) add(key, value);
        }
    }

    const map = new Map();
    for (const field of result) {
        const key = field.name.toLowerCase();
        if (!map.has(key)) map.set(key, field);
        else {
            const old = map.get(key);
            map.set(key, {
                ...old,
                ...field,
                title: field.title || old.title,
                isMeasure: old.isMeasure || field.isMeasure,
                aggregationAllowed: old.aggregationAllowed || field.aggregationAllowed
            });
        }
    }
    return [...map.values()].map((field, index) => ({ ...field, index }));
}

async function authenticate(body) {
    const c = credentials(body);
    const serverUrl = `http://${c.ip}:${c.port}`;
    const url = `${serverUrl}/resto/api/auth?login=${encodeURIComponent(c.login)}&pass=${sha1(c.password)}`;
    let response;
    try {
        response = await fetch(url, { method: "GET" });
    } catch (error) {
        throw new Error(`Не удалось подключиться к iiko Server: ${error?.message || "fetch failed"}`);
    }
    const token = (await response.text()).trim();
    if (!response.ok || !token) {
        throw new Error(`Ошибка авторизации iiko: HTTP ${response.status}${token ? ` — ${token.slice(0, 1000)}` : ""}`);
    }
    return { ...c, serverUrl, token };
}

async function fetchJson(url, options = {}) {
    let response;
    try {
        response = await fetch(url, options);
    } catch (error) {
        throw new Error(`Ошибка соединения с iiko: ${error?.message || "fetch failed"}`);
    }
    const text = await response.text();
    let data = null;
    try { data = text ? JSON.parse(text) : {}; } catch (_) {}
    return { response, text, data };
}

async function fields(body) {
    const auth = await authenticate(body);
    const reportType = clean(body.reportType || "SALES").toUpperCase();
    const url = `${auth.serverUrl}/resto/api/v2/reports/olap/columns?key=${encodeURIComponent(auth.token)}&reportType=${encodeURIComponent(reportType)}`;
    const { response, text, data } = await fetchJson(url, { method: "GET", headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error(`iiko OLAP columns HTTP ${response.status}: ${text.slice(0, 5000)}`);
    if (!data || typeof data !== "object") throw new Error("iiko вернул некорректный JSON структуры OLAP");
    const list = normalizeColumns(data);
    if (!list.length) throw new Error("iiko вернул 0 OLAP-полей. Проверьте reportType=SALES и права пользователя iiko.");
    return { auth, reportType, raw: data, fields: list };
}

function toArray(value) {
    if (!Array.isArray(value)) return [];
    return value.map(item => {
        if (typeof item === "string") return clean(item);
        if (item && typeof item === "object") return clean(item.technicalName || item.field || item.name || item.key || item.code || item.id);
        return "";
    }).filter(Boolean);
}

function normalizeOperator(operator) {
    const value = clean(operator).toLowerCase();
    if (["exclude", "not equal", "notequal", "excludelist", "excludevalues", "exclude_list"].includes(value)) return "ExcludeValues";
    if (["includelist", "includevalues", "include_list"].includes(value)) return "IncludeValues";
    if (["daterange", "date_range"].includes(value)) return "DateRange";
    return "IncludeValues";
}

function buildFilters(body) {
    let filters = {};
    if (body.filters && !Array.isArray(body.filters) && typeof body.filters === "object") {
        filters = JSON.parse(JSON.stringify(body.filters));
    }
    if (Array.isArray(body.filters)) {
        for (const item of body.filters) {
            const field = clean(item?.field || item?.technicalName || item?.name || item?.key || item?.code || item?.id);
            if (!field) continue;
            const operator = normalizeOperator(item?.operator);
            if (operator === "DateRange") {
                const from = clean(item?.from).slice(0, 10);
                const to = clean(item?.to || item?.from).slice(0, 10);
                if (from && to) filters[field] = { filterType: "DateRange", periodType: "CUSTOM", from, to, includeLow: true, includeHigh: true };
            } else if (Array.isArray(item?.values)) {
                filters[field] = { filterType: operator, values: item.values.filter(value => value !== "") };
            } else if (item?.value !== undefined && item?.value !== null && item?.value !== "") {
                filters[field] = { filterType: operator, values: [item.value] };
            }
        }
    }
    if (body.from || body.to) {
        const from = clean(body.from).slice(0, 10);
        const to = clean(body.to || body.from).slice(0, 10);
        if (from && to) filters["OpenDate.Typed"] = { filterType: "DateRange", periodType: "CUSTOM", from, to, includeLow: true, includeHigh: true };
    }
    return filters;
}

function buildRequest(body) {
    const rows = toArray(body.groupByRowFields ?? body.rows);
    const columns = toArray(body.groupByColumnFields ?? body.groupByColFields ?? body.columns);
    const measures = Array.isArray(body.measures)
        ? toArray(body.measures)
        : toArray(body.aggregateFields);
    return {
        reportType: clean(body.reportType || "SALES").toUpperCase(),
        buildSummary: body.buildSummary !== false,
        groupByRowFields: rows,
        groupByColFields: columns,
        aggregateFields: measures,
        filters: buildFilters(body)
    };
}

async function query(body) {
    const auth = await authenticate(body);
    const request = buildRequest(body);
    if (!request.groupByRowFields.length && !request.groupByColFields.length && !request.aggregateFields.length) {
        return { status: 400, payload: { success: false, type: "EMPTY_QUERY", message: "Выберите хотя бы одно поле в Строки, Колонки или Показатели", request } };
    }
    const url = `${auth.serverUrl}/resto/api/v2/reports/olap?key=${encodeURIComponent(auth.token)}`;
    const { response, text, data } = await fetchJson(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify(request)
    });
    if (!response.ok) {
        const detail = data?.message || data?.error || data?.description || text.slice(0, 5000);
        return { status: Math.min(599, Math.max(400, response.status)), payload: { success: false, type: "IIKO_ERROR", message: `iiko OLAP HTTP ${response.status}${detail ? `: ${detail}` : ""}`, iikoHttpStatus: response.status, iikoStatusText: response.statusText, request, report: data, rawResponse: text.slice(0, 30000) } };
    }
    if (data && (data.error === true || data.success === false || data.errorMessage || data.errorCode)) {
        const detail = data.message || data.errorMessage || data.errorCode || "iiko вернул ошибку внутри HTTP 200";
        return { status: 502, payload: { success: false, type: "IIKO_BODY_ERROR", message: `iiko OLAP HTTP 200: ${detail}`, iikoHttpStatus: response.status, request, report: data, rawResponse: text.slice(0, 30000) } };
    }
    return { status: 200, payload: { success: true, type: "SUCCESS", iikoHttpStatus: response.status, request, report: data, rawResponse: text.slice(0, 30000) } };
}

async function connect(body) {
    const result = await fields({ ...body, reportType: "SALES" });
    const departments = [];
    try {
        const url = `${result.auth.serverUrl}/resto/api/corporation/departments?key=${encodeURIComponent(result.auth.token)}`;
        const { response, text, data } = await fetchJson(url, { method: "GET", headers: { Accept: "application/json", "Accept-Language": "ru" } });
        if (response.ok) {
            const items = Array.isArray(data) ? data : data?.items || data?.departments || data?.corporateItems || data?.data || [];
            for (const item of items) {
                if (!item || typeof item !== "object") continue;
                const id = item.id ?? item.Id ?? item.ID ?? item.uuid ?? item.UUID;
                const type = String(item.type ?? item.Type ?? "DEPARTMENT").toUpperCase();
                if (id != null && (!type || type === "DEPARTMENT")) departments.push({ id: String(id), name: String(item.name ?? item.Name ?? item.code ?? id), code: String(item.code ?? item.Code ?? ""), type: "DEPARTMENT" });
            }
        } else {
            console.warn("IIKO DEPARTMENTS HTTP", response.status, text.slice(0, 500));
        }
    } catch (error) {
        console.warn("IIKO DEPARTMENTS ERROR", error.message);
    }
    const organizations = departments.map(item => ({ id: item.id, name: item.name, code: item.code, address: "", type: "DEPARTMENT" }));
    return {
        organizationId: departments[0]?.id || "",
        organizations,
        departmentIds: departments.map(item => item.id),
        departments,
        source: "iiko-server-local",
        identityType: "DEPARTMENT",
        identitySource: departments.length ? "departments" : "olap-auth"
    };
}

module.exports = { fields, query, connect };
