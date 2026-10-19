"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { createOwnerEventForwarder, forwardedEvent,
    isAlertCandidate, safeWebhookUrl } = require("../owner-event-forwarder");

const plugin = {
    pluginId: "pos-1",
    departmentId: "123e4567-e89b-12d3-a456-426614174000",
    departmentName: "Test Restaurant",
    version: "1.2", serverUrl: "http://internal-iiko"
};
const discount = {
    uuid: "e41484e7-ae0a-4db6-b1d3-123456789abc",
    pluginEventType: "AddDiscountSurchargeItem",
    timestamp: "2026-10-19T15:26:00+04:00",
    data: { orderNum: "45", isDiscount: true, discountName: "Endirim 10%" }
};
const url = "https://feature-owner-mobile-readonl.smarthoreca.pages.dev/api/plugin/ingest";

test("webhook accepts only encrypted pages project endpoint", () => {
    assert.equal(safeWebhookUrl(url), url);
    assert.equal(safeWebhookUrl("https://smarthoreca.pages.dev/api/plugin/ingest"),
        "https://smarthoreca.pages.dev/api/plugin/ingest");
    assert.equal(safeWebhookUrl("http://smarthoreca.pages.dev/api/plugin/ingest"), null);
    assert.equal(safeWebhookUrl("https://evil.example/api/plugin/ingest"), null);
    assert.equal(safeWebhookUrl("https://smarthoreca.pages.dev/api/plugin/events"), null);
});

test("candidate filter excludes ordinary order and payment events", () => {
    assert.equal(isAlertCandidate("AddDiscountSurchargeItem"), true);
    assert.equal(isAlertCandidate("RemovingDiscount"), true);
    assert.equal(isAlertCandidate("DeletionOfPrintedItem"), true);
    assert.equal(isAlertCandidate("NewOrder"), false);
    assert.equal(isAlertCandidate("Payment"), false);
});

test("missing bound socket scope or stable event ID cannot be forwarded", () => {
    assert.equal(forwardedEvent(plugin,{...discount,uuid:null}),null);
    assert.equal(forwardedEvent({...plugin,departmentId:"other"},discount),null);
    assert.equal(forwardedEvent(plugin,{...discount,pluginEventType:"Payment"}),null);
});

test("opt-in legacy forwarding sends only allowlisted POS event to authenticated HTTPS endpoint", async () => {
    const calls=[];
    const bridge=createOwnerEventForwarder({
        env:{SH_PUSH_EVENT_WEBHOOK_URL:url,SH_PUSH_EVENT_WEBHOOK_TOKEN:"test-only-secret",
            SH_PUSH_EVENT_ALLOWED_PLUGIN_IDS:"pos-1"},
        fetchImpl:async (target,options)=>{
            calls.push({target,options});
            return {status:202};
        }
    });
    const first=bridge.enqueue(plugin,discount);
    assert.equal(first.queued,true);
    assert.equal(bridge.enqueue(plugin,discount).duplicate,true);
    // Let the asynchronous network task finish.
    await new Promise(resolve=>setImmediate(resolve));
    assert.equal(calls.length,1);
    assert.equal(calls[0].target,url);
    assert.equal(calls[0].options.headers["X-Plugin-Token"],"test-only-secret");
    const body=JSON.parse(calls[0].options.body);
    assert.equal(body.eventId,discount.uuid);
    assert.equal(body.event,"AddDiscountSurchargeItem");
    assert.equal(body.data.orderNum,"45");
    assert.equal(body.data.isDiscount,true);
    assert.equal(body.departmentId,plugin.departmentId);
    assert.equal(body.pluginId,plugin.pluginId);
    assert.equal(body.timestamp,discount.timestamp);
    assert.ok(!calls[0].options.body.includes("test-only-secret"));
    assert.equal(bridge.enqueue(plugin,discount).duplicate,true);
});

test("forwarding remains off when either endpoint or secret is missing",()=>{
    for(const env of [{}, {SH_PUSH_EVENT_WEBHOOK_URL:url},
      {SH_PUSH_EVENT_WEBHOOK_TOKEN:"not-used"},
      {SH_PUSH_EVENT_WEBHOOK_URL:url,SH_PUSH_EVENT_WEBHOOK_TOKEN:"not-used"}]){
        const bridge=createOwnerEventForwarder({env,fetchImpl:()=>{throw Error("should not call")}});
        assert.equal(bridge.enabled,false);
        assert.equal(bridge.enqueue(plugin,discount).enabled,false);
    }
});


test("legacy POS only alerts for explicitly allowlisted terminal", async () => {
    const calls=[];
    const bridge=createOwnerEventForwarder({
        env:{SH_PUSH_EVENT_WEBHOOK_URL:url,SH_PUSH_EVENT_WEBHOOK_TOKEN:"test-only-secret",
            SH_PUSH_EVENT_ALLOWED_PLUGIN_IDS:"different-terminal"},
        fetchImpl:async (...args) => { calls.push(args); return {status:202}; }
    });
    assert.equal(bridge.enabled,true);
    assert.equal(bridge.enqueue(plugin,discount).eligible,false);
    await new Promise(resolve=>setImmediate(resolve));
    assert.equal(calls.length,0);
    assert.ok(forwardedEvent(plugin,discount));
});

test("event metadata cannot override socket department scope", () => {
    const body = forwardedEvent(plugin, {
        ...discount,
        departmentId: "not-allowed",
        pluginId: "fake-pos",
        data: { orderNum: "45", isDiscount: true }
    });
    assert.equal(body.departmentId, plugin.departmentId);
    assert.equal(body.pluginId, plugin.pluginId);
});

test("real iikoFront discount events are eligible, surcharges are not",()=>{
 const add={...discount,uuid:"df4d9509-46f5-410b-96bb-fdc7abbc3a61",
   pluginEventType:"addingDiscount",data:{name:"Endirim 10%",orderNum:45,valuePercent:10}};
 const remove={...add,pluginEventType:"removingDiscount"};
 const a=forwardedEvent(plugin,add);
 assert.equal(a.event,"addingDiscount");
 assert.equal(a.data.orderNum,45);
 assert.equal(a.data.name,"Endirim 10%");
 assert.equal(forwardedEvent(plugin,remove).event,"removingDiscount");
 assert.equal(isAlertCandidate("addingSurcharge"),false);
 assert.equal(isAlertCandidate("removingSurcharge"),false);
 assert.equal(forwardedEvent(plugin,{...add,pluginEventType:"addingSurcharge"}),null);
});
