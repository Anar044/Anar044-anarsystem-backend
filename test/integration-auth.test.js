"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const net = require("node:net");
const { setTimeout: sleep } = require("node:timers/promises");
const { io } = require("socket.io-client");

async function reservePort() {
  const server = net.createServer();
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}
const A="a".repeat(48), B="b".repeat(48);
async function connectClient(origin, auth) {
  const socket=io(origin+"/plugin-websocket",{
    path:"/plugin-websocket/socket.io",reconnection:false,transports:["websocket"],timeout:3000,auth
  });
  const event=await new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>reject(Error("Socket.IO handshake timeout")),5000);
    socket.once("connect",()=>{clearTimeout(timer);resolve("connected");});
    socket.once("connect_error",error=>{clearTimeout(timer);resolve(error?.message||"error");});
  });
  return {socket,event};
}

test("HTTP and Socket.IO enforce identity when strict mode is enabled", {timeout:25000}, async t=>{
  const port=await reservePort(),origin="http://127.0.0.1:"+port;
  const env={...process.env,PORT:String(port),SH_BACKEND_AUTH_MODE:"strict",
    SH_BACKEND_PROXY_TOKEN:A,SH_PLUGIN_AUTH_KEYS:JSON.stringify({
      "test-plugin":{token:B,departmentId:"restaurant-1",serverUrl:"http://rms-1.example:8080"}
    })};
  const child=spawn(process.execPath,["server.js"],{cwd:require("node:path").join(__dirname,".."),env,
    stdio:["ignore","pipe","pipe"]});
  let stdout="",stderr="";
  child.stdout.on("data",chunk=>{stdout+=chunk.toString().slice(-1000);});
  child.stderr.on("data",chunk=>{stderr+=chunk.toString().slice(-1000);});
  t.after(()=>{if(child.exitCode===null)child.kill("SIGTERM");});
  let healthy=false;
  for(let i=0;i<90;i++){
    if(child.exitCode!==null)throw Error("Backend exited early: "+stderr.slice(-1200));
    try{const r=await fetch(origin+"/health");if(r.ok){healthy=true;break;}}catch(_){}
    await sleep(100);
  }
  assert.ok(healthy,"Backend failed to start: "+stderr.slice(-1200)+" "+stdout.slice(-600));

  const bad=await fetch(origin+"/api/plugin/data?departmentIds=restaurant-1");
  assert.equal(bad.status,401);
  const auth={"X-SH-Backend-Token":A};
  const unscoped=await fetch(origin+"/api/plugin/data",{headers:auth});
  assert.equal(unscoped.status,400);

  const rejected=await connectClient(origin,{pluginId:"test-plugin",token:"wrong"});
  assert.equal(rejected.event,"PLUGIN_UNAUTHORIZED");
  rejected.socket.disconnect();
  const forged=await connectClient(origin,{pluginId:"test-plugin",token:B,departmentId:"restaurant-2"});
  assert.equal(forged.event,"PLUGIN_UNAUTHORIZED");
  forged.socket.disconnect();

  const good=await connectClient(origin,{pluginId:"test-plugin",token:B,departmentId:"restaurant-1"});
  assert.equal(good.event,"connected");
  t.after(()=>good.socket.disconnect());
  const matching=await fetch(origin+"/api/plugin/data?departmentIds=restaurant-1",{headers:auth});
  assert.equal(matching.status,200);
  const own=await matching.json();
  assert.equal(own.plugins.some(p=>p.pluginId==="test-plugin" && p.departmentId==="restaurant-1"),true);
  const other=await fetch(origin+"/api/plugin/data?departmentIds=restaurant-2",{headers:auth});
  assert.equal(other.status,200);
  assert.equal((await other.json()).plugins.length,0);
  good.socket.emit("plugin_to_server",{pluginId:"test-plugin",departmentId:"restaurant-2"});
  await sleep(100);
  const after=await (await fetch(origin+"/api/plugin/data?departmentIds=restaurant-1",{headers:auth})).json();
  assert.equal(after.plugins.some(p=>p.pluginId==="test-plugin"&&p.departmentId==="restaurant-1"),true);
});
