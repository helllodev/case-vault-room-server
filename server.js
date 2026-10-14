const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const PORT = process.env.PORT || 10000;
const rooms = new Map();
const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, {'content-type':'application/json','access-control-allow-origin':'*'});
    return res.end(JSON.stringify({ok:true,service:'Case Vault multiplayer',rooms:rooms.size}));
  }
  res.writeHead(404); res.end('Not found');
});
const wss = new WebSocketServer({server, maxPayload: 256 * 1024});
function send(ws, obj) { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
function roster(room) { return [...room.clients].filter(c=>c.readyState===WebSocket.OPEN).map(c=>({peer:c.cvPeer,presence:c.cvPresence||{}})); }
function broadcastRoster(room) { const peers=roster(room); for(const c of room.clients) send(c,{type:'roster',peers}); }
function cleanCode(v) { return String(v||'').toLowerCase().replace(/[^a-z0-9]/g,'').slice(0,12); }
wss.on('connection', ws => {
  ws.cvRoom=null; ws.cvRole=null; ws.cvPeer=''; ws.cvPresence={};
  ws.on('message', raw => {
    let m; try { m=JSON.parse(raw.toString()); } catch { return send(ws,{type:'error',message:'Invalid message.'}); }
    if (m.type==='host' || m.type==='join') {
      if (ws.cvRoom) return send(ws,{type:'error',message:'Already in a room. Reconnect first.'});
      const code=cleanCode(m.code); if(!code) return send(ws,{type:'error',message:'Enter a valid room code.'});
      let room=rooms.get(code);
      if(m.type==='host') {
        if(room) return send(ws,{type:'error',message:'That room code is already in use. Host a new room.'});
        room={code,host:ws,clients:new Set()}; rooms.set(code,room); ws.cvRole='host';
      } else {
        if(!room || !room.host || room.host.readyState!==WebSocket.OPEN) return send(ws,{type:'error',message:'Room not found or host is offline. Host a new room and use its current code.'});
        ws.cvRole='guest';
      }
      ws.cvRoom=room; ws.cvPeer=String(m.peer||'').slice(0,40); ws.cvPresence=m.presence&&typeof m.presence==='object'?m.presence:{}; room.clients.add(ws);
      send(ws,{type:'joined',code,role:ws.cvRole,peer:ws.cvPeer}); broadcastRoster(room); return;
    }
    if (m.type==='presence') {
      if(!ws.cvRoom) return send(ws,{type:'error',message:'Join a room before sending updates.'});
      if(m.presence&&typeof m.presence==='object') ws.cvPresence=m.presence;
      if(m.peer) ws.cvPeer=String(m.peer).slice(0,40);
      broadcastRoster(ws.cvRoom); return;
    }
  });
  ws.on('close',()=>{
    const room=ws.cvRoom; if(!room)return;
    room.clients.delete(ws);
    if(ws.cvRole==='host') {
      for(const c of room.clients) send(c,{type:'roomClosed'});
      rooms.delete(room.code);
      for(const c of room.clients){c.cvRoom=null;c.cvRole=null;}
    } else broadcastRoster(room);
  });
  ws.on('error',()=>{});
});
server.listen(PORT,'0.0.0.0',()=>console.log(`Case Vault room server listening on ${PORT}`));
