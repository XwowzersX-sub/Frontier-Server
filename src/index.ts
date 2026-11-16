import express from 'express';
import cors from 'cors';
import { createClient } from '@supabase/supabase-js';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';

const app = express();
const allowedOrigins = (process.env.CLIENT_ORIGIN || '*').split(',').map(v => v.trim());
app.use(cors({ origin: (origin, callback) => callback(null, !origin || allowedOrigins.includes('*') || allowedOrigins.includes(origin)) }));
app.use(express.json({ limit: '16kb' }));
const http = createServer(app);
const wss = new WebSocketServer({ noServer: true, maxPayload: 8 * 1024 });
const port = Number(process.env.PORT || 10000);
const supabase = process.env.SUPABASE_URL && process.env.SUPABASE_SECRET_KEY
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SECRET_KEY, { auth: { persistSession: false } }) : null;

const WIDTH = 30, HEIGHT = 20, TICK_MS = 5000;
type Tile = { owner: string | null; terrain: 'plain' | 'forest' | 'mountain'; troops: number };
type Player = { id: string; name: string; color: string; gold: number; score: number; connected: boolean; home: number };
type Client = { id: string; socket: WebSocket; joined: boolean; lastAction: number };
type GameState = { width: number; height: number; tiles: Tile[]; players: Player[]; tick: number };
const palette = ['#4b9bd1','#db755e','#b18be0','#d2ad4f','#57b78a','#d77eb0','#78aeb5','#c18b62'];
const players = new Map<string, Player>();
const clients = new Map<WebSocket, Client>();
let tick = 0;
const tiles: Tile[] = Array.from({ length: WIDTH * HEIGHT }, (_, i) => {
  const x = i % WIDTH, y = Math.floor(i / WIDTH);
  const n = Math.abs(Math.sin(x * 12.9898 + y * 78.233) * 43758.5453) % 1;
  return { owner: null, terrain: n > .88 ? 'mountain' : n > .73 ? 'forest' : 'plain', troops: 0 };
});
function idx(x: number, y: number) { return y * WIDTH + x; }
function neighbors(i: number) { const x = i % WIDTH, y = Math.floor(i / WIDTH); return [[x-1,y],[x+1,y],[x,y-1],[x,y+1]].filter(([a,b]) => a! >= 0 && a! < WIDTH && b! >= 0 && b! < HEIGHT).map(([a,b]) => idx(a!,b!)); }
function spawn(player: Player) {
  const starts = [idx(2,2),idx(27,2),idx(2,17),idx(27,17),idx(14,2),idx(14,17),idx(2,10),idx(27,10)];
  const home = starts.find(i => !tiles[i]!.owner && neighbors(i).every(n => !tiles[n]!.owner));
  const chosen = home ?? tiles.findIndex(t => !t.owner);
  player.home = chosen;
  tiles[chosen] = { owner: player.id, terrain: tiles[chosen]!.terrain, troops: 12 };
  for (const n of neighbors(chosen)) if (!tiles[n]!.owner && tiles[n]!.terrain !== 'mountain') tiles[n]!.troops = 0;
}
function state(): GameState { return { width: WIDTH, height: HEIGHT, tiles, players: [...players.values()].map(p => ({...p})), tick }; }
function send(ws: WebSocket, packet: unknown) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(packet)); }
function broadcast() { const packet = JSON.stringify({ type: 'state', state: state() }); for (const ws of clients.keys()) if (ws.readyState === WebSocket.OPEN) ws.send(packet); }
function notify(text: string) { const packet = JSON.stringify({ type: 'notice', text }); for (const ws of clients.keys()) send(ws, packet); }
async function verify(token?: string) {
  if (!token || !supabase) return null;
  const { data, error } = await supabase.auth.getUser(token);
  return error ? null : data.user;
}
function join(client: Client, requestedName?: string) {
  if (client.joined) return;
  client.joined = true;
  let player = players.get(client.id);
  if (!player) {
    const name = (requestedName || 'Commander').replace(/[^a-zA-Z0-9 _-]/g, '').trim().slice(0, 18) || 'Commander';
    player = { id: client.id, name, color: palette[players.size % palette.length]!, gold: 60, score: 0, connected: true, home: -1 };
    players.set(client.id, player); spawn(player);
    notify(name + ' entered the frontier.');
  } else player.connected = true;
  send(client.socket, { type: 'welcome', id: client.id });
  broadcast();
}
function command(client: Client, raw: string) {
  if (raw.length > 2048) return;
  let message: any; try { message = JSON.parse(raw); } catch { return; }
  if (!message || typeof message.type !== 'string') return;
  if (message.type === 'join') { join(client, typeof message.name === 'string' ? message.name : undefined); return; }
  if (!client.joined) return;
  const player = players.get(client.id); if (!player) return;
  const now = Date.now(); if (now - client.lastAction < 180) return; client.lastAction = now;
  if (message.type === 'recruit') {
    if (player.gold < 25) { send(client.socket, { type:'error', text:'You need 25 gold to recruit.' }); return; }
    const owned = tiles.map((t,i) => t.owner === player.id ? i : -1).filter(i => i >= 0);
    const homeTile = tiles[player.home];
    const target = homeTile?.owner === player.id ? player.home : owned[0];
    if (target === undefined || target < 0) return;
    player.gold -= 25; tiles[target]!.troops += 5; notify(player.name + ' recruited 5 troops.'); broadcast(); return;
  }
  if (message.type === 'claim' && Number.isInteger(message.tile)) {
    const target = message.tile as number;
    if (target < 0 || target >= tiles.length) return;
    const tile = tiles[target]!;
    if (tile.owner === player.id) { send(client.socket,{type:'error',text:'That tile is already yours.'}); return; }
    const ownedNeighbor = neighbors(target).some(n => tiles[n]!.owner === player.id);
    if (!ownedNeighbor) { send(client.socket,{type:'error',text:'Expand from a tile next to your territory.'}); return; }
    if (tile.terrain === 'mountain') { send(client.socket,{type:'error',text:'Mountains cannot be occupied.'}); return; }
    const cost = tile.owner ? 8 : 4;
    if (player.gold < cost) { send(client.socket,{type:'error',text:'Not enough gold.'}); return; }
    if (tile.owner && tile.troops >= 4) { send(client.socket,{type:'error',text:'Enemy tile is defended. Attack after building up more troops.'}); return; }
    player.gold -= cost;
    if (tile.owner) { const defender = players.get(tile.owner); if (defender) defender.score = Math.max(0, defender.score - 1); }
    tile.owner = player.id; tile.troops = Math.max(1, tile.troops + 2); player.score++;
    notify(player.name + (cost === 8 ? ' captured enemy land!' : ' expanded their territory.'));
    broadcast(); return;
  }
  if (message.type === 'ping') send(client.socket,{type:'pong',time:Date.now()});
}
app.get('/', (_req,res) => res.send('Pixel Frontier multiplayer server'));
app.get('/health', (_req,res) => res.json({ ok:true, service:'frontier-server', players:players.size, uptime:Math.floor(process.uptime()) }));
app.get('/api/leaderboard', (_req,res) => res.json([...players.values()].sort((a,b)=>b.score-a.score).slice(0,10).map(({id,name,color,score})=>({id,name,color,score}))));
http.on('upgrade', async (req,socket,head) => {
  const url = new URL(req.url || '/', 'http://localhost');
  if (url.pathname !== '/ws') { socket.destroy(); return; }
  try {
    const user = await verify(url.searchParams.get('token') || undefined);
    const id = user?.id || 'guest-' + randomUUID();
    wss.handleUpgrade(req,socket,head,ws => {
      const client: Client = { id, socket:ws, joined:false, lastAction:0 }; clients.set(ws,client);
      ws.on('message',data => command(client,data.toString()));
      ws.on('close',() => {
        clients.delete(ws);
        const stillConnected = [...clients.values()].some(c => c.id === id && c.joined);
        if (!stillConnected) { const player = players.get(id); if (player) player.connected = false; broadcast(); }
      });
      ws.on('error',() => clients.delete(ws));
    });
  } catch { socket.destroy(); }
});
setInterval(() => {
  tick++;
  for (const player of players.values()) {
    const land = tiles.filter(t=>t.owner===player.id);
    player.gold = Math.min(9999, player.gold + Math.max(1,Math.floor(land.length/3)));
    for (const tile of land) if (tile.troops < 99 && tick % 3 === 0) tile.troops++;
  }
  broadcast();
}, TICK_MS);
http.listen(port,'0.0.0.0',()=>console.log('Frontier server listening on '+port));
