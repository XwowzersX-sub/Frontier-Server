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
const wss = new WebSocketServer({ noServer: true, maxPayload: 8 * 1024, handleProtocols: protocols => protocols.has('frontier') ? 'frontier' : false });
const port = Number(process.env.PORT || 10000);
const supabase = process.env.SUPABASE_URL && process.env.SUPABASE_SECRET_KEY
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SECRET_KEY, { auth: { persistSession: false } }) : null;

const WIDTH = 60, HEIGHT = 40, TICK_MS = 5000;
type BuildingType = 'city' | 'factory' | 'port' | 'defense_post' | 'market' | 'farm';
type Building = { type: BuildingType; level: number };
type Tile = { owner: string | null; terrain: 'plain' | 'forest' | 'mountain' | 'water'; troops: number; buildings: Building[] };
type Player = { id: string; name: string; color: string; gold: number; score: number; connected: boolean; home: number; };
type Client = { id: string; socket: WebSocket; joined: boolean; lastAction: number };
type GameState = { width: number; height: number; tiles: Tile[]; players: Player[]; tick: number };
const palette = ['#4b9bd1','#db755e','#b18be0','#d2ad4f','#57b78a','#d77eb0','#78aeb5','#c18b62'];
const players = new Map<string, Player>();
const clients = new Map<WebSocket, Client>();
let tick = 0;
let tiles: Tile[] = Array.from({ length: WIDTH * HEIGHT }, (_, i) => {
  const x = i % WIDTH, y = Math.floor(i / WIDTH);
  const n = Math.abs(Math.sin(x * 12.9898 + y * 78.233) * 43758.5453) % 1;
  const coast = x < 2 || y < 2 || x >= WIDTH - 2 || y >= HEIGHT - 2;
  const lake = Math.abs(x - 43) < 3 && Math.abs(y - 19) < 4;
  return { owner: null, terrain: coast || lake ? 'water' : n > .88 ? 'mountain' : n > .73 ? 'forest' : 'plain', troops: 0, buildings: [] };
});
function idx(x: number, y: number) { return y * WIDTH + x; }
function neighbors(i: number) { const x = i % WIDTH, y = Math.floor(i / WIDTH); return [[x-1,y],[x+1,y],[x,y-1],[x,y+1]].filter(([a,b]) => a! >= 0 && a! < WIDTH && b! >= 0 && b! < HEIGHT).map(([a,b]) => idx(a!,b!)); }
function spawn(player: Player) {
  const starts = [idx(5,5),idx(54,5),idx(5,34),idx(54,34),idx(30,5),idx(30,34),idx(5,20),idx(54,20)];
  const home = starts.find(i => !tiles[i]!.owner && neighbors(i).every(n => !tiles[n]!.owner));
  const chosen = home ?? tiles.findIndex(t => !t.owner);
  player.home = chosen;
  tiles[chosen] = { owner: player.id, terrain: tiles[chosen]!.terrain === 'water' ? 'plain' : tiles[chosen]!.terrain, troops: 20, buildings: [] };
  for (const n of neighbors(chosen)) if (!tiles[n]!.owner && tiles[n]!.terrain !== 'mountain') tiles[n]!.troops = 0;
}
function state(): GameState { return { width: WIDTH, height: HEIGHT, tiles, players: [...players.values()].map(p => ({...p})), tick }; }
let saveQueue = Promise.resolve();
function persistSnapshot(): Promise<void> {
  if (!supabase) return Promise.resolve();
  const snapshot = { id:'main', state:{ width:WIDTH, height:HEIGHT, tiles:tiles.map(t=>({...t})), players:[...players.values()].map(p=>({...p,connected:false})), tick }, updated_at:new Date().toISOString() };
  saveQueue = saveQueue.then(async () => {
    const { error } = await supabase!.from('game_snapshots').upsert(snapshot);
    if (error) console.error('Could not save game snapshot:', error.message);
  }).catch(error => console.error('Snapshot save failed:', error));
  return saveQueue;
}
async function restoreSnapshot() {
  if (!supabase) { console.warn('Supabase server key not configured; match state will be temporary.'); return; }
  try {
    const { data, error } = await supabase.from('game_snapshots').select('state').eq('id','main').maybeSingle();
    if (error) throw error;
    const saved = data?.state as GameState | undefined;
    if (!saved || !Array.isArray(saved.tiles) || saved.tiles.length !== WIDTH * HEIGHT || !Array.isArray(saved.players)) return;
    tiles = saved.tiles.map(t => ({ ...t, buildings: Array.isArray(t.buildings) ? t.buildings : [] }));
    tick = Number.isFinite(saved.tick) ? saved.tick : 0;
    players.clear();
    for (const p of saved.players) if (p && typeof p.id === 'string' && typeof p.name === 'string') players.set(p.id,{...p,connected:false});
    console.log('Restored saved frontier snapshot.');
  } catch (error) { console.error('Could not restore game snapshot:', error); }
}
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
    player = { id: client.id, name, color: palette[players.size % palette.length]!, gold: 160, score: 0, connected: true, home: -1 };
    players.set(client.id, player); spawn(player);
    notify(name + ' entered the frontier.');
  } else { player.connected = true; const updatedName=(requestedName||'').replace(/[^a-zA-Z0-9 _-]/g,'').trim().slice(0,18); if(updatedName)player.name=updatedName; }
  send(client.socket, { type: 'welcome', id: client.id });
  broadcast();
  void persistSnapshot();
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
    player.gold -= 25; tiles[target]!.troops += 15; notify(player.name + ' recruited 15 troops.'); broadcast(); persistSnapshot(); return;
  }
  if (message.type === 'expand' && Number.isInteger(message.source) && Array.isArray(message.path)) {
    const sourceIndex = message.source as number;
    const path = message.path.slice(0,80);
    const percent = Math.max(1,Math.min(100,Math.floor(Number(message.percent)||50)));
    if (sourceIndex < 0 || sourceIndex >= tiles.length || !path.length || path.some((v:unknown)=>!Number.isInteger(v)||Number(v)<0||Number(v)>=tiles.length)) return;
    const source = tiles[sourceIndex]!;
    if (source.owner !== player.id) { send(client.socket,{type:'error',text:'Choose one of your own territories as the troop source.'}); return; }
    let previous = sourceIndex;
    for (const value of path as number[]) {
      if (!neighbors(previous).includes(value)) { send(client.socket,{type:'error',text:'Your route must follow adjacent tiles.'}); return; }
      previous = value;
    }
    const requested = Math.floor(source.troops * percent / 100);
    if (requested < 1) { send(client.socket,{type:'error',text:'That territory does not have enough troops to send.'}); return; }
    source.troops -= requested;
    let remaining = requested, lastTile = sourceIndex, captured = 0;
    for (const targetIndex of path as number[]) {
      const target = tiles[targetIndex]!;
      if (target.owner === player.id) { lastTile = targetIndex; continue; }
      if (target.terrain === 'mountain') break;
      const defender = target.owner ? players.get(target.owner) : undefined;
      const defense = target.owner ? target.troops : 0;
      if (remaining <= defense) break;
      if (defender) defender.score = Math.max(0,defender.score-1);
      remaining -= defense + 1;
      if (tiles[lastTile]!.owner === player.id && lastTile !== sourceIndex) tiles[lastTile]!.troops = 0;
      target.owner = player.id;
      target.troops = 0;
      player.score++;
      captured++;
      lastTile = targetIndex;
      if (remaining <= 0) break;
    }
    if (captured === 0) { source.troops += requested; send(client.socket,{type:'error',text:'Your troops could not capture the first tile on that route.'}); return; }
    tiles[lastTile]!.troops += remaining;
    notify(player.name + ' sent troops along a route and captured ' + captured + ' territories.');
    broadcast(); void persistSnapshot(); return;
  }
  if (message.type === 'build' && Number.isInteger(message.tile) && typeof message.building === 'string') {
    const targetIndex = message.tile as number;
    if (targetIndex < 0 || targetIndex >= tiles.length) return;
    const tile = tiles[targetIndex]!;
    const building = message.building as BuildingType;
    const rules: Record<BuildingType,{cost:number;max:number}> = {
      city:{cost:100,max:4}, factory:{cost:150,max:3}, port:{cost:120,max:1},
      defense_post:{cost:80,max:1}, market:{cost:130,max:3}, farm:{cost:80,max:3}
    };
    const rule = rules[building];
    if (!rule || tile.owner !== player.id || tile.terrain === 'water' || tile.terrain === 'mountain') {
      send(client.socket,{type:'error',text:'Build on one of your own suitable land territories.'}); return;
    }
    tile.buildings = Array.isArray(tile.buildings) ? tile.buildings : [];
    const existing = tile.buildings.filter(b => b.type === building);
    const currentLevel = building === 'city' ? (existing[0]?.level || 0) : existing.length;
    if (currentLevel >= rule.max) { send(client.socket,{type:'error',text:'That building is already at its maximum level here.'}); return; }
    if (building === 'port' && !neighbors(targetIndex).some(n => tiles[n]!.terrain === 'water')) {
      send(client.socket,{type:'error',text:'Ports must be built on a coastline.'}); return;
    }
    const cost = rule.cost * (building === 'city' ? currentLevel + 1 : 1);
    if (player.gold < cost) { send(client.socket,{type:'error',text:'Not enough gold. This build costs ' + cost + '.'}); return; }
    player.gold -= cost;
    if (building === 'city' && existing.length) existing[0]!.level = currentLevel + 1;
    else tile.buildings.push({type:building,level:1});
    notify(player.name + ' constructed ' + building.replace('_',' ') + '.');
    broadcast(); void persistSnapshot(); return;
  }
  if (message.type === 'claim' && Number.isInteger(message.tile)) {
    const targetIndex = message.tile as number;
    const percent = Math.max(1,Math.min(100,Math.floor(Number(message.percent)||50)));
    if (targetIndex < 0 || targetIndex >= tiles.length) return;
    const tile = tiles[targetIndex]!;
    if (tile.owner === player.id) { send(client.socket,{type:'error',text:'That territory is already yours.'}); return; }
    if (tile.terrain === 'mountain' || tile.terrain === 'water') { send(client.socket,{type:'error',text:'That terrain cannot be captured.'}); return; }
    const friendlyNeighbors = neighbors(targetIndex).filter(n => tiles[n]!.owner === player.id);
    if (!friendlyNeighbors.length) { send(client.socket,{type:'error',text:'Click land next to your territory to expand.'}); return; }
    const sourceIndex = friendlyNeighbors.sort((a,b) => tiles[b]!.troops - tiles[a]!.troops)[0]!;
    const source = tiles[sourceIndex]!;
    const sent = Math.floor(source.troops * percent / 100);
    if (sent < 1) { send(client.socket,{type:'error',text:'Not enough troops in the neighboring territory.'}); return; }
    const hasDefense = (tile.buildings || []).some(b => b.type === 'defense_post');
    const defense = Math.ceil(tile.troops * (hasDefense ? 2 : 1));
    source.troops -= sent;
    if (sent <= defense) {
      tile.troops = Math.max(0,defense-sent);
      send(client.socket,{type:'notice',text:'The attack was repelled. Select a larger troop percentage.'});
      broadcast(); void persistSnapshot(); return;
    }
    if (tile.owner) {
      const defender = players.get(tile.owner);
      if (defender) defender.score = Math.max(0,defender.score-1);
    }
    tile.owner = player.id;
    tile.troops = Math.max(1,sent-defense-1);
    player.score++;
    notify(player.name + ' captured a territory.');
    broadcast(); void persistSnapshot(); return;
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
    const offered = (req.headers['sec-websocket-protocol'] || '').split(',').map(value => value.trim());
    const authProtocol = offered.find(value => value.startsWith('auth.'));
    const user = await verify(authProtocol?.slice(5));
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
async function boot() {
  await restoreSnapshot();
  setInterval(() => {
    tick++;
    for (const player of players.values()) {
      const land = tiles.filter(t=>t.owner===player.id);
      const buildingIncome = land.reduce((sum,t) => sum + (t.buildings || []).reduce((n,b) => n + (b.type === 'factory' ? 3 : b.type === 'city' ? b.level * 2 : b.type === 'port' ? 2 : b.type === 'market' ? 4 : 0),0),0);
      player.gold = Math.min(9999, player.gold + Math.max(1,Math.floor(land.length/3)) + buildingIncome);
      for (const tile of land) {
        const city = (tile.buildings || []).find(b => b.type === 'city');
        const cap = city ? 99 + city.level * 50 : 99;
        if (tile.troops < cap && tick % 3 === 0) tile.troops++;
      }
    }
    broadcast();
    if (tick % 3 === 0) persistSnapshot();
  }, TICK_MS);
  http.listen(port,'0.0.0.0',()=>console.log('Frontier server listening on '+port));
}
process.on('SIGTERM', async () => { await persistSnapshot(); http.close(() => process.exit(0)); });
void boot();
