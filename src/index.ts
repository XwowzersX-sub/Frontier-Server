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
const wss = new WebSocketServer({ noServer: true, maxPayload: 8 * 1024, perMessageDeflate: { threshold: 1024, serverNoContextTakeover: true, clientNoContextTakeover: true, zlibDeflateOptions: { level: 3 } }, handleProtocols: protocols => protocols.has('frontier') ? 'frontier' : false });
const port = Number(process.env.PORT || 10000);
const supabase = process.env.SUPABASE_URL && process.env.SUPABASE_SECRET_KEY
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SECRET_KEY, { auth: { persistSession: false } }) : null;

const WIDTH = 180, HEIGHT = 110, TICK_MS = 4500, MAX_PLAYERS = 50, WIN_LAND = 0.8;
const MAPS = ['Continents','Island Chain','Pangaea','Highlands','Twin Worlds','Inland Sea','Atolls','Supercontinent','Fractal Wilds','Earthlike'];
type BuildingType = 'city' | 'factory' | 'port' | 'defense_post' | 'market' | 'farm';
type Building = { type: BuildingType; level: number };
type Tile = { owner: string | null; terrain: 'plain' | 'forest' | 'mountain' | 'water'; troops: number; buildings: Building[] };
type Player = { id: string; name: string; color: string; gold: number; score: number; connected: boolean; home: number; spawnPreference: string };
type Client = { id: string; socket: WebSocket; joined: boolean; lastAction: number };
type GameState = { width: number; height: number; tiles: Tile[]; players: Player[]; tick: number; round: number; mapName: string; capacity: number };
const players = new Map<string, Player>();
const clients = new Map<WebSocket, Client>();
let tick = 0, round = 1, mapName = 'Continents', roundPlayerCount = 0, finishing = false;
let tiles: Tile[] = [];
let nextMatchTimer: ReturnType<typeof setTimeout> | null = null;

function index(x: number, y: number) { return y * WIDTH + x; }
function colorFor(slot: number) { return 'hsl(' + ((slot * 137.508 + 195) % 360).toFixed(1) + ' 66% 58%)'; }
function hashNoise(x: number, y: number, seed: number) {
  const v = Math.sin((x + seed * 31.7) * 12.9898 + (y - seed * 17.3) * 78.233) * 43758.5453;
  return Math.abs(v) % 1;
}
function makeMap(name: string, seed: number): Tile[] {
  return Array.from({ length: WIDTH * HEIGHT }, (_, i) => {
    const x = i % WIDTH, y = Math.floor(i / WIDTH);
    const nx = (x - WIDTH / 2) / (WIDTH / 2), ny = (y - HEIGHT / 2) / (HEIGHT / 2);
    const wave = Math.sin(x * .055 + seed) * .42 + Math.cos(y * .073 - seed * .7) * .36
      + Math.sin((x + y) * .031 + seed * 1.4) * .27 + Math.cos((x - y) * .039 - seed) * .2;
    const n = hashNoise(x, y, seed), d1 = Math.hypot((nx + .48) * .94, (ny + .05) * 1.12), d2 = Math.hypot((nx - .43) * 1.05, (ny - .1) * 1.06);
    let land = wave > -.2;
    if (name === 'Island Chain') land = wave > .18;
    if (name === 'Pangaea') land = (nx * nx * .86 + ny * ny * 1.2 < .82) && wave > -.95;
    if (name === 'Highlands') land = wave > -.32;
    if (name === 'Twin Worlds') land = (d1 < .73 || d2 < .72) && wave > -.48;
    if (name === 'Inland Sea') land = wave > -.18 && !(Math.pow(nx / .42, 2) + Math.pow(ny / .34, 2) < 1);
    if (name === 'Atolls') {
      const ring1 = Math.abs(Math.hypot(nx + .42, ny + .08) - .34) < .15;
      const ring2 = Math.abs(Math.hypot(nx - .4, ny - .13) - .3) < .14;
      land = ring1 || ring2 || (wave > .45 && Math.abs(nx) < .8);
    }
    if (name === 'Supercontinent') land = Math.abs(nx) < .91 && Math.abs(ny) < .82 && wave > -.78;
    if (name === 'Fractal Wilds') land = wave > -.05;
    if (name === 'Earthlike') land = (wave > -.08 && (nx < .28 || ny > -.38)) || (d1 < .62 && wave > -.55);
    const edge = x < 2 || y < 2 || x >= WIDTH - 2 || y >= HEIGHT - 2;
    if (edge) land = false;
    if (name === 'Pangaea' && Math.abs(nx + .16) < .035 && Math.abs(ny - .02) < .16) land = false;
    if (name === 'Inland Sea' && Math.abs(nx) < .42 && Math.abs(ny) < .34) land = false;
    const mountainChance = name === 'Highlands' ? .77 : .955;
    const terrain: Tile['terrain'] = !land ? 'water' : n > mountainChance ? 'mountain' : n > .66 ? 'forest' : 'plain';
    return { owner: null, terrain, troops: 0, buildings: [] };
  });
}
function neighbors(i: number) {
  const x = i % WIDTH, y = Math.floor(i / WIDTH);
  return [[x-1,y],[x+1,y],[x,y-1],[x,y+1]]
    .filter(([a,b]) => a! >= 0 && a! < WIDTH && b! >= 0 && b! < HEIGHT)
    .map(([a,b]) => index(a!,b!));
}
function spawnSites() {
  const sites: number[] = [];
  const cols = 10, rows = 5;
  for (let row = 0; row < rows; row++) for (let col = 0; col < cols; col++) {
    const x = Math.round(12 + col * (WIDTH - 24) / (cols - 1));
    const y = Math.round(10 + row * (HEIGHT - 20) / (rows - 1));
    sites.push(index(x,y));
  }
  return sites;
}
function preferencePoint(pref: string): [number,number] {
  const points: Record<string,[number,number]> = {
    northwest:[.16,.16], north:[.5,.12], northeast:[.84,.16],
    west:[.12,.5], center:[.5,.5], east:[.88,.5],
    southwest:[.16,.84], south:[.5,.88], southeast:[.84,.84]
  };
  return points[pref] || [.5,.5];
}
function spawn(player: Player, used: Set<number>) {
  const sites = spawnSites().filter(site => !used.has(site));
  const [px,py] = preferencePoint(player.spawnPreference);
  sites.sort((a,b) => {
    const ax = (a % WIDTH) / WIDTH, ay = Math.floor(a / WIDTH) / HEIGHT;
    const bx = (b % WIDTH) / WIDTH, by = Math.floor(b / WIDTH) / HEIGHT;
    return (ax-px)**2 + (ay-py)**2 - ((bx-px)**2 + (by-py)**2);
  });
  let chosen = sites[0] ?? index(10,10);
  const sx = chosen % WIDTH, sy = Math.floor(chosen / WIDTH);
  for (let dy = -3; dy <= 3; dy++) for (let dx = -3; dx <= 3; dx++) {
    const x = sx + dx, y = sy + dy;
    if (x < 2 || y < 2 || x >= WIDTH - 2 || y >= HEIGHT - 2) continue;
    const tile = tiles[index(x,y)]!;
    if (tile.terrain === 'water' || tile.terrain === 'mountain') tile.terrain = 'plain';
  }
  used.add(chosen);
  player.home = chosen;
  tiles[chosen] = { owner: player.id, terrain: 'plain', troops: 36, buildings: [] };
}
function freshWorld(randomMap = true) {
  mapName = randomMap ? MAPS[Math.floor(Math.random() * MAPS.length)]! : mapName;
  tiles = makeMap(mapName, Math.random() * 1000);
  tick = 0;
  const connected = [...players.values()].filter(p => p.connected).slice(0, MAX_PLAYERS);
  players.clear();
  connected.forEach((p,slot) => {
    p.gold = 160; p.score = 0; p.home = -1; p.color = colorFor(slot);
    players.set(p.id,p);
  });
  const used = new Set<number>();
  for (const p of players.values()) spawn(p,used);
  roundPlayerCount = players.size;
  finishing = false;
}
function state(): GameState {
  return { width: WIDTH, height: HEIGHT, tiles, players: [...players.values()].map(p => ({...p})), tick, round, mapName, capacity: MAX_PLAYERS };
}
let saveQueue = Promise.resolve();
function persistSnapshot(): Promise<void> {
  if (!supabase) return Promise.resolve();
  const snapshot = { id:'main', state:state(), updated_at:new Date().toISOString() };
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
    if (!saved || saved.width !== WIDTH || saved.height !== HEIGHT || !Array.isArray(saved.tiles) || saved.tiles.length !== WIDTH * HEIGHT || !Array.isArray(saved.players)) return;
    tiles = saved.tiles.map(t => ({ ...t, buildings: (Array.isArray(t.buildings) ? t.buildings : []).filter(b => ['city','factory','port','defense_post','market','farm'].includes(b.type)) }));
    tick = Number.isFinite(saved.tick) ? saved.tick : 0;
    round = Math.max(1,Number(saved.round) || 1);
    mapName = MAPS.includes(saved.mapName) ? saved.mapName : 'Continents';
    players.clear();
    for (const p of saved.players) if (p && typeof p.id === 'string' && typeof p.name === 'string') players.set(p.id,{...p,connected:false,spawnPreference:p.spawnPreference || 'auto'});
    roundPlayerCount = players.size;
    console.log('Restored saved frontier match.');
  } catch (error) { console.error('Could not restore saved frontier match:', error); }
}
function send(ws: WebSocket, packet: unknown) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(packet)); }
function broadcast() {
  const packet = JSON.stringify({ type: 'state', state: state() });
  for (const ws of clients.keys()) if (ws.readyState === WebSocket.OPEN) ws.send(packet);
}
function notify(text: string) {
  const packet = JSON.stringify({ type: 'notice', text });
  for (const ws of clients.keys()) send(ws, packet);
}
async function verify(token?: string) {
  if (!token || !supabase) return null;
  const { data, error } = await supabase.auth.getUser(token);
  return error ? null : data.user;
}
function join(client: Client, requestedName?: string, requestedPreference?: string) {
  if (client.joined) return;
  let player = players.get(client.id);
  if (!player && players.size >= MAX_PLAYERS) {
    send(client.socket,{type:'error',text:'This live match is full (50/50). Try the next round.'});
    return;
  }
  const preference = ['northwest','north','northeast','west','center','east','southwest','south','southeast'].includes(requestedPreference || '') ? requestedPreference! : 'auto';
  if (!player) {
    const name = (requestedName || 'Commander').replace(/[^a-zA-Z0-9 _-]/g, '').trim().slice(0, 18) || 'Commander';
    player = { id: client.id, name, color: colorFor(players.size), gold: 160, score: 0, connected: true, home: -1, spawnPreference: preference };
    players.set(client.id, player);
    roundPlayerCount = Math.max(roundPlayerCount, players.size);
    const used = new Set([...players.values()].filter(p=>p.id!==player!.id).map(p=>p.home).filter(h=>h>=0));
    spawn(player,used);
    notify('✦ ' + name + ' joined the live match (' + players.size + '/' + MAX_PLAYERS + ').');
  } else {
    player.connected = true;
    const updatedName = (requestedName || '').replace(/[^a-zA-Z0-9 _-]/g,'').trim().slice(0,18);
    if (updatedName) player.name = updatedName;
  }
  client.joined = true;
  send(client.socket, { type: 'welcome', id: client.id });
  broadcast();
  void persistSnapshot();
}
function linePath(from: number, to: number): number[] {
  let x0 = from % WIDTH, y0 = Math.floor(from / WIDTH);
  const x1 = to % WIDTH, y1 = Math.floor(to / WIDTH);
  const dx = Math.abs(x1-x0), sx = x0 < x1 ? 1 : -1;
  const dy = -Math.abs(y1-y0), sy = y0 < y1 ? 1 : -1;
  let err = dx + dy, steps = 0;
  const path: number[] = [];
  while (steps < 16) {
    path.push(index(x0,y0));
    if (x0 === x1 && y0 === y1) break;
    const e2 = 2 * err;
    if (e2 >= dy) { err += dy; x0 += sx; }
    if (e2 <= dx) { err += dx; y0 += sy; }
    steps++;
    if (x0 < 0 || x0 >= WIDTH || y0 < 0 || y0 >= HEIGHT) break;
  }
  return path.slice(1);
}
function claim(client: Client, player: Player, message: any) {
  const targetIndex = message.tile as number;
  if (!Number.isInteger(targetIndex) || targetIndex < 0 || targetIndex >= tiles.length) return;
  const target = tiles[targetIndex]!;
  if (target.owner === player.id) { send(client.socket,{type:'error',text:'That territory is already yours.'}); return; }
  if (target.terrain === 'mountain' || target.terrain === 'water') { send(client.socket,{type:'error',text:'Choose a land region.'}); return; }
  let sourceIndex = Number.isInteger(message.source) ? Number(message.source) : -1;
  if (sourceIndex < 0 || sourceIndex >= tiles.length || tiles[sourceIndex]!.owner !== player.id) {
    let best = Infinity;
    sourceIndex = -1;
    for (let i=0;i<tiles.length;i++) if (tiles[i]!.owner === player.id) {
      const d = Math.hypot((i%WIDTH)-(targetIndex%WIDTH),Math.floor(i/WIDTH)-Math.floor(targetIndex/WIDTH));
      if (d < best) { best=d; sourceIndex=i; }
    }
  }
  if (sourceIndex < 0) { send(client.socket,{type:'error',text:'Select one of your territories first.'}); return; }
  const distance = Math.hypot((sourceIndex%WIDTH)-(targetIndex%WIDTH),Math.floor(sourceIndex/WIDTH)-Math.floor(targetIndex/WIDTH));
  if (distance > 15) { send(client.socket,{type:'error',text:'That frontier is too far away. Select a closer launch territory.'}); return; }
  const source = tiles[sourceIndex]!;
  const percent = Math.max(1,Math.min(100,Math.floor(Number(message.percent)||50)));
  const sent = Math.floor(source.troops * percent / 100);
  if (sent < 1) { send(client.socket,{type:'error',text:'Not enough troops in that territory.'}); return; }
  const route = linePath(sourceIndex,targetIndex);
  source.troops -= sent;
  let remaining = sent, captured = 0, lastCaptured = -1;
  for (const step of route) {
    const tile = tiles[step]!;
    if (tile.terrain === 'water' || tile.terrain === 'mountain') break;
    if (tile.owner === player.id) continue;
    const defender = Math.ceil(tile.troops * ((tile.buildings || []).some(b=>b.type==='defense_post') ? 2 : 1));
    if (remaining <= defender) break;
    if (tile.owner) {
      const previous = players.get(tile.owner);
      if (previous) previous.score = Math.max(0,previous.score-1);
    }
    remaining -= defender + 1;
    tile.owner = player.id;
    tile.troops = 1;
    player.score++;
    captured++;
    lastCaptured = step;
  }
  if (!captured) {
    source.troops += sent;
    send(client.socket,{type:'notice',text:'The expansion front was stopped. Send more troops or choose another direction.'});
    return;
  }
  if (lastCaptured >= 0) tiles[lastCaptured]!.troops += remaining;
  notify('✦ ' + player.name + ' expanded the frontier by ' + captured + ' tile' + (captured===1?'':'s') + '.');
  broadcast();
  void persistSnapshot();
  checkVictory();
}
function checkVictory() {
  if (finishing || players.size < 1) return;
  const playable = tiles.filter(t=>t.terrain!=='water' && t.terrain!=='mountain').length;
  if (!playable) return;
  const land = new Map<string,number>();
  for (const p of players.values()) land.set(p.id,0);
  for (const t of tiles) if (t.owner && land.has(t.owner)) land.set(t.owner,(land.get(t.owner)||0)+1);
  const winner = [...players.values()].find(p => (land.get(p.id)||0)/playable >= WIN_LAND);
  const alive = [...players.values()].filter(p => (land.get(p.id)||0)>0);
  const lastStanding = roundPlayerCount >= 2 && alive.length === 1 ? alive[0] : undefined;
  const champion = winner || lastStanding;
  if (champion) {
    finishing = true;
    const reason = winner ? 'controlled 80% of the playable world' : 'is the last commander standing';
    notify('🏆 ' + champion.name + ' won Round ' + round + ' — ' + reason + '. A new random map is loading.');
    void persistSnapshot();
    nextMatchTimer = setTimeout(() => {
      round++;
      freshWorld(true);
      broadcast();
      notify('🌌 ROUND ' + round + ' — ' + mapName + ' is live. ' + players.size + '/' + MAX_PLAYERS + ' commanders.');
      void persistSnapshot();
    },7000);
  }
}
function command(client: Client, raw: string) {
  if (raw.length > 2048) return;
  let message: any; try { message = JSON.parse(raw); } catch { return; }
  if (!message || typeof message.type !== 'string') return;
  if (message.type === 'join') { join(client, typeof message.name === 'string' ? message.name : undefined, typeof message.spawnPreference === 'string' ? message.spawnPreference : undefined); return; }
  if (!client.joined) return;
  const player = players.get(client.id); if (!player) return;
  const now = Date.now(); if (now - client.lastAction < 180) return; client.lastAction = now;
  if (message.type === 'recruit') {
    if (player.gold < 25) { send(client.socket,{type:'error',text:'You need 25 gold to recruit.'}); return; }
    const homeTile = tiles[player.home];
    const target = homeTile?.owner === player.id ? player.home : tiles.findIndex(t=>t.owner===player.id);
    if (target === undefined || target < 0) return;
    player.gold -= 25; tiles[target]!.troops += 15; notify('＋ ' + player.name + ' recruited 15 troops.'); broadcast(); void persistSnapshot(); return;
  }
  if (message.type === 'chat' && typeof message.text === 'string') {
    const chatText = message.text.split('').map((ch: string) => { const code=ch.charCodeAt(0); return code<32 || code===127 ? ' ' : ch; }).join('').trim().slice(0,80);
    if (chatText) notify(player.name + ': ' + chatText);
    return;
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
    if (currentLevel >= rule.max) { send(client.socket,{type:'error',text:'That structure is already at its maximum level here.'}); return; }
    if (building === 'port' && !neighbors(targetIndex).some(n => tiles[n]!.terrain === 'water')) {
      send(client.socket,{type:'error',text:'Ports must be built on a coastline.'}); return;
    }
    const cost = rule.cost * (building === 'city' ? currentLevel + 1 : 1);
    if (player.gold < cost) { send(client.socket,{type:'error',text:'Not enough gold. This build costs ' + cost + '.'}); return; }
    player.gold -= cost;
    if (building === 'city' && existing.length) existing[0]!.level = currentLevel + 1;
    else tile.buildings.push({type:building,level:1});
    notify('🏗 ' + player.name + ' constructed ' + building.replace('_',' ') + '.');
    broadcast(); void persistSnapshot(); return;
  }
  if (message.type === 'claim') { claim(client,player,message); return; }
  if (message.type === 'ping') send(client.socket,{type:'pong',time:Date.now()});
}
app.get('/', (_req,res) => res.send('Pixel Frontier live match server'));
app.get('/health', (_req,res) => res.json({
  ok:true, service:'frontier-server', players:players.size, capacity:MAX_PLAYERS,
  round, map:mapName, uptime:Math.floor(process.uptime())
}));
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
        if (!stillConnected) {
          const player = players.get(id);
          if (player) {
            players.delete(id);
            for (const tile of tiles) if (tile.owner === id) { tile.owner = null; tile.troops = 0; tile.buildings = []; }
            if (player.connected) notify('↘ ' + player.name + ' left the frontier.');
            checkVictory(); broadcast(); void persistSnapshot();
          }
        }
      });
      ws.on('error',() => clients.delete(ws));
    });
  } catch { socket.destroy(); }
});
async function boot() {
  await restoreSnapshot();
  if (!tiles.length) freshWorld(true);
  else {
    for (const player of players.values()) { player.connected = false; player.spawnPreference = player.spawnPreference || 'auto'; }
    roundPlayerCount = players.size;
  }
  setInterval(() => {
    tick++;
    for (const player of players.values()) {
      const land = tiles.filter(t=>t.owner===player.id);
      const buildingIncome = land.reduce((sum,t) => sum + (t.buildings || []).reduce((n,b) => n + (b.type === 'factory' ? 3 : b.type === 'city' ? b.level * 2 : b.type === 'port' ? 2 : b.type === 'market' ? 4 : 0),0),0);
      player.gold = Math.min(9999, player.gold + Math.max(1,Math.floor(land.length/3)) + buildingIncome);
      for (const tile of land) {
        const city = (tile.buildings || []).find(b => b.type === 'city');
        const cap = city ? 99 + city.level * 50 : 99;
        const farms = (tile.buildings || []).filter(b => b.type === 'farm').length;
        if (tile.troops < cap && tick % 3 === 0) tile.troops = Math.min(cap,tile.troops + 1 + farms);
      }
    }
    checkVictory();
    broadcast();
    if (tick % 3 === 0) void persistSnapshot();
  }, TICK_MS);
  http.listen(port,'0.0.0.0',()=>console.log('Frontier live-match server listening on '+port));
}
process.on('SIGTERM', async () => { if(nextMatchTimer)clearTimeout(nextMatchTimer);await persistSnapshot();http.close(() => process.exit(0)); });
void boot();
