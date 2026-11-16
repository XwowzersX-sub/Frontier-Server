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

const WIDTH = 1000, HEIGHT = 1000, TICK_MS = 4500, MAX_PLAYERS = 50, WIN_LAND = 0.8, CHUNK_SIZE = 64;
const MAPS = ['Continents','Island Chain','Pangaea','Highlands','Twin Worlds','Inland Sea','Atolls','Supercontinent','Fractal Wilds','Earthlike'];
type BuildingType = 'city' | 'factory' | 'port' | 'defense_post' | 'market' | 'farm';
type Building = { type: BuildingType; level: number };
type Tile = { owner: string | null; terrain: 'plain' | 'forest' | 'mountain' | 'water'; troops: number; buildings: Building[] };
type Player = { id: string; name: string; color: string; gold: number; score: number; connected: boolean; home: number; spawnPreference: string; workerRatio?: number };
type Client = { id: string; socket: WebSocket; joined: boolean; lastAction: number; loadedChunks: Set<string> };
type Ship = { id: string; owner: string; from: number; to: number; path: number[]; step: number; kind: 'colony'|'trade'; destinationOwner?: string };
type GameState = { width: number; height: number; players: Player[]; tick: number; round: number; mapName: string; mapSeed: number; playable: number; capacity: number; ships: Array<{id:string;owner:string;x:number;y:number;kind:string}> };
const players = new Map<string, Player>();
const clients = new Map<WebSocket, Client>();
let tick=0,round=1,mapName='Continents',mapSeed=1,roundPlayerCount=0,finishing=false,playableTiles=0;
let tiles:Tile[]=[],ships:Ship[]=[];
const dirtyTiles=new Set<number>(),persistentTiles=new Set<number>();
let nextMatchTimer:ReturnType<typeof setTimeout>|null=null;
function markTile(i:number){if(i>=0&&i<tiles.length){dirtyTiles.add(i);persistentTiles.add(i);}}
function chunkKey(cx:number,cy:number){return cx+':'+cy;}
function chunkKeyForTile(i:number){return chunkKey(Math.floor((i%WIDTH)/CHUNK_SIZE),Math.floor(Math.floor(i/WIDTH)/CHUNK_SIZE));}

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
  player.home=chosen;
  tiles[chosen]={owner:player.id,terrain:'plain',troops:36,buildings:[]};
  for(let dy=-3;dy<=3;dy++)for(let dx=-3;dx<=3;dx++){const x=sx+dx,y=sy+dy;if(x>=0&&x<WIDTH&&y>=0&&y<HEIGHT)markTile(index(x,y));}
}
function freshWorld(randomMap = true) {
  mapName=randomMap?MAPS[Math.floor(Math.random()*MAPS.length)]!:mapName;
  mapSeed=Math.floor(Math.random()*1_000_000_000);
  tiles=makeMap(mapName,mapSeed);
  playableTiles=tiles.reduce((sum,t)=>sum+(t.terrain==='water'||t.terrain==='mountain'?0:1),0);
  dirtyTiles.clear();persistentTiles.clear();ships=[];
  tick=0;
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
  return {width:WIDTH,height:HEIGHT,players:[...players.values()].map(p=>({...p})),tick,round,mapName,mapSeed,playable:playableTiles,capacity:MAX_PLAYERS,ships:ships.map(s=>{const i=s.path[Math.min(s.step,s.path.length-1)]??s.from;return{id:s.id,owner:s.owner,x:i%WIDTH,y:Math.floor(i/WIDTH),kind:s.kind};})};
}
let saveQueue = Promise.resolve();
function persistSnapshot(): Promise<void> {
  if (!supabase) return Promise.resolve();
  const compactState={width:WIDTH,height:HEIGHT,tick,round,mapName,mapSeed,players:[...players.values()].map(p=>({...p,connected:false})),changes:[...persistentTiles].map(i=>[i,tiles[i]])};
  const snapshot={id:'main',state:compactState,updated_at:new Date().toISOString()};
  saveQueue = saveQueue.then(async () => {
    const { error } = await supabase!.from('game_snapshots').upsert(snapshot);
    if (error) console.error('Could not save game snapshot:', error.message);
  }).catch(error => console.error('Snapshot save failed:', error));
  return saveQueue;
}
async function restoreSnapshot(){
 if(!supabase){console.warn('Supabase server key not configured; match state will be temporary.');return;}
 try{
  const {data,error}=await supabase.from('game_snapshots').select('state').eq('id','main').maybeSingle();if(error)throw error;
  const saved=data?.state as any;
  if(!saved||saved.width!==WIDTH||saved.height!==HEIGHT||!Array.isArray(saved.players)||!Array.isArray(saved.changes)||!Number.isFinite(saved.mapSeed))return;
  tick=Number.isFinite(saved.tick)?saved.tick:0;round=Math.max(1,Number(saved.round)||1);mapName=MAPS.includes(saved.mapName)?saved.mapName:'Continents';mapSeed=Number(saved.mapSeed);
  tiles=makeMap(mapName,mapSeed);playableTiles=tiles.reduce((sum,t)=>sum+(t.terrain==='water'||t.terrain==='mountain'?0:1),0);persistentTiles.clear();dirtyTiles.clear();
  for(const entry of saved.changes){if(!Array.isArray(entry)||!Number.isInteger(entry[0])||entry[0]<0||entry[0]>=tiles.length||!entry[1])continue;const t=entry[1] as Tile;if(!['plain','forest','mountain','water'].includes(t.terrain)||(t.owner!==null&&typeof t.owner!=='string'))continue;tiles[entry[0]]={owner:t.owner,terrain:t.terrain,troops:Math.max(0,Number(t.troops)||0),buildings:Array.isArray(t.buildings)?t.buildings.filter(b=>['city','factory','port','defense_post','market','farm'].includes(b.type)):[]};persistentTiles.add(entry[0]);}
  players.clear();for(const p of saved.players)if(p&&typeof p.id==='string'&&typeof p.name==='string')players.set(p.id,{...p,connected:false,spawnPreference:p.spawnPreference||'auto',workerRatio:Number.isFinite(p.workerRatio)?p.workerRatio:30});
  roundPlayerCount=players.size;ships=[];console.log('Restored compact 1000x1000 frontier snapshot.');
 }catch(error){console.error('Could not restore saved frontier snapshot:',error);}
}
function send(ws: WebSocket, packet: unknown) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(packet)); }
function sendChunk(client:Client,cx:number,cy:number){
 const startX=cx*CHUNK_SIZE,startY=cy*CHUNK_SIZE;if(startX<0||startY<0||startX>=WIDTH||startY>=HEIGHT)return;
 const width=Math.min(CHUNK_SIZE,WIDTH-startX),height=Math.min(CHUNK_SIZE,HEIGHT-startY),items:Array<{i:number;tile:Tile}>=[];
 for(let y=0;y<height;y++)for(let x=0;x<width;x++){const i=index(startX+x,startY+y);items.push({i,tile:tiles[i]!});}
 client.loadedChunks.add(chunkKey(cx,cy));send(client.socket,{type:'chunk',cx,cy,items});
}
function broadcast(){
 const base=state();
 for(const client of clients.values()){
  if(!client.joined||client.socket.readyState!==WebSocket.OPEN)continue;
  const updates:Array<[number,Tile]>=[];
  for(const i of dirtyTiles)if(client.loadedChunks.has(chunkKeyForTile(i)))updates.push([i,tiles[i]!]);
  send(client.socket,{type:'state',state:{...base,updates}});
 }
 dirtyTiles.clear();
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
    player = { id: client.id, name, color: colorFor(players.size), gold: 160, score: 0, connected: true, home: -1, spawnPreference: preference, workerRatio:30 };
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
function seaRoute(from:number,to:number):number[]{
 const starts=neighbors(from).filter(i=>tiles[i]!.terrain==='water');
 const goals=new Set(neighbors(to).filter(i=>tiles[i]!.terrain==='water'));
 if(!starts.length||!goals.size)return [];
 const previous=new Int32Array(tiles.length);previous.fill(-2);
 const queue=new Int32Array(tiles.length);let head=0,tail=0,goal=-1;
 for(const i of starts){previous[i]=-1;queue[tail++]=i;}
 while(head<tail&&tail<tiles.length){
  const current=queue[head++];if(goals.has(current)){goal=current;break;}
  for(const next of neighbors(current)){
   if(previous[next]!==-2||tiles[next]!.terrain!=='water')continue;
   previous[next]=current;queue[tail++]=next;
  }
 }
 if(goal<0)return [];
 const waterPath:number[]=[];let at=goal;
 while(at>=0){waterPath.push(at);at=previous[at]!;}
 waterPath.reverse();return [from,...waterPath,to];
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
function neighbors8(i: number) {
  const x = i % WIDTH, y = Math.floor(i / WIDTH), out: number[] = [];
  for (let oy=-1; oy<=1; oy++) for (let ox=-1; ox<=1; ox++) {
    if (!ox && !oy) continue;
    const nx=x+ox, ny=y+oy;
    if (nx>=0 && nx<WIDTH && ny>=0 && ny<HEIGHT) out.push(index(nx,ny));
  }
  return out;
}
function claim(client: Client, player: Player, message: any) {
  const targetIndex = message.tile as number;
  if (!Number.isInteger(targetIndex) || targetIndex < 0 || targetIndex >= tiles.length) return;
  const target = tiles[targetIndex]!;
  if (target.owner === player.id) { send(client.socket,{type:'error',text:'That territory is already yours.'}); return; }
  if (target.terrain === 'mountain' || target.terrain === 'water') { send(client.socket,{type:'error',text:'Choose a land region.'}); return; }
  const sourceIndex = Number.isInteger(message.source) ? Number(message.source) : -1;
  if (sourceIndex < 0 || sourceIndex >= tiles.length || tiles[sourceIndex]!.owner !== player.id) {
    send(client.socket,{type:'error',text:'Select one of your own launch territories first.'}); return;
  }
  const sx=sourceIndex%WIDTH,sy=Math.floor(sourceIndex/WIDTH),tx=targetIndex%WIDTH,ty=Math.floor(targetIndex/WIDTH);
  const dx=tx-sx,dy=ty-sy,distance=Math.hypot(dx,dy);
  if (distance > 15) { send(client.socket,{type:'error',text:'That frontier is too far away. Select a destination within 15 tiles.'}); return; }
  const dirX=dx/(distance||1),dirY=dy/(distance||1);
  const percent = Math.max(1,Math.min(100,Math.floor(Number(message.percent)||50)));
  const source = tiles[sourceIndex]!;
  const sent = Math.floor(source.troops * percent / 100);
  if (sent < 1) { send(client.socket,{type:'error',text:'Not enough troops in that territory.'}); return; }
  source.troops -= sent;
  const maxRadius=Math.min(15,Math.max(3,Math.floor(3+Math.sqrt(sent)*.34)));
  const queue:{index:number;troops:number;depth:number}[]=[{index:sourceIndex,troops:sent,depth:0}];
  const visited=new Set<number>([sourceIndex]);
  let captured=0,processed=0,remainingArmy=0;
  while(queue.length && processed<220) {
    const wave=queue.shift()!; processed++;
    if(wave.depth>=maxRadius || wave.troops<3) continue;
    const candidates=neighbors8(wave.index).filter(next=>{
      if(visited.has(next)) return false;
      const tile=tiles[next]!;
      if(tile.terrain==='water'||tile.terrain==='mountain'||tile.owner===player.id) return false;
      const rx=(next%WIDTH)-sx,ry=Math.floor(next/WIDTH)-sy,radial=Math.hypot(rx,ry);
      if(radial>maxRadius||radial<.1) return false;
      const forward=(rx*dirX+ry*dirY)/(radial||1);
      return forward>-.08;
    }).map(next=>{
      const rx=(next%WIDTH)-sx,ry=Math.floor(next/WIDTH)-sy,radial=Math.hypot(rx,ry);
      const forward=(rx*dirX+ry*dirY)/(radial||1),lateral=Math.abs(rx*dirY-ry*dirX)/(radial||1);
      const tile=tiles[next]!;
      return {next,score:forward*1.7-lateral*.48+(tile.owner?-.08:0)+Math.random()*.23};
    }).sort((a,b)=>b.score-a.score);
    if(!candidates.length){remainingArmy+=wave.troops;continue;}
    const branchCount=Math.min(wave.depth<2?3:2,candidates.length,Math.max(1,Math.floor(wave.troops/4)));
    const chosen=candidates.slice(0,branchCount),share=Math.floor(wave.troops/branchCount);
    for(const option of chosen) {
      const next=option.next; visited.add(next);
      const tile=tiles[next]!;
      const defender=Math.ceil(tile.troops*((tile.buildings||[]).some(b=>b.type==='defense_post')?2:1));
      const attrition=Math.max(1,Math.ceil(share*(.025+wave.depth*.004)));
      const effective=share-attrition;
      if(effective<=defender){tile.troops=Math.max(0,defender-effective);continue;}
      if(tile.owner){const previous=players.get(tile.owner);if(previous)previous.score=Math.max(0,previous.score-1);}
      const survivors=effective-defender-1;
      tile.owner=player.id;tile.troops=survivors;player.score++;captured++;markTile(next);
      if(survivors>=3)queue.push({index:next,troops:survivors,depth:wave.depth+1});
      else remainingArmy+=survivors;
    }
  }
  if(!captured) {
    markTile(sourceIndex);
    notify('🛡 ' + player.name + '\'s expansion wave ran out of strength before capturing land.');
    broadcast(); void persistSnapshot(); return;
  }
  markTile(sourceIndex);
  notify('⚔ ' + player.name + ' spread an army outward and captured ' + captured + ' territory tiles. Troops diminished along the way.');
  broadcast();
  void persistSnapshot();
  checkVictory();
}
function checkVictory() {
  if (finishing || players.size < 1 || !playableTiles) return;
  const playable = playableTiles;
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
  if(message.type==='chunks'&&Array.isArray(message.chunks)){for(const item of message.chunks.slice(0,36))if(item&&Number.isInteger(item.x)&&Number.isInteger(item.y))sendChunk(client,item.x,item.y);return;}
  if(message.type==='workers'){player.workerRatio=Math.max(0,Math.min(100,Math.floor(Number(message.ratio)||0)));broadcast();return;}
  const now = Date.now(); if (now - client.lastAction < 180) return; client.lastAction = now;
  if (message.type === 'recruit') {
    if (player.gold < 25) { send(client.socket,{type:'error',text:'You need 25 gold to recruit.'}); return; }
    const homeTile = tiles[player.home];
    const target = homeTile?.owner === player.id ? player.home : tiles.findIndex(t=>t.owner===player.id);
    if (target === undefined || target < 0) return;
    player.gold -= 25; tiles[target]!.troops += 15; markTile(target); notify('＋ ' + player.name + ' recruited 15 troops.'); broadcast(); void persistSnapshot(); return;
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
    markTile(targetIndex);
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
      const client: Client = { id, socket:ws, joined:false, lastAction:0, loadedChunks:new Set<string>() }; clients.set(ws,client);
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
async function boot(){
 await restoreSnapshot();
 if(!tiles.length)freshWorld(true);
 else{for(const player of players.values()){player.connected=false;player.spawnPreference=player.spawnPreference||'auto';player.workerRatio=Number.isFinite(player.workerRatio)?player.workerRatio:30;}roundPlayerCount=players.size;}
 setInterval(()=>{
  tick++;
  const summary=new Map<string,{land:number;troops:number;cityLevels:number;income:number;home:number}>();
  for(const player of players.values())summary.set(player.id,{land:0,troops:0,cityLevels:0,income:0,home:-1});
  for(let i=0;i<tiles.length;i++){
   const tile=tiles[i]!,stats=tile.owner?summary.get(tile.owner):undefined;if(!stats)continue;
   stats.land++;stats.troops+=tile.troops;if(stats.home<0)stats.home=i;
   for(const building of tile.buildings||[]){
    if(building.type==='city'){stats.cityLevels+=building.level;stats.income+=building.level*2;}
    if(building.type==='factory')stats.income+=3;
    if(building.type==='port')stats.income+=2;
    if(building.type==='market')stats.income+=4;
   }
  }
  for(const player of players.values()){
   const stats=summary.get(player.id)!;
   const workers=Math.max(0,Math.min(100,Number(player.workerRatio??30)));
   const maxTroops=Math.floor(2*(Math.pow(Math.max(1,stats.land),.6)*15+50)+stats.cityLevels*180);
   player.gold=Math.min(999999,player.gold+Math.max(1,Math.floor(stats.land*(.2+workers*.035)))+stats.income);
   const home=player.home>=0&&tiles[player.home]?.owner===player.id?player.home:stats.home;
   if(home>=0){
    const growth=(2+Math.sqrt(stats.troops)*.08)*(1-stats.troops/Math.max(1,maxTroops))*(1-workers/135);
    const add=Math.max(0,Math.floor(growth));
    if(add&&tick%2===0){tiles[home]!.troops+=add;markTile(home);}
   }
  }
  // Civilian ferries colonize unclaimed coast; trade ships connect ports and share gold.
  for(let i=ships.length-1;i>=0;i--){
   const ship=ships[i]!;ship.step=Math.min(ship.path.length-1,ship.step+8);
   if(ship.step>=ship.path.length-1){
    const target=tiles[ship.to]!;
    if(ship.kind==='colony'&&target.owner===null&&target.terrain!=='water'&&target.terrain!=='mountain'){
     target.owner=ship.owner;target.troops=1;markTile(ship.to);
     const owner=players.get(ship.owner);if(owner){owner.score++;notify('🏝 '+owner.name+' founded a new island settlement.');}
    }else if(ship.kind==='trade'){
     const from=players.get(ship.owner),to=ship.destinationOwner?players.get(ship.destinationOwner):from;
     if(from)from.gold=Math.min(999999,from.gold+40);
     if(to&&to.id!==from?.id)to.gold=Math.min(999999,to.gold+40);
    }
    ships.splice(i,1);
   }
  }
  if(tick%20===0&&ships.filter(s=>s.kind==='trade').length<20){
   const ports=tiles.map((t,i)=>({t,i})).filter(v=>(v.t.buildings||[]).some(b=>b.type==='port')&&v.t.owner);
   if(ports.length>=2){
    const a=ports[Math.floor(Math.random()*ports.length)]!;
    const b=ports.find(v=>v.i!==a.i&&v.t.owner!==a.t.owner)||ports.find(v=>v.i!==a.i);
    if(b){const path=seaRoute(a.i,b.i);if(path.length>1)ships.push({id:randomUUID(),owner:a.t.owner!,from:a.i,to:b.i,path,step:0,kind:'trade',destinationOwner:b.t.owner||undefined});}
   }
  }
  checkVictory();broadcast();if(tick%3===0)void persistSnapshot();
 },TICK_MS);
 http.listen(port,'0.0.0.0',()=>console.log('Frontier 1000x1000 chunked world server listening on '+port));
}
process.on('SIGTERM', async () => { if(nextMatchTimer)clearTimeout(nextMatchTimer);await persistSnapshot();http.close(() => process.exit(0)); });
void boot();
