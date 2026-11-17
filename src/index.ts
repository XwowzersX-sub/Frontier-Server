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
type BuildingType = 'city' | 'factory' | 'port' | 'defense_post' | 'market' | 'farm' | 'highway';
type Building = { type: BuildingType; level: number };
type Tile = { owner: string | null; terrain: 'plain' | 'forest' | 'mountain' | 'water'; troops: number; buildings: Building[] };
type Player = { id: string; name: string; color: string; gold: number; score: number; connected: boolean; home: number; spawnPreference: string; workerRatio?: number; territory?:number; troopsTotal?:number; troopMax?:number; troopRate?:number; goldRate?:number };
type Client = { id: string; socket: WebSocket; joined: boolean; lastAction: number; loadedChunks: Set<string> };
type StatIncrement = {gamesPlayed?:number;wins?:number;losses?:number;playTimeSeconds?:number;landClaimed?:number};
type Ship = { id: string; owner: string; from: number; to: number; path: number[]; step: number; kind: 'colony'|'trade'; destinationOwner?: string };
type MatchPhase='countdown'|'spawning'|'playing';
type GameState = { width: number; height: number; players: Player[]; tick: number; round: number; mapName: string; mapSeed: number; playable: number; capacity: number; ships: Array<{id:string;owner:string;x:number;y:number;kind:string}>; expansions?:Array<{owner:string;tile:number;troops:number}>; phase:MatchPhase;countdownEndsAt:number;spawnEndsAt:number;serverTime:number };
type ExpansionCandidate={index:number;edge:number;pressure:number;priority:number};
type ActiveExpansion={id:string;owner:string;targetOwner:string|null;sent:number;remaining:number;defenderTroops:number;frontWidth:number;launchIndex:number;candidates:ExpansionCandidate[];cursor:number;captured:number};
const players = new Map<string, Player>();
const clients = new Map<WebSocket, Client>();
let tick=0,round=1,mapName='Continents',mapSeed=1,roundPlayerCount=0,finishing=false,playableTiles=0;
let matchPhase:MatchPhase='countdown',countdownEndsAt=0,spawnEndsAt=0,restoredPhaseSnapshot=false;
let tiles:Tile[]=[],ships:Ship[]=[];
const activeExpansions:ActiveExpansion[]=[];
const dirtyTiles=new Set<number>(),persistentTiles=new Set<number>();
let nextMatchTimer:ReturnType<typeof setTimeout>|null=null;
const AUTH_USER_ID=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const roundParticipants=new Map<string,string>();
const pendingLandClaimed=new Map<string,number>();
const activePlaySessions=new Map<string,number>();
let statsFlushRunning=false;
function isAccountId(id:string){return AUTH_USER_ID.test(id);}
async function recordFrontierStats(userId:string,increment:StatIncrement):Promise<boolean>{
 if(!supabase||!isAccountId(userId))return false;
 const {error}=await supabase.rpc('record_frontier_stats',{
  p_user_id:userId,p_games_played:increment.gamesPlayed||0,p_wins:increment.wins||0,
  p_losses:increment.losses||0,p_play_time_seconds:increment.playTimeSeconds||0,
  p_land_claimed:increment.landClaimed||0
 });
 if(error){console.error('Leaderboard stat update failed:',error.message);return false;}
 return true;
}
function trackAccountJoin(userId:string,name:string){
 if(!isAccountId(userId))return;
 roundParticipants.set(userId,name);
 if(!activePlaySessions.has(userId))activePlaySessions.set(userId,Date.now());
 if(supabase){
  void (async()=>{
   const clean=name.replace(/[^a-zA-Z0-9 _-]/g,'').trim().slice(0,18)||'Commander';
   const first=await supabase!.from('profiles').upsert({id:userId,username:clean},{onConflict:'id'});
   if(first.error&&first.error.code==='23505'){
    const uniqueName=(clean.slice(0,14)+'-'+userId.slice(0,4)).slice(0,20);
    const retry=await supabase!.from('profiles').upsert({id:userId,username:uniqueName},{onConflict:'id'});
    if(retry.error)console.error('Could not update commander profile:',retry.error.message);
   }else if(first.error)console.error('Could not update commander profile:',first.error.message);
  })();
 }
}
function addLandClaimed(userId:string,count:number){
 if(!isAccountId(userId)||count<=0)return;
 pendingLandClaimed.set(userId,(pendingLandClaimed.get(userId)||0)+Math.floor(count));
}
async function flushPlayerPlayTime(userId:string){
 const last=activePlaySessions.get(userId);if(last===undefined)return;
 const seconds=Math.floor((Date.now()-last)/1000);
 if(seconds>0){
  const saved=await recordFrontierStats(userId,{playTimeSeconds:seconds});
  if(!saved)return;
  activePlaySessions.set(userId,last+seconds*1000);
 }
 const stillConnected=[...clients.values()].some(client=>client.id===userId&&client.joined);
 if(!stillConnected&&activePlaySessions.has(userId))activePlaySessions.delete(userId);
}
async function flushLeaderboardBuffers(){
 if(statsFlushRunning)return;statsFlushRunning=true;
 try{
  for(const userId of [...activePlaySessions.keys()])await flushPlayerPlayTime(userId);
  for(const [userId,count] of [...pendingLandClaimed.entries()]){
   if(await recordFrontierStats(userId,{landClaimed:count})){
    const remaining=(pendingLandClaimed.get(userId)||0)-count;
    if(remaining>0)pendingLandClaimed.set(userId,remaining);else pendingLandClaimed.delete(userId);
   }
  }
 }finally{statsFlushRunning=false;}
}
async function persistRoundResults(winnerId:string){
 const participants=[...roundParticipants.keys()];
 await flushLeaderboardBuffers();
 for(const userId of participants){
  await recordFrontierStats(userId,{gamesPlayed:1,wins:userId===winnerId?1:0,losses:userId===winnerId?0:1});
 }
}
function markTile(i:number){if(i>=0&&i<tiles.length){dirtyTiles.add(i);persistentTiles.add(i);}}
function chunkKey(cx:number,cy:number){return cx+':'+cy;}
function chunkKeyForTile(i:number){return chunkKey(Math.floor((i%WIDTH)/CHUNK_SIZE),Math.floor(Math.floor(i/WIDTH)/CHUNK_SIZE));}

function index(x: number, y: number) { return y * WIDTH + x; }
function colorFor(slot: number) { return 'hsl(' + ((slot * 137.508 + 195) % 360).toFixed(1) + ' 66% 58%)'; }
function latticeNoise(x:number,y:number,seed:number){
  let n=(Math.imul(x|0,374761393)^Math.imul(y|0,668265263)^Math.imul(seed|0,1442695041))|0;
  n=Math.imul(n^(n>>>13),1274126177);
  return((n^(n>>>16))>>>0)/4294967295;
}
function valueNoise(x:number,y:number,seed:number){
  const ix=Math.floor(x),iy=Math.floor(y),fx=x-ix,fy=y-iy;
  const sx=fx*fx*(3-2*fx),sy=fy*fy*(3-2*fy);
  const a=latticeNoise(ix,iy,seed),b=latticeNoise(ix+1,iy,seed),c=latticeNoise(ix,iy+1,seed),d=latticeNoise(ix+1,iy+1,seed);
  const top=a+(b-a)*sx,bottom=c+(d-c)*sx;
  return top+(bottom-top)*sy;
}
function terrainAt(x:number,y:number,name:string,seed:number):Tile['terrain']{
  const nx=(x-WIDTH/2)/(WIDTH/2),ny=(y-HEIGHT/2)/(HEIGHT/2);
  const warpX=(valueNoise(x*.004,y*.004,seed+811)-.5)*44;
  const warpY=(valueNoise(x*.004,y*.004,seed+977)-.5)*44;
  const px=x+warpX,py=y+warpY;
  const macro=valueNoise(px*.0032,py*.0032,seed+11)-.5;
  const regional=valueNoise(px*.012,py*.012,seed+73)-.5;
  const detail=valueNoise(px*.04,py*.04,seed+137)-.5;
  const field=macro*.62+regional*.28+detail*.10;
  const d1=Math.hypot((nx+.48)*.94,(ny+.05)*1.12),d2=Math.hypot((nx-.43)*1.05,(ny-.1)*1.06);
  const key=String(name||'Continents').toLowerCase().replace(/[^a-z]+/g,'-');
  let land=false;
  if(key==='islands'||key==='island-chain')land=field>.12;
  else if(key==='pangaea'){
    const radius=Math.sqrt(nx*nx*.86+ny*ny*1.2);
    land=field+(.88-radius)*.55>-.05;
    if(Math.abs(nx+.16)<.035&&Math.abs(ny-.02)<.16)land=false;
  }else if(key==='highlands')land=field>-.12;
  else if(key==='twin-worlds')land=field+(.9-Math.min(d1/.73,d2/.72))*.44>-.05;
  else if(key==='inland-sea'){
    const seaRadius=Math.sqrt(Math.pow(nx/.42,2)+Math.pow(ny/.34,2));
    land=field>-.03&&seaRadius>.92+detail*.24;
  }else if(key==='atolls'){
    const ring1=Math.abs(Math.hypot(nx+.42,ny+.08)-.34);
    const ring2=Math.abs(Math.hypot(nx-.4,ny-.13)-.3);
    const atoll=Math.max(1-ring1/.17,1-ring2/.16);
    land=atoll+field*.22>.12||(field>.3&&Math.abs(nx)<.8);
  }else if(key==='supercontinent'){
    const edge=Math.max(Math.abs(nx)/.91,Math.abs(ny)/.82);
    land=field+(.98-edge)*.55>-.2;
  }else if(key==='fractal'||key==='fractal-wilds')land=field>-.04;
  else if(key==='earthlike'){
    const continents=Math.max(field+(.78-d1)*.56,field+(.72-d2)*.48);
    land=continents>-.08&&(nx<.28||ny>-.38||d1<.62);
  }else land=field>-.02;
  if(x<2||y<2||x>=WIDTH-2||y>=HEIGHT-2)land=false;
  if(!land)return'water';
  const elevation=macro*.45+regional*.4+detail*.15;
  const moisture=valueNoise(px*.021,py*.021,seed+313)-.5;
  if(elevation>(key==='highlands'?.075:.205))return'mountain';
  return moisture>.035?'forest':'plain';
}
function makeMap(name:string,seed:number):Tile[]{
  return Array.from({length:WIDTH*HEIGHT},(_,i)=>({
    owner:null,
    terrain:terrainAt(i%WIDTH,Math.floor(i/WIDTH),name,seed),
    troops:0,
    buildings:[]
  }));
}
function neighbors(i: number) {
  const x = i % WIDTH, y = Math.floor(i / WIDTH);
  return [[x-1,y],[x+1,y],[x,y-1],[x,y+1]]
    .filter(([a,b]) => a! >= 0 && a! < WIDTH && b! >= 0 && b! < HEIGHT)
    .map(([a,b]) => index(a!,b!));
}
function getSpawnBlob(center:number):number[]|null {
 if(!Number.isInteger(center)||center<0||center>=tiles.length)return null;
 const sx=center%WIDTH,sy=Math.floor(center/WIDTH),blob:number[]=[];
 for(let y=Math.max(0,sy-4);y<=Math.min(HEIGHT-1,sy+4);y++)for(let x=Math.max(0,sx-4);x<=Math.min(WIDTH-1,sx+4);x++){
  const dx=x-(sx-.5),dy=y-(sy-.5);if(dx*dx+dy*dy>16)continue;
  const i=index(x,y),tile=tiles[i]!;
  if(tile.owner!==null||tile.terrain==='water')return null;
  blob.push(i);
 }
 return blob.length>=35?blob:null;
}
function spawn(player:Player,center?:number):boolean {
 const occupied=[...players.values()].filter(other=>other.id!==player.id&&other.home>=0).map(other=>other.home);
 let chosen=center;
 for(let attempt=0;attempt<(center===undefined?5000:1);attempt++){
  if(chosen===undefined)chosen=index(8+Math.floor(Math.random()*(WIDTH-16)),8+Math.floor(Math.random()*(HEIGHT-16)));
  const sx=chosen%WIDTH,sy=Math.floor(chosen/WIDTH);
  if(occupied.some(home=>Math.hypot(sx-home%WIDTH,sy-Math.floor(home/WIDTH))<22)){if(center!==undefined)return false;chosen=undefined;continue;}
  const blob=getSpawnBlob(chosen);
  if(!blob){if(center!==undefined)return false;chosen=undefined;continue;}
  player.home=chosen;player.territory=blob.length;player.troopsTotal=36;player.troopMax=Math.floor(2*(Math.pow(Math.max(1,blob.length),.6)+50));
  addLandClaimed(player.id,blob.length);
  for(const tileIndex of blob){const tile=tiles[tileIndex]!;tile.owner=player.id;tile.troops=tileIndex===chosen?36:0;tile.buildings=[];markTile(tileIndex);}
  return true;
 }
 return false;
}
function maybeFinishSpawnPhase(){
 if(matchPhase!=='spawning')return;
 const active=[...players.values()].filter(player=>player.connected);
 if(active.length&&active.every(player=>player.home>=0)){
  matchPhase='playing';spawnEndsAt=0;notify('🌎 All commanders are placed. The match has begun!');broadcast();void persistSnapshot();
 }
}
function freshWorld(randomMap = true) {
  mapName=randomMap?MAPS[Math.floor(Math.random()*MAPS.length)]!:mapName;
  mapSeed=Math.floor(Math.random()*1_000_000_000);
  tiles=makeMap(mapName,mapSeed);
  playableTiles=tiles.reduce((sum,t)=>sum+(t.terrain==='water'?0:1),0);
  dirtyTiles.clear();persistentTiles.clear();ships=[];activeExpansions.length=0;tick=0;
  const connected=[...players.values()].filter(p=>p.connected).slice(0,MAX_PLAYERS);
  players.clear();roundParticipants.clear();
  connected.forEach((p,slot)=>{
   p.gold=160;p.score=0;p.home=-1;p.color=colorFor(slot);p.territory=0;p.troopsTotal=0;p.troopMax=0;p.troopRate=0;p.goldRate=1;
   players.set(p.id,p);
   if(isAccountId(p.id))roundParticipants.set(p.id,p.name);
  });
  roundPlayerCount=players.size;finishing=false;
  matchPhase='countdown';countdownEndsAt=connected.length?Date.now()+60000:0;spawnEndsAt=0;
}
function state(): GameState {
  return {width:WIDTH,height:HEIGHT,players:[...players.values()].map(p=>({...p})),tick,round,mapName,mapSeed,playable:playableTiles,capacity:MAX_PLAYERS,ships:ships.map(s=>{const i=s.path[Math.min(s.step,s.path.length-1)]??s.from;return{id:s.id,owner:s.owner,x:i%WIDTH,y:Math.floor(i/WIDTH),kind:s.kind};}),expansions:activeExpansions.map(exp=>({owner:exp.owner,tile:exp.candidates[Math.min(exp.cursor,exp.candidates.length-1)]?.index??exp.launchIndex,troops:Math.max(0,Math.ceil(exp.remaining))})),phase:matchPhase,countdownEndsAt,spawnEndsAt,serverTime:Date.now()};
}
let saveQueue = Promise.resolve();
function persistSnapshot(): Promise<void> {
  if (!supabase) return Promise.resolve();
  const compactState={width:WIDTH,height:HEIGHT,tick,round,mapName,mapSeed,phase:matchPhase,countdownEndsAt,spawnEndsAt,players:[...players.values()].map(p=>({...p,connected:false})),changes:[...persistentTiles].map(i=>[i,tiles[i]]),expansions:activeExpansions};
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
  if(saved.phase==='countdown'||saved.phase==='spawning'||saved.phase==='playing'){restoredPhaseSnapshot=true;matchPhase=saved.phase;countdownEndsAt=Math.max(0,Number(saved.countdownEndsAt)||0);spawnEndsAt=Math.max(0,Number(saved.spawnEndsAt)||0);}
  tiles=makeMap(mapName,mapSeed);playableTiles=tiles.reduce((sum,t)=>sum+(t.terrain==='water'?0:1),0);persistentTiles.clear();dirtyTiles.clear();
  for(const entry of saved.changes){if(!Array.isArray(entry)||!Number.isInteger(entry[0])||entry[0]<0||entry[0]>=tiles.length||!entry[1])continue;const t=entry[1] as Tile;if(!['plain','forest','mountain','water'].includes(t.terrain)||(t.owner!==null&&typeof t.owner!=='string'))continue;tiles[entry[0]]={owner:t.owner,terrain:t.terrain,troops:Math.max(0,Number(t.troops)||0),buildings:Array.isArray(t.buildings)?t.buildings.filter(b=>['city','factory','port','defense_post','market','farm','highway'].includes(b.type)):[]};persistentTiles.add(entry[0]);}
  players.clear();for(const p of saved.players)if(p&&typeof p.id==='string'&&typeof p.name==='string')players.set(p.id,{...p,connected:false,spawnPreference:p.spawnPreference||'auto',workerRatio:Number.isFinite(p.workerRatio)?p.workerRatio:30});
  activeExpansions.length=0;
  if(Array.isArray(saved.expansions))for(const expansion of saved.expansions.slice(0,100)){
   if(!expansion||typeof expansion.owner!=='string'||!Array.isArray(expansion.candidates))continue;
   const candidates=expansion.candidates.filter((v:any)=>v&&Number.isInteger(v.index)&&v.index>=0&&v.index<tiles.length&&Number.isFinite(v.edge)&&Number.isFinite(v.pressure)).slice(0,20000);
   const cursor=Math.max(0,Math.min(candidates.length,Math.floor(Number(expansion.cursor)||0)));
   if(candidates.length>cursor)activeExpansions.push({id:String(expansion.id||randomUUID()),owner:expansion.owner,targetOwner:typeof expansion.targetOwner==='string'?expansion.targetOwner:null,sent:Math.max(0,Number(expansion.sent)||0),remaining:Math.max(0,Number.isFinite(expansion.remaining)?Number(expansion.remaining):Number(expansion.sent)||0),defenderTroops:Math.max(0,Number(expansion.defenderTroops)||0),frontWidth:Math.max(1,Math.floor(Number(expansion.frontWidth)||1)),launchIndex:Math.max(0,Math.min(tiles.length-1,Number(expansion.launchIndex)||0)),candidates,cursor,captured:Math.max(0,Math.floor(Number(expansion.captured)||0))});
  }
  roundPlayerCount=players.size;ships=[];console.log('Restored compact 1000x1000 frontier snapshot.');
 }catch(error){console.error('Could not restore saved frontier snapshot:',error);}
}
function send(ws: WebSocket, packet: unknown) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(packet)); }
function sendChunk(client:Client,cx:number,cy:number){
 const startX=cx*CHUNK_SIZE,startY=cy*CHUNK_SIZE;if(startX<0||startY<0||startX>=WIDTH||startY>=HEIGHT)return;
 const width=Math.min(CHUNK_SIZE,WIDTH-startX),height=Math.min(CHUNK_SIZE,HEIGHT-startY),items:Array<{i:number;tile:Tile}>=[];
 for(let y=0;y<height;y++)for(let x=0;x<width;x++){const i=index(startX+x,startY+y);items.push({i,tile:tiles[i]!});}
 client.loadedChunks.add(chunkKey(cx,cy));send(client.socket,{type:'chunk',cx,cy,mapSeed,items});
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
function join(client:Client,requestedName?:string,requestedPreference?:string){
 if(client.joined)return;
 let player=players.get(client.id);
 if(!player&&players.size>=MAX_PLAYERS){send(client.socket,{type:'error',text:'This live match is full (50/50). Try the next round.'});return;}
 const preference=['northwest','north','northeast','west','center','east','southwest','south','southeast'].includes(requestedPreference||'')?requestedPreference!:'auto';
 if(!player){
  const name=(requestedName||'Commander').replace(/[^a-zA-Z0-9 _-]/g,'').trim().slice(0,18)||'Commander';
  player={id:client.id,name,color:colorFor(players.size),gold:160,score:0,connected:true,home:-1,spawnPreference:preference,workerRatio:30,territory:0,troopsTotal:0,troopMax:0,troopRate:0,goldRate:1};
  players.set(client.id,player);roundPlayerCount=Math.max(roundPlayerCount,players.size);
  if(matchPhase==='countdown'&&countdownEndsAt===0)countdownEndsAt=Date.now()+60000;
  if(matchPhase==='playing')spawn(player);
  notify('✦ '+name+' joined the '+(matchPhase==='countdown'?'waiting lobby':matchPhase==='spawning'?'spawn phase':'live match')+' ('+players.size+'/'+MAX_PLAYERS+').');
 }else{
  player.connected=true;const updatedName=(requestedName||'').replace(/[^a-zA-Z0-9 _-]/g,'').trim().slice(0,18);if(updatedName)player.name=updatedName;
  if(matchPhase==='playing'&&player.home<0)spawn(player);
 }
 client.joined=true;
 trackAccountJoin(client.id,player.name);
 send(client.socket,{type:'welcome',id:client.id});
 broadcast();void persistSnapshot();
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
function buildExpansionCandidates(ownerId:string,targetOwner:string|null,owned:number[],centerX:number,centerY:number,dirX:number,dirY:number,targetIndex:number,sent:number):ExpansionCandidate[]{
 const visited=new Uint8Array(tiles.length),simulatedOwned=new Uint8Array(tiles.length);
 const heap:ExpansionCandidate[]=[],candidates:ExpansionCandidate[]=[];
 const tx=targetIndex%WIDTH,ty=Math.floor(targetIndex/WIDTH),limit=Math.min(16000,Math.max(160,Math.ceil(sent*90)));
 const canEnter=(i:number)=>tiles[i]!.terrain!=='water'&&tiles[i]!.owner===targetOwner;
 const push=(candidate:ExpansionCandidate)=>{
  let i=heap.length;heap.push(candidate);
  while(i>0){const parent=(i-1)>>1;if(heap[parent]!.priority<=candidate.priority)break;heap[i]=heap[parent]!;i=parent;}
  heap[i]=candidate;
 };
 const pop=():ExpansionCandidate|undefined=>{
  if(!heap.length)return undefined;
  const first=heap[0]!,last=heap.pop()!;
  if(heap.length){let i=0;while(true){const left=i*2+1,right=left+1;if(left>=heap.length)break;const child=right<heap.length&&heap[right]!.priority<heap[left]!.priority?right:left;if(heap[child]!.priority>=last.priority)break;heap[i]=heap[child]!;i=child;}heap[i]=last;}
  return first;
 };
 const addFrontierTile=(tileIndex:number)=>{
  if(visited[tileIndex]||!canEnter(tileIndex))return;
  visited[tileIndex]=1;
  const tile=tiles[tileIndex]!,x=tileIndex%WIDTH,y=Math.floor(tileIndex/WIDTH),rx=x-centerX,ry=y-centerY;
  const forward=rx*dirX+ry*dirY,lateral=Math.abs(rx*dirY-ry*dirX);
  const edge=.55+lateral*.018-forward*.035+Math.hypot(tx-x,ty-y)*.0007+(valueNoise(x*.08,y*.08,mapSeed)-.5)*.025;
  let adjacentOwned=0;
  for(const next of neighbors(tileIndex))if(tiles[next]!.owner===ownerId||simulatedOwned[next])adjacentOwned++;
  const terrainBias=tile.terrain==='mountain'?2:tile.terrain==='forest'?1.5:1;
  // OpenFront-style heap priority: each claimed tile exposes a fresh border,
  // and the next tile is chosen from the whole active edge, not a fixed line.
  const priority=valueNoise(x*.17,y*.17,mapSeed+tileIndex)*7+(1-adjacentOwned*.5+terrainBias/2)*10+Math.max(0,lateral-Math.max(0,forward)*.25)*.012-Math.max(0,forward)*.002;
  const pressure=tile.terrain==='mountain'?.34:tile.terrain==='forest'?.19:.16;
  push({index:tileIndex,edge,pressure,priority});
 };
 for(const i of owned)for(const next of neighbors(i))addFrontierTile(next);
 while(heap.length&&candidates.length<limit){
  const candidate=pop();if(!candidate)break;
  if(!canEnter(candidate.index))continue;
  candidates.push(candidate);
  simulatedOwned[candidate.index]=1;
  for(const next of neighbors(candidate.index))addFrontierTile(next);
 }
 return candidates;
}
function claim(client:Client,player:Player,message:any){
 const targetIndex=message.tile as number;
 if(!Number.isInteger(targetIndex)||targetIndex<0||targetIndex>=tiles.length)return;
 const target=tiles[targetIndex]!,targetOwner=target.owner;
 if(targetOwner===player.id){send(client.socket,{type:'error',text:'That territory is already yours.'});return;}
 if(target.terrain==='water'){send(client.socket,{type:'error',text:'Aim at reachable land to expand in that direction.'});return;}
 if(activeExpansions.filter(exp=>exp.owner===player.id).length>=2){send(client.socket,{type:'error',text:'Your nation already has two expansion fronts moving. Let them advance first.'});return;}
 const owned:number[]=[];let totalTroops=0,centerX=0,centerY=0,targetTroops=0,targetLand=0;
 for(let i=0;i<tiles.length;i++){
  const tile=tiles[i]!;
  if(tile.owner===player.id){owned.push(i);totalTroops+=tile.troops;centerX+=i%WIDTH;centerY+=Math.floor(i/WIDTH);}
  else if(targetOwner&&tile.owner===targetOwner){targetTroops+=tile.troops;targetLand++;}
 }
 if(!owned.length||totalTroops<1){send(client.socket,{type:'error',text:'Your nation has no available troops.'});return;}
 centerX/=owned.length;centerY/=owned.length;
 const tx=targetIndex%WIDTH,ty=Math.floor(targetIndex/WIDTH);
 let dirX=tx-centerX,dirY=ty-centerY,distance=Math.hypot(dirX,dirY);
 if(distance<.001){dirX=(player.home%WIDTH)-centerX;dirY=Math.floor(player.home/WIDTH)-centerY;distance=Math.hypot(dirX,dirY);}
 if(distance<.001){dirX=1;dirY=0;distance=1;}
 dirX/=distance;dirY/=distance;
 let launchIndex=-1,bestScore=-Infinity,frontWidth=0;
 for(const i of owned){
  const border=neighbors(i).some(next=>tiles[next]!.owner===targetOwner&&tiles[next]!.terrain!=='water');
  if(!border)continue;
  frontWidth++;
  const x=i%WIDTH,y=Math.floor(i/WIDTH),rx=x-centerX,ry=y-centerY,forward=rx*dirX+ry*dirY,lateral=Math.abs(rx*dirY-ry*dirX);
  const targetDistance=Math.hypot(tx-x,ty-y),score=forward-lateral*.32-targetDistance*.015;
  if(score>bestScore){bestScore=score;launchIndex=i;}
 }
 if(launchIndex<0){send(client.socket,{type:'error',text:'Your nation has no connected border with that territory.'});return;}
 const percent=Math.max(1,Math.min(100,Math.floor(Number.isFinite(Number(message.percent))?Number(message.percent):25)));
 const sent=Math.floor(totalTroops*percent/100);
 if(sent<1){send(client.socket,{type:'error',text:'Not enough troops are available for this expansion.'});return;}
 const candidates=buildExpansionCandidates(player.id,targetOwner,owned,centerX,centerY,dirX,dirY,targetIndex,sent);
 if(!candidates.length){send(client.socket,{type:'error',text:'There is no connected land ahead of your border.'});return;}
 const withdrawal=sent/totalTroops;
 for(const i of owned){tiles[i]!.troops=Math.max(0,tiles[i]!.troops*(1-withdrawal));markTile(i);}
 player.troopsTotal=Math.max(0,totalTroops-sent);
 activeExpansions.push({id:randomUUID(),owner:player.id,targetOwner,sent,remaining:sent,defenderTroops:targetTroops,frontWidth,launchIndex,candidates,cursor:0,captured:0});
 notify('🌐 '+player.name+' launched a '+percent+'% directed border advance.');
 broadcast();void persistSnapshot();
}
function advanceExpansionFronts(){
 if(!activeExpansions.length)return;
 let changed=false,completed=false;
 for(let e=activeExpansions.length-1;e>=0;e--){
  const expansion=activeExpansions[e]!,player=players.get(expansion.owner);
  if(!player){activeExpansions.splice(e,1);continue;}
  const strength=expansion.targetOwner?Math.max(.6,Math.min(2,expansion.defenderTroops/Math.max(1,expansion.sent))):1;
  // Unclaimed settlement growth advances as a broad connected wave; contested land keeps its current progression.
  const settlementSweep=expansion.targetOwner===null;
  // Small, frequent steps make the frontier read as a continuous flow instead of a single large fill.
  const batch=settlementSweep?Math.min(expansion.candidates.length-expansion.cursor,Math.max(8,Math.min(48,Math.ceil(expansion.frontWidth*.35)))):Math.max(6,Math.min(30,Math.ceil(expansion.frontWidth*.22)));
  const end=Math.min(expansion.candidates.length,expansion.cursor+batch);
  while(expansion.cursor<end){
   const candidate=expansion.candidates[expansion.cursor++]!,tile=tiles[candidate.index]!;
   if(tile.terrain==='water'||tile.owner!==expansion.targetOwner)continue;
   const onBorder=neighbors(candidate.index).some(next=>tiles[next]!.owner===expansion.owner);
   if(!onBorder)continue;
   const defensePost=(tile.buildings||[]).some(b=>b.type==='defense_post');
   const strength=expansion.targetOwner?Math.max(.6,Math.min(2,expansion.defenderTroops/Math.max(1,expansion.sent))):1;
   const cost=candidate.pressure*(expansion.targetOwner?strength:1)+(expansion.targetOwner?tile.troops*.015:0)+(defensePost?.55:0);
   if(expansion.remaining<cost){
    if(expansion.targetOwner&&tile.troops>0&&expansion.remaining>0){tile.troops=Math.max(0,tile.troops-Math.floor(expansion.remaining*2));markTile(candidate.index);changed=true;}
    expansion.remaining=0;expansion.cursor=expansion.candidates.length;break;
   }
   expansion.remaining-=cost;
   if(tile.owner){const previous=players.get(tile.owner);if(previous){previous.score=Math.max(0,previous.score-1);previous.territory=Math.max(0,(previous.territory||0)-1);}}
   tile.owner=expansion.owner;player.territory=Math.max(0,(player.territory||0)+1);tile.troops=Math.max(0,Math.floor(Math.min(1,expansion.remaining*.025)));
   player.troopsTotal=Math.max(0,(player.troopsTotal||0)+tile.troops);
   addLandClaimed(expansion.owner,1);
   player.score++;expansion.captured++;markTile(candidate.index);changed=true;
  }
  if(expansion.cursor>=expansion.candidates.length){
   const home=player.home>=0&&tiles[player.home]?.owner===expansion.owner?player.home:expansion.launchIndex;
   const refund=expansion.captured?Math.floor(expansion.remaining*.8):Math.floor(expansion.sent*.7);
   if(refund>0&&home>=0){tiles[home]!.troops+=refund;player.troopsTotal=Math.max(0,(player.troopsTotal||0)+refund);markTile(home);}
   if(!expansion.captured)notify('🛡 '+player.name+'s attack front could not break through that territory.');
   else notify('🌎 '+player.name+'s border finished advancing and captured '+expansion.captured+' tiles.');
   activeExpansions.splice(e,1);completed=true;
  }
 }
 if(changed)broadcast();
 if(completed){checkVictory();void persistSnapshot();}
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
    void persistRoundResults(champion.id);
    void persistSnapshot();
    nextMatchTimer = setTimeout(() => {
      round++;
      freshWorld(true);
      for(const client of clients.values())client.loadedChunks.clear();
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
  if(message.type==='spawn'){
    if(matchPhase!=='spawning'){send(client.socket,{type:'error',text:'Spawn selection is not open yet.'});return;}
    if(player.home>=0){send(client.socket,{type:'error',text:'You have already placed your starting territory.'});return;}
    if(!Number.isInteger(message.tile)||!spawn(player,Number(message.tile))){send(client.socket,{type:'error',text:'Choose an open land location with room for the full starting territory.'});return;}
    notify('🏁 '+player.name+' placed a starting territory.');
    broadcast();maybeFinishSpawnPhase();void persistSnapshot();return;
  }
  if(matchPhase!=='playing'){
    if(message.type==='ping')send(client.socket,{type:'pong',time:Date.now()});
    return;
  }
  if(message.type==='workers'){const now=Date.now();if(now-client.lastAction<140)return;client.lastAction=now;player.workerRatio=Math.max(0,Math.min(100,Math.floor(Number(message.ratio)||0)));broadcast();return;}
  const now = Date.now(); if (now - client.lastAction < 180) return; client.lastAction = now;
  if (message.type === 'recruit') {
    const homeTile = tiles[player.home];
    const target = homeTile?.owner === player.id ? player.home : tiles.findIndex(t=>t.owner===player.id);
    if (target === undefined || target < 0) return;
    const troopRoom=Math.max(0,(player.troopMax||0)-(player.troopsTotal||0));
    const amount=Math.min(15,troopRoom);
    if(amount<=0){send(client.socket,{type:'error',text:'Your troop bar is full. Expand your capacity before recruiting more.'});return;}
    const cost=Math.ceil(25*amount/15);
    if(player.gold<cost){send(client.socket,{type:'error',text:'You need '+cost+' gold to recruit.'});return;}
    player.gold-=cost;tiles[target]!.troops+=amount;player.troopsTotal=(player.troopsTotal||0)+amount;markTile(target);
    notify('＋ '+player.name+' recruited '+amount+' troops.');broadcast();void persistSnapshot();return;
  }
  if(message.type==='boat'&&Number.isInteger(message.source)&&Number.isInteger(message.target)){
    const sourceIndex=Number(message.source),targetIndex=Number(message.target);
    if(sourceIndex<0||sourceIndex>=tiles.length||targetIndex<0||targetIndex>=tiles.length)return;
    const source=tiles[sourceIndex]!,target=tiles[targetIndex]!;
    const hasPort=(source.buildings||[]).some(b=>b.type==='port');
    const coastal=neighbors(sourceIndex).some(n=>tiles[n]!.terrain==='water');
    const targetCoastal=neighbors(targetIndex).some(n=>tiles[n]!.terrain==='water');
    if(source.owner!==player.id||!hasPort||!coastal){send(client.socket,{type:'error',text:'Build a Port on your coast before dispatching a settlement ferry.'});return;}
    if(target.owner!==null||target.terrain==='water'||target.terrain==='mountain'||!targetCoastal){send(client.socket,{type:'error',text:'Ferries can only settle an unclaimed coastal land tile.'});return;}
    if(player.gold<60){send(client.socket,{type:'error',text:'A settlement ferry costs 60 gold.'});return;}
    const path=seaRoute(sourceIndex,targetIndex);
    if(path.length<3){send(client.socket,{type:'error',text:'No open sea route connects those coasts.'});return;}
    player.gold-=60;ships.push({id:randomUUID(),owner:player.id,from:sourceIndex,to:targetIndex,path,step:0,kind:'colony'});
    notify('🚢 '+player.name+' dispatched a settlement ferry.');broadcast();void persistSnapshot();return;
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
      defense_post:{cost:80,max:1}, market:{cost:130,max:3}, farm:{cost:80,max:3}, highway:{cost:110,max:1}
    };
    const rule = rules[building];
    if (!rule || tile.owner !== player.id || tile.terrain === 'water' || tile.terrain === 'mountain') {
      send(client.socket,{type:'error',text:'Build on one of your own suitable land territories.'}); return;
    }
    tile.buildings = Array.isArray(tile.buildings) ? tile.buildings : [];
    const existing = tile.buildings.filter(b => b.type === building);
    const currentLevel = building === 'city' ? (existing[0]?.level || 0) : existing.length;
    if (currentLevel >= rule.max) { send(client.socket,{type:'error',text:'That structure is already at its maximum level here.'}); return; }
    if (building === 'highway' && !neighbors(targetIndex).some(n => { const neighbor=tiles[n]!; return neighbor.owner===player.id && (neighbor.buildings||[]).some(b=>['city','factory','market','highway'].includes(b.type)); })) { send(client.socket,{type:'error',text:'Highways must connect to a City, Factory, Market, or another Highway.'}); return; }
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
            void flushLeaderboardBuffers();
            players.delete(id);
            for (let i=0;i<tiles.length;i++) if (tiles[i]!.owner===id) { tiles[i]!.owner=null;tiles[i]!.troops=0;tiles[i]!.buildings=[];markTile(i); }
            if (player.connected) notify('↘ ' + player.name + ' left the frontier.');
            if(matchPhase==='spawning')maybeFinishSpawnPhase();
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
 if(!tiles.length||!restoredPhaseSnapshot)freshWorld(true);
 else{for(const player of players.values()){player.connected=false;player.spawnPreference=player.spawnPreference||'auto';player.workerRatio=Number.isFinite(player.workerRatio)?player.workerRatio:30;player.territory=player.territory||0;player.troopsTotal=player.troopsTotal||0;player.troopMax=player.troopMax||0;}roundPlayerCount=players.size;}
 setInterval(()=>{
  const now=Date.now();
  if(matchPhase==='countdown'&&countdownEndsAt>0&&now>=countdownEndsAt){
   matchPhase='spawning';spawnEndsAt=now+30000;
   notify('🧭 Countdown complete. Choose your starting territory on the map!');
   broadcast();void persistSnapshot();
  }else if(matchPhase==='spawning'&&spawnEndsAt>0&&now>=spawnEndsAt){
   for(const player of players.values())if(player.connected&&player.home<0)spawn(player);
   matchPhase='playing';spawnEndsAt=0;notify('🌎 Starting phase is over. The match is live!');
   broadcast();void persistSnapshot();
  }
 },250);
 setInterval(()=>{if(matchPhase!=='playing')return;advanceExpansionFronts();},60);
 setInterval(()=>{void flushLeaderboardBuffers();},60000);
 setInterval(()=>{
  if(matchPhase!=='playing')return;
  tick++;
  const summary=new Map<string,{land:number;troops:number;cityLevels:number;farms:number;income:number;home:number;owned:number[]}>();
  for(const player of players.values())summary.set(player.id,{land:0,troops:0,cityLevels:0,farms:0,income:0,home:-1,owned:[]});
  for(let i=0;i<tiles.length;i++){
   const tile=tiles[i]!,stats=tile.owner?summary.get(tile.owner):undefined;if(!stats)continue;
   stats.land++;stats.troops+=tile.troops;stats.owned.push(i);if(stats.home<0)stats.home=i;
   for(const building of tile.buildings||[]){
    if(building.type==='city'){stats.cityLevels+=building.level;stats.income+=building.level*2;}
    if(building.type==='factory')stats.income+=3;
    if(building.type==='port')stats.income+=2;
    if(building.type==='market')stats.income+=4;
    if(building.type==='farm')stats.farms++;
    if(building.type==='highway')stats.income+=2+(neighbors(i).some(n=>{const neighbor=tiles[n]!;return neighbor.owner===tile.owner&&(neighbor.buildings||[]).some(b=>['city','factory','market','highway'].includes(b.type));})?1:0);
   }
  }
  for(const player of players.values()){
   const stats=summary.get(player.id)!;
   // Resource values are scaled down by 1,000 versus OpenFront so Frontier's
   // existing building prices remain meaningful.
   const maxTroops=Math.floor(2*(Math.pow(Math.max(1,stats.land),.6)+50)+stats.cityLevels*250);
   // Enforce the national troop cap even if older saves or manual actions left an army over limit.
   if(stats.troops>maxTroops){
    const overflowStart=stats.troops-maxTroops;let overflow=overflowStart;
    const preferredHome=player.home>=0&&tiles[player.home]?.owner===player.id?player.home:stats.home;
    if(preferredHome>=0){const tile=tiles[preferredHome]!;const removed=Math.min(overflow,tile.troops);tile.troops-=removed;overflow-=removed;markTile(preferredHome);}
    for(const tileIndex of stats.owned){if(overflow<=0)break;if(tileIndex===preferredHome)continue;const tile=tiles[tileIndex]!;const removed=Math.min(overflow,tile.troops);if(removed>0){tile.troops-=removed;overflow-=removed;markTile(tileIndex);}}
    stats.troops-=overflowStart-overflow;
   }
   const baseGoldPerSecond=1;
   const baseTroopRate=(.01+Math.pow(Math.max(0,stats.troops),.73)/(4*Math.pow(1000,.27)))*10*1.35;
   const troopRate=Math.max(0,baseTroopRate*(1-stats.troops/Math.max(1,maxTroops)))*(1+Math.min(12,stats.farms)*.10);
   player.territory=stats.land;player.troopsTotal=stats.troops;player.troopMax=maxTroops;
   player.troopRate=troopRate;
   player.goldRate=baseGoldPerSecond+stats.income/(TICK_MS/1000);
   player.gold=Math.min(999999,player.gold+baseGoldPerSecond*(TICK_MS/1000)+stats.income);
   const home=player.home>=0&&tiles[player.home]?.owner===player.id?player.home:stats.home;
   if(home>=0){
    const add=Math.min(Math.max(0,maxTroops-stats.troops),troopRate*(TICK_MS/1000));
    if(add>0){tiles[home]!.troops+=add;player.troopsTotal=stats.troops+add;markTile(home);}
   }
  }
  // Civilian ferries colonize unclaimed coast; trade ships connect ports and share gold.
  for(let i=ships.length-1;i>=0;i--){
   const ship=ships[i]!;ship.step=Math.min(ship.path.length-1,ship.step+8);
   if(ship.step>=ship.path.length-1){
    const target=tiles[ship.to]!;
    if(ship.kind==='colony'&&target.owner===null&&target.terrain!=='water'&&target.terrain!=='mountain'){
     target.owner=ship.owner;target.troops=1;markTile(ship.to);addLandClaimed(ship.owner,1);
     const owner=players.get(ship.owner);if(owner){owner.score++;owner.territory=Math.max(0,(owner.territory||0)+1);owner.troopsTotal=Math.max(0,(owner.troopsTotal||0)+1);notify('🏝 '+owner.name+' founded a new island settlement.');}
    }else if(ship.kind==='trade'){
     const from=players.get(ship.owner),to=ship.destinationOwner?players.get(ship.destinationOwner):from;
     const payout=Math.min(250,Math.max(20,Math.floor(ship.path.length*.3)));
     if(from)from.gold=Math.min(999999,from.gold+payout);
     if(to&&to.id!==from?.id)to.gold=Math.min(999999,to.gold+payout);
    }
    ships.splice(i,1);
   }
  }
  if(tick%20===0&&ships.filter(s=>s.kind==='trade').length<20){
   const ports:number[]=[];for(let i=0;i<tiles.length;i++){const tile=tiles[i]!;if(tile.owner&&(tile.buildings||[]).some(b=>b.type==='port'))ports.push(i);}
   if(ports.length>=2){
    const from=ports[Math.floor(Math.random()*ports.length)]!;
    const to=ports.find(i=>i!==from&&tiles[i]!.owner!==tiles[from]!.owner)||ports.find(i=>i!==from);
    if(to!==undefined){const path=seaRoute(from,to);if(path.length>1)ships.push({id:randomUUID(),owner:tiles[from]!.owner!,from,to,path,step:0,kind:'trade',destinationOwner:tiles[to]!.owner||undefined});}
   }
  }
  checkVictory();broadcast();if(tick%3===0)void persistSnapshot();
 },TICK_MS);
 http.listen(port,'0.0.0.0',()=>console.log('Frontier 1000x1000 chunked world server listening on '+port));
}
process.on('SIGTERM', async () => { if(nextMatchTimer)clearTimeout(nextMatchTimer);await flushLeaderboardBuffers();await persistSnapshot();http.close(() => process.exit(0)); });
void boot();
