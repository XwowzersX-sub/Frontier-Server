import express from 'express';
import cors from 'cors';
import { createClient } from '@supabase/supabase-js';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer } from 'node:http';
const app = express();
app.use(cors({ origin: process.env.CLIENT_ORIGIN?.split(',') ?? true }));
const http = createServer(app), wss = new WebSocketServer({ noServer: true });
const port = Number(process.env.PORT || 10000);
const admin = process.env.SUPABASE_URL && process.env.SUPABASE_SECRET_KEY ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SECRET_KEY, { auth: { persistSession: false } }) : null;
type Player = { id: string; socket: WebSocket };
const players = new Map<string, Player>();
app.get('/', (_q, r) => r.send('Pixel Frontier server online'));
app.get('/health', (_q, r) => r.json({ ok: true, players: players.size }));
app.get('/wake', (_q, r) => r.json({ ok: true }));
function broadcast() { const data = JSON.stringify({ type: 'players', players: players.size }); for (const p of players.values()) if (p.socket.readyState === WebSocket.OPEN) p.socket.send(data); }
async function verify(token?: string) { if (!token || !admin) return null; const { data, error } = await admin.auth.getUser(token); return error ? null : data.user; }
http.on('upgrade', async (req, socket, head) => {
 const u = new URL(req.url || '/', 'http://localhost'); if (u.pathname !== '/ws') { socket.destroy(); return; }
 try { const user = await verify(u.searchParams.get('token') || undefined); const id = user?.id || 'guest-' + Math.random().toString(36).slice(2);
  wss.handleUpgrade(req, socket, head, ws => { if (user?.id && players.has(id)) players.get(id)!.socket.close(); players.set(id, { id, socket: ws }); ws.send(JSON.stringify({ type: 'welcome', id, players: players.size })); broadcast();
   ws.on('message', raw => { try { const m = JSON.parse(raw.toString()); if (m.type === 'ping') ws.send(JSON.stringify({ type: 'pong', time: Date.now() })); } catch {} });
   ws.on('close', () => { if (players.get(id)?.socket === ws) players.delete(id); broadcast(); });
  });
 } catch { socket.destroy(); }
});
http.listen(port, '0.0.0.0', () => console.log('Frontier server listening on ' + port));