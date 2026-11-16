# Frontier Server

Express health/wake endpoints and WebSocket player presence.

## Deploy on Render
1. Render → New → Web Service → connect this repository. Render reads `render.yaml`.
2. Add environment variables:
   - `SUPABASE_URL` = `https://rkenxijtbmiqcgwhvtgr.supabase.co`
   - `SUPABASE_SECRET_KEY` = the server-side Supabase secret key (keep private)
   - `CLIENT_ORIGIN` = your GitHub Pages origin, e.g. `https://xwowzersx-sub.github.io`
3. Deploy and test `https://YOUR-SERVICE.onrender.com/health`.
4. Add the full Render URL as `VITE_SERVER_URL` in the client repository's Actions Variables.

Free Render services sleep when idle. The client's `/health` request wakes the service; the first connection may take a minute or more. Active sockets and in-memory player presence reset on restart.

This is a connection/presence foundation. The client map is currently a visual prototype; authoritative RTS simulation, matchmaking, and gameplay commands remain to be built.