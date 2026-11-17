# Frontier Server

Authoritative multiplayer server for Pixel Frontier. Provides a shared 60×40 world, troop-based click expansion, economy and construction rules, persistence through Supabase snapshots, and WebSocket state broadcasts.

## Deploy on Render
1. Render → New → Web Service → connect this repository. Render reads `render.yaml`.
2. Add environment variables:
   - `SUPABASE_URL` = your Supabase project URL
   - `SUPABASE_SECRET_KEY` = your Supabase server-side secret key (Render environment only)
   - `CLIENT_ORIGIN` = `https://xwowzersx-sub.github.io`
3. Deploy and test `https://YOUR-SERVICE.onrender.com/health`.
4. Add that Render URL as `VITE_SERVER_URL` in the client repository's Actions Variables.

## Current game rules
- Players start with 160 gold and 20 troops on a home tile.
- Every five seconds, owned territory generates gold and troops recover over time.
- Recruit troops for 25 gold.
- Click adjacent neutral or rival land to expand. The selected attack ratio determines the troops committed; expansion does not cost gold.
- Quick chat messages are length-limited, sanitized, and broadcast to connected players.
- Mountains and water cannot be captured.
- Build on owned land: City (100 gold), Factory (150), Port (120), Defense Post (80), Market (130), Farm (80), and Highway (110). Highways must connect to a City, Factory, Market, or another Highway.
- Ports require a coastline and enable peaceful settlement ferries and trade boats. Cities can be upgraded to level 4 and increase troop capacity. Factories, Ports, Markets, and connected Highways improve gold income. Farms increase troop recovery. Expansion fronts now advance in smaller batches.

## Persistence and limitations
The server stores the shared map, player scores, gold, troops, terrain, and buildings in `public.game_snapshots` in Supabase. Snapshots save after player actions and periodically. Connected status resets after restart. The server still has one global multiplayer map. The client provides local-only FFA, Duos, Trios, and Squads with AI, map presets, difficulty, pause, speed controls, and land-win targets. Ranked multiplayer, multiplayer teams, separate lobbies, public map selection, replays, and diplomacy are not implemented.

## Local development
```bash
npm install
npm run dev
```
