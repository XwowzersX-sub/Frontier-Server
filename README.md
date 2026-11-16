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
- Mountains and water cannot be captured.
- Build on owned land: City (100 gold), Factory (150), Port (120), Defense Post (80), Market (130), and Farm (80).
- Ports require a coastline. Cities can be upgraded to level 4 and increase local troop capacity. Factories, Ports, and Markets improve gold income. Farms increase troop recovery. Defense Posts strengthen a tile's garrison.

## Persistence and limitations
The server stores the shared map, player scores, gold, troops, terrain, and buildings in `public.game_snapshots` in Supabase. Snapshots save after player actions and periodically. Connected status resets after restart. The server still has one global match; ranked play, teams, separate lobbies, map selection, replays, and advanced diplomacy are not implemented.

## Local development
```bash
npm install
npm run dev
```
