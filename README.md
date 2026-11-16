# Frontier Server

Authoritative starter server for Pixel Frontier. Provides a shared 30×20 world, player spawning, passive gold/troop income, expansion, recruitment, enemy territory captures, a leaderboard endpoint, and WebSocket state broadcasts.

## Deploy on Render
1. Render → New → Web Service → connect this repository. Render reads `render.yaml`.
2. Add environment variables:
   - `SUPABASE_URL` = `https://rkenxijtbmiqcgwhvtgr.supabase.co`
   - `SUPABASE_SECRET_KEY` = your Supabase server-side secret key (Render environment only)
   - `CLIENT_ORIGIN` = `https://xwowzersx-sub.github.io`
3. Deploy and test `https://YOUR-SERVICE.onrender.com/health`.
4. Add that Render URL as `VITE_SERVER_URL` in the client repository's Actions Variables.

## Game rules
- Players spawn with a home tile, 60 gold, and 12 troops.
- Every 5 seconds, land generates gold and every third tick owned tiles gain a troop (up to 99).
- Recruit 5 troops for 25 gold.
- Expand into adjacent neutral land for 4 gold.
- Capture enemy land for 8 gold when the total troops on adjacent owned tiles exceeds the defending garrison. Captures consume 3 troops from the strongest adjacent owned tile.
- Mountains are blocked.

## Persistence and limitations
The server stores the shared map, player scores, gold, and troop counts in `public.game_snapshots` in Supabase. Snapshots save on player actions and every 15 seconds. Connected status is reset after restart. The server still has one global match; matchmaking, multiple rooms, full unit movement, and long-term player-stat aggregation are future work.

## Local development
```bash
npm install
npm run dev
```


## Troop-based route expansion

The client sends an `expand` command with a source territory, an adjacent-tile route, and a troop percentage. The server validates every step, removes the selected force from the source, resolves each neutral/enemy garrison along the route, updates scores and ownership, and broadcasts/persists the new match state. Expansion no longer consumes gold; gold remains a resource for recruiting.
