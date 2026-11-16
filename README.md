# Frontier Server

Authoritative starter server for Pixel Frontier. Provides a shared 30×20 map, player spawning, passive gold/troop income, expansion, recruitment, a leaderboard endpoint, and WebSocket state broadcasts.

## Render deployment
Connect this repository as a **Web Service**. Render reads `render.yaml`. Configure:
- `SUPABASE_URL`: your Supabase project URL
- `SUPABASE_SECRET_KEY`: server-side Supabase secret key (Render environment only)
- `CLIENT_ORIGIN`: exact GitHub Pages origin, e.g. `https://xwowzersx-sub.github.io`

The client sends the Supabase access token during WebSocket handshake; server validates it with Supabase Auth. Guests can still join with temporary IDs. Never place the secret key in the client.

## Game rules
- Players spawn with a home tile, 60 gold, and 12 troops.
- Every 5 seconds, land generates gold and troops slowly replenish.
- Recruit 5 troops at your home for 25 gold.
- Select a neighboring non-mountain tile and press Expand. Neutral land costs 4 gold; enemy land costs 8 gold and can be taken only if its garrison is below 4.
- State is currently held in memory. Render restarts/sleep clear the match; persistent match storage and room matchmaking are future work.

## Local development
```bash
npm install
npm run dev
```
