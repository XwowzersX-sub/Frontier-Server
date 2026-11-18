# WORTH server

A small authoritative WebSocket service for WORTH's 1v1 arena-sport matches. It provides a player queue, five arena themes, two match presets (Loadout and Addition), authoritative WASD movement, jump/crouch/roll state, central-zone scoring, first-to-five rounds, and account progress persistence.

## Deploy on Render

This folder includes a Blueprint at `worth/render.yaml`. Render supports choosing a custom Blueprint path when creating a Blueprint; choose `worth/render.yaml`. The service config uses `rootDir: worth`.

Set the secret environment variable:
- `SUPABASE_SERVICE_ROLE_KEY`: your project's server-side secret key. Add it only in Render's environment settings; never put it in GitHub or browser config.

The public Supabase URL and publishable key are already set in the Blueprint. The client origin is configured to your GitHub Pages site.

After deployment:
1. Open `https://YOUR-SERVICE.onrender.com/health`; expect JSON with `ok: true`.
2. In the WORTH client's `worth/config.js`, set `serverUrl` to `https://YOUR-SERVICE.onrender.com` (no trailing slash).
3. Push the client change and redeploy GitHub Pages.

Render free web services may sleep after inactivity; the first WebSocket connection can take longer while the service wakes.

## Local development
```bash
npm install
npm run dev
```
Server listens on `PORT` (Render injects it; local default is 10000).

## Protocol
The client sends `hello` with a Supabase access token (optional for guest practice) and then `queue` with map/mode and a movement/support loadout. Players are paired by map and mode. The server simulates movement and zone-hold scoring; it does not accept client-side score claims.

This is an early 1v1 foundation. Parties, ranked queues, reconnects, spectators, Unity networking, persistent match history, and production anti-cheat are future work.
