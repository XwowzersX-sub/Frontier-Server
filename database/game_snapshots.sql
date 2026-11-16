-- Already applied to the connected Supabase project.
-- Used by the server to preserve the current shared match across Render restarts.
create table if not exists public.game_snapshots (
  id text primary key check (id = 'main'),
  state jsonb not null,
  updated_at timestamptz not null default now()
);
alter table public.game_snapshots enable row level security;
revoke all on table public.game_snapshots from anon, authenticated;
grant select, insert, update, delete on table public.game_snapshots to service_role;
