# Sharing the app on the LAN

Two isolated instances run on **this PC** (all CPU/GPU/DB on this machine). The friend only
needs a browser on the same Wi-Fi.

| Instance | Open in browser | Backend (local only) | Data |
|---|---|---|---|
| me | `http://<LAN-IP>:5173` (or `http://localhost:5173`) | `127.0.0.1:8000` | `backend/agentic_ai_demo.db` (from `backend/.env`) |
| friend | `http://<LAN-IP>:5174` | `127.0.0.1:8010` | `backend/agentic_ai_friend.db` (from `backend/.env.friend`) |

`<LAN-IP>` is printed by the start scripts (currently `192.168.80.96`; it can change if your
router hands out a new address).

## Start / stop

Double-click in `scripts\lan\` (or run the `.ps1` files from PowerShell):

- `start-me.cmd` – your instance, reachable on the LAN. If your backend is already running on
  8000 (e.g. `uvicorn --reload`), it is reused. A plain `npm run dev` on 5173 must be stopped
  first (it only listens on localhost).
- `start-friend.cmd` – the friend's instance.
- `stop-friend.cmd` / `stop-all.cmd` – stop what the scripts started (never your own
  manually started servers).

Logs: `.lan-run\<instance>-backend.*.log`, `.lan-run\<instance>-frontend.*.log`.

The ML service (`ml_service`, port 8001) and Ollama are shared by both instances; start them
as usual if you want diagnosis/assistant features.

## Friend's login

Email `friend@example.com`, password = `ADMIN_PASSWORD` in `backend/.env.friend`. The account
was created on the first start of the empty friend database; changing `ADMIN_PASSWORD` later
does not change an existing account.

## How it works

- Browsers only talk to the Vite dev server (bound to `0.0.0.0`). Vite proxies `/api` to that
  instance's backend (`API_PROXY_TARGET`), so the API is same-origin and the httpOnly
  `SameSite=Strict` refresh cookie works over plain http. The backends stay on `127.0.0.1`.
- `backend/.env.friend` is loaded as process env vars for the friend backend only; it overrides
  `backend/.env` (own DB, JWT secret, encryption key, admin) and everything else is inherited.
- The friend instance cannot reach your local Airflow / databases (`*_ALLOWED_HOSTS` set to an
  unresolvable host); Mock Airflow connections still work. Webhooks: Slack only.
  Delete those lines in `.env.friend` to share your sandbox.
- Your normal workflow is unchanged: `npm run dev` + `uvicorn app.main:app --reload` behave
  exactly as before.

## Reset the friend's data

Stop the friend instance, rename/delete `backend/agentic_ai_friend.db`, then from `backend\`:

```powershell
$env:DATABASE_URL='sqlite:///./agentic_ai_friend.db'; .\.venv\Scripts\python -m alembic upgrade head; Remove-Item Env:DATABASE_URL
```

## Firewall (admin PowerShell)

Your Wi-Fi is on the **Public** profile, so the rule is limited to your local subnet:

```powershell
New-NetFirewallRule -DisplayName "agenticAi LAN frontends" -Direction Inbound -Action Allow `
  -Protocol TCP -LocalPort 5173,5174 -RemoteAddress LocalSubnet -Profile Public,Private
```

Note: Windows already has an auto-created rule "Node.js JavaScript Runtime" that allows
node.exe on **any port from any address** (Public). Optional tightening — limit it to the
local subnet:

```powershell
Get-NetFirewallRule -DisplayName "Node.js JavaScript Runtime" | Set-NetFirewallRule -RemoteAddress LocalSubnet
```

Undo: `Remove-NetFirewallRule -DisplayName "agenticAi LAN frontends"`.

## Files touched by the LAN setup

- `frontend/vite.config.js` – proxy target and cache dir from env (defaults unchanged).
- `backend/.env` – LAN origin added to `CORS_ORIGINS`.
- `.gitignore` – ignores `.lan-backup/`, `.lan-run/`, `backend/.env.friend`.
- New: `backend/.env.friend`, `backend/agentic_ai_friend.db`, `scripts/lan/*`, this file.
- Backups of the originals: `.lan-backup/`.
