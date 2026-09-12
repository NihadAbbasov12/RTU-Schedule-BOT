# RTU Schedule Telegram Bot

Python 3.11 Telegram bot for the public RTU lesson schedule site at `https://nodarbibas.rtu.lv`.

The bot is locked to the current Foreign Students setup and uses a simplified onboarding flow:

- Study period: `2026/2027 Autumn semester (26/27-A)`
- Department: `Foreign Students Department (02A00)`
- User choice: `program title -> exact RTU program code -> course -> group code`

## Features

- Public RTU schedule fetching without browser automation
- Per-chat saved selection in SQLite
- Telegram commands and persistent reply-keyboard actions
- Daily scheduled `Today` and `Tomorrow` messages
- Weekend notification after the last lesson of the academic week
- Automatic reminder delivery 30 minutes before each lesson
- Duplicate reminder protection in SQLite
- Snapshot refresh and change detection
- Admin-only bot usage statistics

## Main User Flow

When a user opens the bot and presses `/start`, the bot automatically uses:

- `2026/2027 Autumn semester (26/27-A)`
- `Foreign Students Department (02A00)`

Then it shows unique program-title buttons without RTU codes, for example:

- `Chemistry and Chemical Technology`
- `Civil Construction and Real Estate Management`
- `Civil Engineering`
- `Computer Science`
- `Computer Systems`

After the title is selected, the bot shows the exact RTU program-code variants for that title:

- `ADBD0`
- `ADMD0`
- `ADBDW`
- `ADMDW`

Repeated program names with different RTU codes are intentionally separated into this second step to avoid Telegram button truncation and wrong automatic representative selection.

After that:

1. Choose the program title
2. Choose the exact RTU program code
3. Choose the course if more than one course exists
4. Choose the group code for that exact program and course
5. The bot saves the exact resolved selection and shows the main menu

## Main Menu

All users see:

- `Today`
- `Tomorrow`
- `Week`
- `Subjects`
- `Refresh`
- `Status`
- `Change selection`

Admin chats also see:

- `Stats`

## Reminders

The bot can send a reminder shortly before a lesson starts.

Default behavior:

- reminder lead time: 30 minutes
- reminder check interval: every 5 minutes
- duplicate reminder prevention: enabled through SQLite reminder keys

Example reminder:

```text
Reminder

In 30 minutes you have:
Discrete Mathematics
14:30-16:05
Lecturer: I. Kremere
Room: Zun. 10-405
```

## Admin Stats

Admins can use `/stats` or the `Stats` button.

Admin stats include:

- total chats ever seen
- chats with saved selection
- active chats in the last 7 days
- active chats in the last 30 days
- total reminders sent
- total schedule requests handled
- scheduler enabled or disabled
- reminder enabled or disabled
- timezone

Admin access is controlled by `ADMIN_CHAT_IDS`.

## RTU Endpoints Used

- `GET /?lang=en`
- `POST /getChousenSemesterStartEndDate`
- `POST /findProgramsBySemesterId`
- `POST /findCourseByProgramId`
- `POST /findGroupByCourseId`
- `POST /isSemesterProgramPublished`
- `POST /getSemProgSubjects`
- `POST /getSemesterProgEventList`

## Setup

Create and activate a virtual environment:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

Create `.env` from `.env.example`:

```powershell
Copy-Item .env.example .env
```

Then fill in at least:

```env
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

If you want admin access to `/stats`, add chat IDs:

```env
ADMIN_CHAT_IDS=123456789,987654321
```

## Example `.env`

```env
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
ADMIN_CHAT_IDS=
RTU_BASE_URL=https://nodarbibas.rtu.lv
RTU_LANG=en
RTU_SEMESTER_ID=31
RTU_SEMESTER_TITLE=2026/2027 Autumn semester (26/27-A)
RTU_DEPARTMENT_CODE=02A00
RTU_DEPARTMENT_TITLE=Foreign Students Department (02A00)
ENABLE_SCHEDULER=true
DAILY_TODAY_HOUR=7
DAILY_TOMORROW_HOUR=19
WEEKEND_CHECK_INTERVAL_MINUTES=15
REMINDER_ENABLED=true
REMINDER_MINUTES_BEFORE=30
REMINDER_CHECK_INTERVAL_MINUTES=5
TIMEZONE=Europe/Riga
DB_PATH=rtu_schedule.db
LOG_LEVEL=INFO
REQUEST_TIMEOUT_SECONDS=20
REQUEST_CONNECT_TIMEOUT_SECONDS=10
REQUEST_RETRIES=5
REQUEST_BACKOFF_SECONDS=0.8
```

Legacy/default RTU values are still supported for migration and deterministic representative selection:

- `RTU_PROGRAM_ID`
- `RTU_COURSE_ID`
- `RTU_GROUP`
- `RTU_SEMESTER_PROGRAM_ID`

## Run

```powershell
python app.py
```

## SQLite Storage

The bot stores:

- chat selections
- per-chat activity timestamps and counters
- reminder delivery log
- snapshot history for schedule change detection
- weekend notification state

## Project Files

- `app.py` - application entrypoint
- `bot.py` - Telegram handlers, onboarding flow, schedule actions, reminders, admin stats
- `config.py` - environment loading
- `formatter.py` - Telegram-friendly message formatting
- `models.py` - typed domain models and date helpers
- `rtu_api.py` - RTU API client, exact program listing, legacy family grouping, course and group resolution
- `scheduler.py` - APScheduler integration
- `storage.py` - SQLite storage for chat selections, reminders, stats, snapshots, and weekend notifications
- `smoke_test.py` - offline end-to-end test with a mocked RTU API and a throwaway database
- `live_check.py` - end-to-end check against the real RTU API and the real database
- `deploy/` - systemd unit and the server-side setup and deploy scripts
- `.github/workflows/deploy.yml` - rsync + restart deployment on every push to `main`

## Deployment

The bot runs as a systemd service on a plain Ubuntu server. Code is delivered by
`rsync` from GitHub Actions, which then restarts the service.

Layout on the server:

- `/opt/rtu-schedule-bot` - application directory, owned by the `deploy` user
- `/opt/rtu-schedule-bot/.env` - secrets, created once by hand, never synced
- `/opt/rtu-schedule-bot/rtu_schedule.db` - SQLite database, never synced
- `/etc/systemd/system/rtu-schedule-bot.service` - the unit

### One-time server setup

```bash
sudo apt-get update && sudo apt-get install -y git rsync
sudo git clone https://github.com/NihadAbbasov12/RTU-Schedule-BOT.git /tmp/rtu-bot
sudo bash /tmp/rtu-bot/deploy/server-setup.sh
```

`server-setup.sh` installs Python and rsync, creates the `deploy` user and
`/opt/rtu-schedule-bot`, allows that user to restart the service without a
password, and installs plus enables the systemd unit. It is idempotent and never
touches `.env` or the database.

Then add the deploy public key and the environment file:

```bash
echo "<github actions public key>" | sudo tee -a /home/deploy/.ssh/authorized_keys
sudo -u deploy nano /opt/rtu-schedule-bot/.env      # see "Example .env" above
sudo chmod 600 /opt/rtu-schedule-bot/.env
```

### GitHub Actions secrets

| Secret | Value |
| --- | --- |
| `DEPLOY_HOST` | server IP or hostname |
| `DEPLOY_USER` | `deploy` |
| `DEPLOY_SSH_KEY` | the **private** key whose public half is in `authorized_keys` |

### Deploying

Push to `main`, or run the `Deploy` workflow manually. The workflow syncs the
repository (excluding `.env`, `*.db`, `.venv`, `__pycache__`) and then runs
`deploy/deploy.sh` on the server, which installs dependencies, byte-compiles the
sources, restarts the unit, and fails the build if the service is not still
running five seconds later.

Useful commands on the server:

```bash
sudo systemctl status rtu-schedule-bot
sudo journalctl -u rtu-schedule-bot -f
sudo systemctl restart rtu-schedule-bot
```

## Notes

- The bot loads `.env` automatically with `python-dotenv`
- `ENABLE_SCHEDULER=false` disables all scheduled jobs
- `REMINDER_ENABLED=false` disables lesson reminders while keeping other scheduler jobs available
- Duplicate reminders are prevented by a stable reminder key per chat and lesson
- `Refresh` compares stored daily snapshots and reports added, removed, or changed lessons
