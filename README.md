# RTU Schedule Telegram Bot

Python 3.11 Telegram bot for RTU public schedules built with `aiogram 3.x`. The bot uses only the public RTU JSON endpoints at `https://nodarbibas.rtu.lv`, stores per-chat state in SQLite, and supports multiple Telegram chats choosing different RTU groups independently.

## Features

- Uses the public RTU endpoints only
- Built around Telegram reply keyboards for normal usage
- Supports per-chat group selection for Computer Systems groups `1`, `2`, `3`, and `4`
- Resolves the correct `semesterProgramId` dynamically for the selected group
- Stores chat selection, resolved target, schedule snapshots, and weekend notification state in SQLite
- Supports slash commands and button-based actions
- Sends scheduled `Today` and `Tomorrow` updates to chats that already selected a group
- Sends one weekend message per chat after that chat's last lesson of the academic week has ended
- Uses retries, timeouts, logging, and friendly error handling

## Current RTU Configuration

- `RTU_BASE_URL=https://nodarbibas.rtu.lv`
- `RTU_SEMESTER_ID=29`
- `RTU_PROGRAM_ID=1128`
- `RTU_COURSE_ID=1`
- `TIMEZONE=Europe/Riga`

The bot resolves the active `semesterProgramId` dynamically per selected group by calling:

- `POST /findGroupByCourseId`
- `POST /isSemesterProgramPublished`
- `POST /getSemProgSubjects`
- `POST /getSemesterProgEventList`

## How Group Selection Works

The bot no longer uses one global group for everyone.

- Every Telegram chat chooses its own group
- `/start` always opens the onboarding flow and group selection keyboard
- Available groups in the onboarding keyboard:
  - `Group 1️⃣`
  - `Group 2️⃣`
  - `Group 3️⃣`
  - `Group 4️⃣`
- After selection, the bot:
  - saves the selected group for that chat in SQLite
  - resolves the correct `semesterProgramId` for that group
  - stores the resolved `semesterProgramId` for that chat
  - switches the keyboard to the main action menu

If a user runs `/start` again later, the bot shows the group selection keyboard again so the group can be changed.

## How /start Works

`/start` always does the same onboarding flow:

1. Sends a clean welcome message
2. Explains what the bot does
3. Shows the group selection keyboard
4. Lets the user choose a new group even if one was already selected before

This keeps group changes simple and predictable.

## Main Buttons

After a group is selected, the bot shows a persistent reply keyboard with:

- `Today`
- `Tomorrow`
- `Week`
- `Subjects`
- `Refresh`
- `Status`

### Button behavior

- `Today` shows today's lessons for the selected group
- `Tomorrow` shows tomorrow's lessons for the selected group
- `Week` shows the next 7 days for the selected group
- `Subjects` shows the current semester subjects from RTU for the selected group
- `Refresh` refreshes stored snapshots and reports detected schedule changes for the selected group
- `Status` shows the active configuration for that chat

Slash commands still work:

- `/start`
- `/today`
- `/tomorrow`
- `/week`
- `/month`
- `/subjects`
- `/refresh`
- `/status`

## Message Formatting

Schedule messages are compact and grouped by date.

Each lesson includes:

- time
- subject
- lecturer
- room

The bot prefers English RTU fields when available:

- `eventTempNameEn`
- `lecturerInfoTextEn`
- `roomInfoTextEn`

If English fields are missing, the bot falls back to Latvian fields automatically.

## Status Output

`Status` shows the current configuration for the active chat:

- `semesterId`
- `programId`
- `courseId`
- selected group
- resolved `semesterProgramId`
- scheduler enabled or disabled
- timezone

## Weekend Notifications

Weekend notifications are tracked per chat.

- The bot checks the current academic week in `Europe/Riga`
- It finds the last lesson of that week for the chat's selected group
- After that lesson has ended, the bot sends a weekend message
- The message is sent only once per week per chat
- SQLite stores the sent state to avoid duplicates
- A new week automatically uses a new week key, so weekend notifications reset naturally for the next week

Current message text:

`That was the last lesson for this week. Have a great weekend!`

## SQLite Storage

The SQLite database now stores:

- per-chat selected group
- per-chat resolved `semesterProgramId`
- per-chat schedule snapshots for change detection
- per-chat weekend notification state

Legacy single-chat snapshot data can still be migrated automatically if old values are present in the environment.

## Project Files

- `app.py` - application entrypoint
- `bot.py` - aiogram handlers, onboarding flow, scheduled delivery, weekend notifications
- `config.py` - environment loading
- `formatter.py` - Telegram message formatting
- `models.py` - domain models and date helpers
- `rtu_api.py` - RTU public API client
- `scheduler.py` - APScheduler jobs
- `storage.py` - SQLite storage and change detection

## Requirements

- Python 3.11
- Telegram bot token from BotFather

`TELEGRAM_CHAT_ID` is optional now. The bot can run without it because chats register themselves by using `/start` and selecting a group.

## Setup

Example on Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create a `.env` file in the project root and set at least:

```env
TELEGRAM_BOT_TOKEN=your_bot_token
```

## Environment Variables

```env
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
RTU_BASE_URL=https://nodarbibas.rtu.lv
RTU_LANG=en
RTU_SEMESTER_ID=29
RTU_PROGRAM_ID=1128
RTU_COURSE_ID=1
ENABLE_SCHEDULER=true
DAILY_TODAY_HOUR=7
DAILY_TOMORROW_HOUR=19
WEEKEND_CHECK_INTERVAL_MINUTES=15
TIMEZONE=Europe/Riga
DB_PATH=rtu_schedule.db
LOG_LEVEL=INFO
REQUEST_TIMEOUT_SECONDS=20
REQUEST_CONNECT_TIMEOUT_SECONDS=10
REQUEST_RETRIES=5
REQUEST_BACKOFF_SECONDS=0.8
RTU_GROUP=4
RTU_SEMESTER_PROGRAM_ID=28962
```

Notes:

- `RTU_GROUP` and `RTU_SEMESTER_PROGRAM_ID` are kept only for legacy migration from older single-group deployments
- Normal runtime behavior resolves the target dynamically from the chat's selected group
- Scheduler jobs only send automatic updates to chats that have already selected a group

## How To Run The Bot

Run:

```powershell
python app.py
```

The application loads `.env` automatically with `python-dotenv`.

## Reliability Notes

- RTU requests use retries and timeouts
- Handlers and scheduler jobs log tracebacks on failure
- User-facing errors are returned instead of failing silently
- The first refresh builds the local snapshot baseline for that chat and selected group

## What I changed

- Added per-chat group selection with onboarding buttons for `Group 1` to `Group 4`
- Fixed the group selection button mismatch so the actual keyboard text and handler lookup always match
- Changed `/start` so it always shows a welcome message and lets the user re-select the group
- Reworked schedule, subject, refresh, and status actions to use the selected group's dynamically resolved `semesterProgramId`
- Added persistent main menu buttons for `Today`, `Tomorrow`, `Week`, `Subjects`, `Refresh`, and `Status`
- Added per-chat SQLite storage for selected group, resolved `semesterProgramId`, snapshots, and weekend notification tracking
- Added multi-user scheduled delivery so different chats can use different groups
- Added automatic once-per-week weekend notifications after the last lesson of the current academic week has ended
- Kept slash commands supported while making the main UX button-based
- Improved onboarding, status, subjects, and schedule formatting for a cleaner Telegram UX
- Preserved retries, timeouts, logging, and friendly error handling
