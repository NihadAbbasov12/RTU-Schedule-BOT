# RTU Schedule Telegram Bot

Python 3.11 Telegram bot for the public RTU lesson schedule site at `https://nodarbibas.rtu.lv`.

The bot is now locked to the current Foreign Students setup and uses a simplified onboarding flow:

- Study period: `2025/2026 Spring semester (25/26-SP)`
- Department: `Foreign Students Department (02A00)`
- User choice: `program family -> course -> group`

## What The Bot Does

- Fetches RTU public schedule data without browser automation
- Groups duplicate-looking RTU program rows into clean program-family buttons
- Resolves one underlying RTU program row per family for course and group loading
- Filters group lists to keep Telegram selection clean and numerically sorted
- Stores one saved study selection per chat in SQLite
- Supports `Today`, `Tomorrow`, `Week`, `Subjects`, `Refresh`, `Status`, and `Change selection`
- Sends scheduled `Today` and `Tomorrow` messages when the scheduler is enabled
- Stores snapshots and reports schedule changes on refresh

## Current Selection Flow

When a user opens the bot and presses `/start`, the bot automatically uses:

- `2025/2026 Spring semester (25/26-SP)`
- `Foreign Students Department (02A00)`

Then it shows only deduplicated program-family buttons, for example:

- `Computer Systems`
- `Economics`
- `Business Informatics`
- `Entrepreneurship and Management`

After that:

1. Choose the program family
2. Choose the course if more than one course exists
3. Choose the filtered group
4. The bot saves the resolved selection and shows the main menu

The main menu stays visible as a persistent reply keyboard:

- `Today`
- `Tomorrow`
- `Week`
- `Subjects`
- `Refresh`
- `Status`
- `Change selection`

## Status Output

`Status` shows:

- fixed study period
- fixed department
- selected program family
- underlying RTU program title and code actually used
- course
- group
- resolved `semesterProgramId`
- timezone
- scheduler state

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

## Example `.env`

```env
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
RTU_BASE_URL=https://nodarbibas.rtu.lv
RTU_LANG=en
RTU_SEMESTER_ID=29
RTU_SEMESTER_TITLE=2025/2026 Spring semester (25/26-SP)
RTU_DEPARTMENT_CODE=02A00
RTU_DEPARTMENT_TITLE=Foreign Students Department (02A00)
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
```

Legacy/default RTU values are still supported in the environment for migration and deterministic representative selection:

- `RTU_PROGRAM_ID`
- `RTU_COURSE_ID`
- `RTU_GROUP`
- `RTU_SEMESTER_PROGRAM_ID`

## Run

```powershell
python app.py
```

## Project Files

- `app.py` - application entrypoint
- `bot.py` - Telegram handlers, onboarding flow, schedule actions, scheduled delivery
- `config.py` - environment loading
- `formatter.py` - Telegram-friendly message formatting
- `models.py` - typed domain models and date helpers
- `rtu_api.py` - RTU API client, family grouping, course and group resolution
- `scheduler.py` - APScheduler integration
- `storage.py` - SQLite storage for chat selections, snapshots, and weekend notifications

## Notes

- The bot loads `.env` automatically with `python-dotenv`
- The scheduler can be disabled with `ENABLE_SCHEDULER=false`
- `Refresh` compares stored daily snapshots and reports added, removed, or changed lessons
- The grouping and filtering layer is designed for the current Foreign Students Department structure, not for all RTU departments at once
