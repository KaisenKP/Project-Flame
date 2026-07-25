# SparkedHost deployment

FlameBot is designed to run as a single Python process on SparkedHost. The
repository is the source of truth; Apollo stores the process environment and
the MySQL connection details.

## Apollo setup

1. Use a supported Python runtime and pin the server to Python 3.12.
2. Deploy a known Git commit or release archive. The server root must contain
   `main.py`, `bot.py`, `requirements.txt`, `cogs/`, `db/`, and `flamebot/`.
3. Configure the startup command as `python main.py`.
4. Install the packages from `requirements.txt` through Apollo's Python
   Packages setting when the panel requires an explicit package list.
5. Create an Apollo MySQL database and copy its host, port, database name,
   username, and password into the environment variables below.

## Required environment

```text
BOT_TOKEN=<Discord bot token>
DB_HOST=<Apollo database host>
DB_PORT=3306
DB_NAME=<Apollo database name>
DB_USER=<Apollo database user>
DB_PASSWORD=<Apollo database password>
ENV=production
```

Optional values are documented in `.env.example`. Do not upload `.env` or put
secrets in Git.

## Restarts and backups

The application does not call `os.execv` or restart itself. Configure planned
restarts through Apollo Schedules and use the panel's restart action. The bot
handles SIGTERM by cancelling tracked tasks, closing Discord, and exiting
cleanly.

Create a database backup before applying a release that contains a migration.
Migrations are recorded in `schema_migrations` and run during startup. A
failed migration is fatal so the bot cannot run against a partially known
schema.

## Release smoke test

After deployment, verify the console contains:

- validated environment configuration;
- every explicitly registered extension loaded;
- database schema current or migrations applied;
- command synchronization completed; and
- `Ready as ...`.

Then test `/ping`, `/embed`, destination-channel selection, image upload, and
publishing after one panel restart. Keep the private `image-storage` channel
and its messages; the database records the storage message IDs so deleted
assets can be reported instead of silently producing broken embeds.
