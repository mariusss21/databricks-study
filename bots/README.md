# Bots backup

This folder holds the setup of every Grok Bot assistant I use, so I can rebuild them on another account with no friction.

Each file covers one bot: its name, purpose, full instructions (persona), schedule, where it saves its work, and how to restore it.

## Bots
| Bot | Job | File |
|---|---|---|
| Grok Bot | Main assistant; sets up and coordinates the other bots | [grok-bot.md](grok-bot.md) |
| Databricks Study Coach | Study plan for the Databricks Data Engineer Associate exam | [databricks-study-coach.md](databricks-study-coach.md) |

## How to restore on a new account
1. Create a new bot with the same name.
2. Paste the **Instructions** section of its file as the bot's description/persona.
3. Ask it to recreate the routines listed under **Schedule**.
4. Point it at this repo (`mariusss21/databricks-study`) and at its progress notes so it picks up where it left off.
