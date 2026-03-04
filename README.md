# Power BI Teams Bot

A Microsoft Teams bot that lets users query Power BI datasets using natural language.
It uses OpenAI (or Azure OpenAI) function calling to translate questions into DAX, executes
them against the Power BI REST API, and returns the results as Markdown tables — all in Spanish.

---

## Architecture

```
Teams User
   │
   ▼
Bot Framework (POST /api/messages)
   │
   ▼
PowerBIBot (bot.py)          ← typing indicator, message splitting
   │
   ▼
AIAgent (ai_agent.py)        ← OpenAI function-calling loop (max 5 iterations)
   │  └─ list_available_datasets
   │  └─ get_dataset_schema
   │  └─ execute_dax_query
   │
   ▼
PowerBIClient (powerbi_client.py)  ← MSAL client-credentials token, httpx
   │
   ▼
Power BI REST API  (executeQueries endpoint)
```

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Python 3.11+ | |
| Azure Bot registration | For `APP_ID` / `APP_PASSWORD` |
| OpenAI API key **or** Azure OpenAI resource | |
| Power BI service principal | Needs Power BI workspace **Member** or **Contributor** role |
| Ngrok / Azure App Service / any HTTPS host | For the Bot Framework messaging endpoint |

---

## Setup

### 1. Clone and install dependencies

```bash
cd powerbi-teams-bot
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your actual values
```

#### Standard OpenAI

```dotenv
APP_ID=<azure-bot-app-id>
APP_PASSWORD=<azure-bot-app-password>
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o
USE_AZURE_OPENAI=false
PBI_TENANT_ID=<your-azure-tenant-id>
PBI_CLIENT_ID=<service-principal-client-id>
PBI_CLIENT_SECRET=<service-principal-client-secret>
```

#### Azure OpenAI

```dotenv
APP_ID=<azure-bot-app-id>
APP_PASSWORD=<azure-bot-app-password>
USE_AZURE_OPENAI=true
AZURE_OPENAI_ENDPOINT=https://<resource>.openai.azure.com/
AZURE_OPENAI_API_KEY=<key>
AZURE_OPENAI_DEPLOYMENT=gpt-4o
PBI_TENANT_ID=<your-azure-tenant-id>
PBI_CLIENT_ID=<service-principal-client-id>
PBI_CLIENT_SECRET=<service-principal-client-secret>
```

### 3. Add Power BI datasets

Edit `datasets_config.json` to add or modify datasets. Each entry requires:

- `name` — display name (used by the AI to look up datasets)
- `workspace_id` — the Power BI workspace (group) GUID
- `dataset_id` — the dataset GUID
- `description` — short description for the AI
- `schema.tables` — table names, columns, and types for DAX generation
- `schema.relationships` — foreign-key relationships between tables

### 4. Run locally

```bash
python app.py
# Server starts on http://0.0.0.0:3978
```

Use **ngrok** to expose it publicly:

```bash
ngrok http 3978
```

Set the **Messaging endpoint** in your Azure Bot registration to:
```
https://<ngrok-id>.ngrok.io/api/messages
```

### 5. Package for Teams

1. Add `color.png` (192×192 px) and `outline.png` (32×32 px, white/transparent) to `appPackage/`.
2. Replace `{{APP_ID}}` in `appPackage/manifest.json` with your actual Bot App ID.
3. Zip the `appPackage/` folder contents and upload to Teams as a custom app.

---

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/messages` | Bot Framework messaging endpoint |
| `GET` | `/health` | Liveness probe — returns `{"status":"ok"}` |

---

## Adding more datasets

1. Find the workspace GUID: Power BI Portal → Workspace → URL contains `/groups/<GUID>/`
2. Find the dataset GUID: Settings → Datasets → URL contains `datasets/<GUID>`
3. Add an entry to `datasets_config.json` with the schema (table names + column names/types).

---

## Conversation commands

| Command | Action |
|---------|--------|
| `ayuda` | Shows help message |
| `datasets` | Lists available datasets (handled by AI agent) |
| Any question | AI translates to DAX and queries Power BI |

---

## Power BI Service Principal permissions

The service principal (`PBI_CLIENT_ID`) must be:

1. Added to the Power BI workspace as **Member** or **Contributor**.
2. Tenant admin must enable *"Allow service principals to use Power BI APIs"* in the Power BI Admin Portal.

---

## File structure

```
powerbi-teams-bot/
├── app.py                  # aiohttp entry point
├── bot.py                  # Teams ActivityHandler
├── ai_agent.py             # OpenAI function-calling loop
├── powerbi_client.py       # Power BI REST API + MSAL auth
├── config.py               # Environment config loader
├── datasets_config.json    # Dataset registry + schemas
├── requirements.txt
├── .env.example
└── appPackage/
    ├── manifest.json       # Teams app manifest v1.16
    ├── color.png           # (add manually: 192×192 px)
    └── outline.png         # (add manually: 32×32 px)
```
