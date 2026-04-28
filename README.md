# Rainmaker Remodel — Cold Email Analytics Sync

Read-only sync script that pulls data from SmartLead + GHL into a Google Sheet.

## Read-Only Guarantee

This script only sends GET requests to SmartLead and GHL. It never creates, updates, or deletes anything in those platforms. Write operations are blocked at the HTTP client level — attempts to POST/PUT/PATCH/DELETE will throw an error before any request is made.

## One-Time Setup

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Put your credentials in place

**a. Service account JSON**
Drop your freshly-generated Google service account JSON file next to `sync.py` and name it `service-account.json` (or update `GOOGLE_CREDS_PATH` in `.env`).

**b. Environment variables**
Copy the template and fill it in:

```bash
cp .env.template .env
```

Then open `.env` and fill in:
- `SMARTLEAD_API_KEY` — from SmartLead Settings → API
- `GHL_PRIVATE_TOKEN` — from GHL Settings → Private Integrations
- `GHL_LOCATION_ID` — your GHL sub-account ID
- `GHL_PIPELINE_ID` — the pipeline ID for your cold email pipeline
- `GOOGLE_SHEET_ID` — already pre-filled for Rainmaker Remodel's sheet

### 3. Share the Google Sheet with the service account

1. Open the Google Sheet
2. Click Share
3. Paste the service account email (ends in `@*.iam.gserviceaccount.com`) — you can find it in the JSON file under `client_email`
4. Give it **Editor** access
5. Uncheck "Notify people"
6. Click Share

### 4. Finding your GHL Pipeline ID and Location ID

**Location ID:** Log into GHL → URL shows `/location/{locationId}/...` — copy that ID.

**Pipeline ID:** Settings → Pipelines → click your pipeline → the URL now contains `/pipelines/{pipelineId}` — copy that.

### 5. GHL Custom Fields (IMPORTANT)

The script reads these custom fields from GHL opportunities (set them up in your GHL automation):

- `campaign_id` — SmartLead campaign ID the lead came from
- `campaign_name` — SmartLead campaign name
- `smartlead_reply_timestamp` — when they replied in SmartLead (ISO format)
- `entered_inbound_reply_engaged_at`
- `entered_awaiting_response_at`
- `entered_day_1_followup_at`
- `entered_day_2_followup_at`
- `entered_call_proposed_at`
- `entered_call_booked_at`

Your GHL workflow should write to these fields when a contact enters each pipeline stage.

## Running

### Manually

```bash
python sync.py
```

### Scheduled (Mac / Linux)

Edit your crontab:

```bash
crontab -e
```

Add a line to run daily at 2 AM:

```
0 2 * * * cd /path/to/sync && /usr/bin/python3 sync.py >> sync.log 2>&1
```

### Scheduled (Windows)

Open Task Scheduler → Create Basic Task → trigger Daily → action: Start a program → program: `python.exe`, arguments: `C:\path\to\sync.py`, start in: `C:\path\to\sync\`.

## What gets synced

| Sheet Tab | Source | Data |
|---|---|---|
| Raw SmartLead Stats | SmartLead | Per-campaign totals (sent, opened, replied, bounced) |
| Raw SmartLead Replies | SmartLead | Every replied lead with reply text + SL category |
| Raw SmartLead Sequences | SmartLead | All email copy (subjects, bodies, CTAs) |
| Raw GHL Data | GHL | All opportunities with stage-entry timestamps |
| Config | (this script) | Last sync timestamps + status |

The Campaign Overview and Time Range Views tabs auto-calculate from the raw tabs.

## Troubleshooting

**"Service account JSON not found"** — check `GOOGLE_CREDS_PATH` in `.env` points to the correct file.

**"403 Permission denied" on Sheets** — you forgot to share the sheet with the service account email.

**"429 rate limit" in logs** — the script handles this automatically with exponential backoff.

**SmartLead returns unexpected JSON shape** — the script has fallbacks for different response shapes, but SmartLead occasionally changes field names. Check the logs and adjust the field names in `fetch_smartlead_stats()` if needed.

**No data in Raw GHL Data** — your GHL automation isn't writing the custom fields yet. Set those up first (see section 5 above).

## Scheduling upgrade path

When you're ready to move off your local machine:

1. **Google Cloud Functions** — deploy `sync.py` as a function, schedule with Cloud Scheduler
2. **Railway / Render** — push to a git repo, use their cron trigger
3. **GitHub Actions** — add a `.github/workflows/sync.yml` with a scheduled trigger

All three work with the same script — just move the env vars and service account JSON to the platform's secret storage.
