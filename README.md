# SurveySync Connect

SurveySync Connect is a SurveyCTO → PostgreSQL synchronization platform that lets research teams authenticate to a SurveyCTO server, select a form, map it to a PostgreSQL table, and run a managed sync job with progress tracking.

**Project home**: https://surveysync-connect.example.com

## What this repo contains

- **Frontend (this repo)**: A Vite + React UI for configuring connections, mapping tables, and monitoring sync jobs.
- **Backend (API + worker)**: A service that handles SurveyCTO sessions, PostgreSQL connections, schema validation, and long-running sync jobs.

## Local development

### Frontend (UI)

```sh
npm install
npm run dev
```

The UI starts on `http://localhost:5173` by default.

### Backend (API + worker)

Run your backend service separately. The UI expects a REST API with the routes listed below. If you are running the API locally, make sure it is reachable (for example `http://localhost:4000`) and configure the frontend with `VITE_API_BASE_URL`.

## Environment variables

### Frontend

Create a `.env.local` file (or set environment variables in your shell):

```bash
VITE_API_BASE_URL=http://localhost:4000
VITE_APP_ENV=local
```

### Backend

These variables are expected by the API service (adjust as needed for your implementation):

```bash
# SurveyCTO
SURVEYCTO_SERVER=https://your-server.surveycto.com
SURVEYCTO_USERNAME=your-username
SURVEYCTO_PASSWORD=your-password

# PostgreSQL
PGHOST=localhost
PGPORT=5432
PGDATABASE=surveysync
PGUSER=surveysync
PGPASSWORD=surveysync
PGSSLMODE=prefer

# App
API_PORT=4000
SYNC_JOB_POLL_INTERVAL_MS=1000
```

## High-level architecture

1. **Frontend (React/Vite)** collects SurveyCTO credentials, form selection, and PostgreSQL targets.
2. **Backend API** authenticates SurveyCTO sessions, lists forms, and validates PostgreSQL schema compatibility.
3. **Sync worker** starts a sync job, pulls data from SurveyCTO, writes to PostgreSQL (insert or upsert), and emits progress updates.
4. **PostgreSQL** stores the synced form data in existing or newly created tables.

## Key API routes

The UI expects the following API endpoints:

### SurveyCTO
- `POST /api/sessions/surveycto` — authenticate and return available forms.
- `GET /api/surveycto/forms` — list forms for the current session.
- `GET /api/surveycto/forms/:formId` — fetch a single form with field metadata.

### PostgreSQL
- `POST /api/pg/connect` — test connection and return schemas.
- `GET /api/pg/schemas` — list schemas.
- `GET /api/pg/schemas/:schemaName/tables` — list tables for a schema.
- `POST /api/pg/validate-schema` — validate field compatibility and primary key mapping.
- `POST /api/pg/tables` — create a new table from SurveyCTO fields.

### Sync jobs
- `POST /api/sync-jobs` — start a sync job.
- `GET /api/sync-jobs/:jobId` — read sync progress.
- `DELETE /api/sync-jobs/:jobId` — cancel a running job.

## Sync flow (SurveyCTO → PostgreSQL)

1. **Authenticate** to SurveyCTO (`POST /api/sessions/surveycto`).
2. **Select a form** (`GET /api/surveycto/forms`).
3. **Connect to PostgreSQL** (`POST /api/pg/connect`).
4. **Choose target table** or create a new one (`GET /api/pg/schemas`, `GET /api/pg/schemas/:schemaName/tables`, `POST /api/pg/tables`).
5. **Validate schema compatibility** (`POST /api/pg/validate-schema`).
6. **Start sync** (`POST /api/sync-jobs`) with insert or upsert mode.
7. **Track progress** via polling (`GET /api/sync-jobs/:jobId`) until completion or cancellation (`DELETE /api/sync-jobs/:jobId`).

## Tech stack

- Vite
- React
- TypeScript
- shadcn-ui
- Tailwind CSS
