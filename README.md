# Employee Attendance & Analytics API

FastAPI + MongoDB (6.0+). All application code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
# MONGO_URI / MONGO_DB from the environment (a local .env also works; real env vars win)
uvicorn app.main:app --port 8000
```
Indexes are created at startup (idempotent). If MongoDB is down at startup, `/health` returns 503 and creates the indexes on the first successful ping.

## Notes
- Instants are epoch milliseconds in the API and UTC BSON dates in MongoDB; the client uses `tz_aware=True`.
- Rounding is half-up via `Decimal` everywhere a rule asks for it (R4, R8).
- Trend range: more than 92 calendar days (inclusive) is rejected with 422.
- `moving_avg_7d` averages the unrounded daily rates, then rounds to 4 decimals.
