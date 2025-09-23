# FastAPI Layer for DRP Email Processing

This service wraps the **object-oriented** processing you now have in `email_workflow` and exposes a simple HTTP API.

## Endpoints

- `GET /health` — health check
- `POST /process-emails` — run the pipeline

### Request (application/json)

```json
{
  "emails": [
    {"date": "2025-08-08T10:20:00Z", "subject": "Hej", "body": "Text...", "sender": "a@b.com"}
  ],
  "errands": [
    {"errandId": 123, "date": "2025-08-07T00:00:00Z", "reference": "ABC123"}
  ],
  "do_categorize": true,
  "do_enrich_staff_animal": false
}
```

### Response

```json
{
  "rows": [
    {"date": "...", "category": "Settlement_Approved", "errandId": [123], "...": "..."}
  ]
}
```

## Run locally

1. Make sure **your project** can import the two packages:
   - `email_workflow` (from the *refactored_oop* zip you got)
   - Your existing `oop` package (Processor/Detector/Extractor/Connector/Category…).

   The simplest way is to put both directories on `PYTHONPATH`, or install them in your venv.

2. Install deps:

```bash
pip install -r requirements.txt
```

3. Start the server:

```bash
uvicorn app.main:app --reload --port 8000
```

4. Open docs:
   - Swagger UI: http://127.0.0.1:5000/docs
   - ReDoc: http://127.0.0.1:5000/redoc

## Notes

- The service expects **JSON** payloads (list of dicts) for both `emails` and optional `errands`.
- If you prefer CSV uploads (multipart/form-data), we can add another endpoint to accept files and parse them into DataFrames.
- The business logic runs exactly the same as your OOP pipeline. The API layer is intentionally thin.
