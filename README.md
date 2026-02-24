# Fine App — Backend

API backend for the Fine health & wellness app. Built with Python/FastAPI, deployed on Google Cloud Run.

**API URL:** `https://REDACTED`

---

## Running Tests

The `tests/` folder contains 4 test files (~77 tests) that check every API endpoint for correct behavior, bad inputs, auth failures, and security issues.

### Setup (one time)
```bash
cd ~/Documents/MaestroOS/backend
python3 -m venv .venv
source .venv/bin/activate
pip install pytest httpx
```

### Run all tests
```bash
cd ~/Documents/MaestroOS/backend
source .venv/bin/activate

export TEST_API_URL='https://REDACTED'
export TEST_USER_EMAIL='REDACTED'
export TEST_USER_PASSWORD='REDACTED'
export FIREBASE_API_KEY='REDACTED'

pytest tests/ -v --tb=short
```

> **Note:** Use single quotes for env vars to avoid shell issues with special characters like `!`

### Run a specific file
```bash
pytest tests/test_auth_fp.py -v
pytest tests/test_nutrition_quest.py -v
pytest tests/test_interests_analytics.py -v
pytest tests/test_security_edge.py -v
```

### Run a specific test category
```bash
pytest tests/ -v -k "Security"
pytest tests/ -v -k "Nutrition"
pytest tests/ -v -k "Auth"
pytest tests/ -v -k "Quest"
pytest tests/ -v -k "Strava"
```

### Test files
| File | Tests | Covers |
|------|-------|--------|
| test_auth_fp.py | 10 | Auth tokens, Fine Points |
| test_nutrition_quest.py | 19 | Meal logging, photo analysis, steps |
| test_interests_analytics.py | 21 | Topics, articles, analytics tracking |
| test_security_edge.py | 22 | XSS, injection, edge cases, Strava |

### When to run
- Before inviting new users
- After major backend changes
- When something feels broken

---

## Deploy
```bash
cd ~/Documents/MaestroOS/backend
gcloud run deploy maestro-api --source . --region europe-west1 --project REDACTED --allow-unauthenticated --port 8080
```
