# Jira Cloud connector

Read Jira Cloud issues into Spark on AIDP with `jira.py` (only `requests`).
`jira.py` imports the sibling `connectors/_shared/` package (HTTP retry
engine, credential resolution) — upload both to the same workspace folder.

Setup: see the example notebook in `examples/` and `LIVE_TEST_GUIDE.md`.
Requirements and acceptance criteria: `REQUIREMENTS.md`. Verified API behaviour: `spike/RESULTS.md`.
Live-test status: `live-results/RESULTS.md`. Not supported until a PASS row exists there.

Running the live AIDP test yourself, or handing it to someone else? See
[`LIVE_TEST_GUIDE.md`](LIVE_TEST_GUIDE.md) — self-contained, no other context needed.

## Test

    pip install -r ../../requirements-dev.txt
    pytest -q
