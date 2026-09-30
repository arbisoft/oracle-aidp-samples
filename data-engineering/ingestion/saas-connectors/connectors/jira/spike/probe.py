"""THROWAWAY spike probe. Not part of the shipped connector.

Answers the spike questions in connectors/jira/REQUIREMENTS.md against a real
site. Run from the repo root with .env set (see spike/_env.py):

    PYTHONPATH=connectors/jira:connectors/jira/spike python connectors/jira/spike/probe.py

Prints structure only. Never prints the site, email or token.
"""

import _env
_env.load(".env")

import jira as j

SITE, EMAIL, TOKEN = j.credentials_from_env()
SESSION = j.jira_session(EMAIL, TOKEN)
URL = "https://%s/rest/api/3/search/jql" % SITE


def call(**body):
    return SESSION.post(URL, json=body, timeout=60)


BOUND = 'updated >= "2000-01-01 00:00"'  # Jira rejects a truly unbounded JQL query


def q1_shape_and_paging(jql=BOUND + " order by updated asc", page=3, max_pages=200):
    seen, token, pages = [], None, 0
    prev_token = object()
    while pages < max_pages:
        body = {"jql": jql, "maxResults": page, "fields": ["key"]}
        if token:
            body["nextPageToken"] = token
        r = call(**body)
        if r.status_code != 200:
            print("Q1 FAIL: HTTP", r.status_code, r.text[:300])
            return
        payload = r.json()
        keys = [i["key"] for i in payload.get("issues", [])]
        seen += keys
        pages += 1
        is_last = payload.get("isLast")
        next_token = payload.get("nextPageToken")
        print("Q1 page=%d keys=%d isLast=%s has_token=%s" % (pages, len(keys), is_last, bool(next_token)))
        if is_last or not next_token:
            break
        if next_token == prev_token:
            print("Q1 FAIL: token did not advance")
            return
        prev_token = token
        token = next_token
    print("Q1 total=%d unique=%d" % (len(seen), len(set(seen))))


def q2_max_results():
    for n in (5000, 5001):
        r = call(jql=BOUND + " order by updated asc", maxResults=n, fields=["key"])
        print("Q2 requested=%d status=%d body=%s" % (n, r.status_code, r.text[:150]))
    r = call(jql=BOUND + " order by updated asc", maxResults=1, fields=["key"])
    interesting = sorted(
        (k, v) for k, v in r.headers.items()
        if k.lower().startswith(("x-ratelimit", "retry-after"))
    )
    print("Q2 headers:", interesting)


def q3_field_shapes():
    r = call(jql=BOUND + " order by updated desc", maxResults=1,
             fields=["key", "assignee", "reporter", "updated", "created", "status", "priority"])
    if r.status_code != 200 or not r.json().get("issues"):
        print("Q3 FAIL or no issues:", r.status_code)
        return
    issue = r.json()["issues"][0]
    fields = issue.get("fields", {})
    shape = {k: (type(v).__name__, sorted(v) if isinstance(v, dict) else None) for k, v in fields.items()}
    print("Q3 field shapes:", shape)
    print("Q3 updated raw value:", fields.get("updated"))
    import datetime as dt
    print("Q3 utc_now:", dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))


def q4_invalid_jql():
    r = call(jql="this is not valid jql at all !!!", maxResults=1)
    print("Q4 invalid jql -> HTTP", r.status_code, r.text[:300])


def q5_custom_fields():
    r = call(jql=BOUND + " order by updated desc", maxResults=1, fields=["*all"])
    if r.status_code != 200 or not r.json().get("issues"):
        print("Q5 FAIL or no issues:", r.status_code)
        return
    fields = r.json()["issues"][0].get("fields", {})
    custom = sorted(k for k in fields if k.startswith("customfield_"))
    print("Q5 custom field keys present:", custom[:10], "total:", len(custom))


if __name__ == "__main__":
    q1_shape_and_paging()
    q2_max_results()
    q3_field_shapes()
    q4_invalid_jql()
    q5_custom_fields()
