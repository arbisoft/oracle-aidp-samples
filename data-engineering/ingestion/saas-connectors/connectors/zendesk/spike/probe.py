"""THROWAWAY spike probe. Not part of the shipped connector.

Answers the spike questions in connectors/zendesk/REQUIREMENTS.md against a
real site. Run from the repo root with .env set (see spike/_env.py):

    PYTHONPATH=connectors/zendesk:connectors/_shared:connectors/zendesk/spike \
        python connectors/zendesk/spike/probe.py

Prints structure only. Never prints the subdomain, email or token.
"""

import time

import _env
_env.load(".env")

import zendesk as z

SUBDOMAIN, EMAIL, TOKEN = z.credentials_from_env()
SESSION = z.zendesk_session(EMAIL, TOKEN)
URL = "https://{}.zendesk.com/api/v2/incremental/tickets/cursor.json".format(SUBDOMAIN)


def call(**params):
    return SESSION.get(URL, params=params, timeout=30)


def q1_shape_and_paging(per_page=2, max_pages=50):
    seen, cursor, pages = [], None, 0
    params = {"start_time": 0, "per_page": per_page}
    while pages < max_pages:
        if cursor:
            params = {"cursor": cursor, "per_page": per_page}
        r = call(**params)
        if r.status_code != 200:
            print("Q1 FAIL: HTTP", r.status_code, r.text[:300])
            return
        payload = r.json()
        ids = [t["id"] for t in payload.get("tickets", [])]
        seen += ids
        pages += 1
        end = payload.get("end_of_stream")
        next_cursor = payload.get("after_cursor")
        print("Q1 page=%d tickets=%d end_of_stream=%s has_cursor=%s" % (
            pages, len(ids), end, bool(next_cursor)))
        if end:
            print("Q1 total=%d unique=%d final_cursor_present=%s" % (
                len(seen), len(set(seen)), bool(next_cursor)))
            return next_cursor
        cursor = next_cursor
    print("Q1 FAIL: did not reach end_of_stream within", max_pages, "pages")


def q2_rate_limit_headers():
    r = call(start_time=0, per_page=1)
    interesting = sorted(
        (k, v) for k, v in r.headers.items()
        if "ratelimit" in k.lower() or k.lower() == "retry-after"
    )
    print("Q2 status=%d headers=%s" % (r.status_code, interesting))


def q3_field_shapes():
    r = call(start_time=0, per_page=1)
    if r.status_code != 200 or not r.json().get("tickets"):
        print("Q3 FAIL or no tickets:", r.status_code)
        return
    ticket = r.json()["tickets"][0]
    shape = {k: type(v).__name__ for k, v in ticket.items()}
    print("Q3 field shapes:", shape)
    print("Q3 updated_at raw value:", ticket.get("updated_at"))
    print("Q3 custom_fields raw value:", ticket.get("custom_fields"))
    print("Q3 requester_id type:", type(ticket.get("requester_id")).__name__)


def q4_invalid_params():
    r = call(cursor="not-a-real-cursor")
    print("Q4 invalid cursor -> HTTP", r.status_code, r.text[:300])


def q5_incremental_resume(prior_cursor):
    if not prior_cursor:
        print("Q5 SKIPPED: no cursor from Q1 to resume from")
        return
    time.sleep(2)
    r = call(cursor=prior_cursor, per_page=10)
    if r.status_code != 200:
        print("Q5 FAIL: HTTP", r.status_code, r.text[:300])
        return
    payload = r.json()
    print("Q5 resumed with prior cursor -> tickets=%d end_of_stream=%s" % (
        len(payload.get("tickets", [])), payload.get("end_of_stream")))


if __name__ == "__main__":
    final_cursor = q1_shape_and_paging()
    q2_rate_limit_headers()
    q3_field_shapes()
    q4_invalid_params()
    q5_incremental_resume(final_cursor)
