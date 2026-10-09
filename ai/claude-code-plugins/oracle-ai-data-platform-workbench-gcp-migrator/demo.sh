#!/usr/bin/env bash
# Offline demo of gcp-aidp. No Google Cloud or OCI credentials needed.
# Run from the plugin directory: ./demo.sh
set -euo pipefail
cd "$(dirname "$0")"

# The output folder is wiped first. An OUT you set is wiped only if it is empty
# or holds an earlier demo, so pointing it at a real folder cannot delete it.
if [ -n "${OUT:-}" ] && [ -n "$(ls -A "$OUT" 2>/dev/null)" ] && [ ! -e "$OUT/.gcp-aidp-demo" ]; then
  echo "demo.sh: OUT=$OUT is not empty and holds no earlier demo; set OUT to a new folder" >&2
  exit 1
fi
OUT="${OUT:-/tmp/gcp-aidp-demo}"
rm -rf "$OUT" && mkdir -p "$OUT" && touch "$OUT/.gcp-aidp-demo"
export OCI_NAMESPACE="${OCI_NAMESPACE:-acme-demo-ns}"

bar() { printf '\n%s\n' "════════════════════════════════════════════════════════════"; }
show() {  # show <asset_id> <field>
  python3 -c "import json,sys; r=[x for x in json.load(open('$OUT/migrated/report.json'))['results'] if x['asset_id']==sys.argv[1]][0]; print(r[sys.argv[2]])" "$1" "$2"
}

bar; echo "  1/4  INVENTORY — scan Google Cloud (fixture: Northwind Retail)"; bar
python3 -m gcp_aidp.cli inventory --fixture demo -o "$OUT/inventory.json"

bar; echo "  2/4  PLAN — manifest → mapping plan + approval document"; bar
python3 -m gcp_aidp.cli plan "$OUT/inventory.json" -o "$OUT/plan.json"

bar; echo "  3/4  MIGRATE — translated artifacts, written locally only"; bar
python3 -m gcp_aidp.cli migrate "$OUT/plan.json" -o "$OUT/migrated" | tail -3

bar; echo "  4/4  VERIFY"; bar
python3 -m gcp_aidp.cli verify "$OUT/migrated" | head -11

bar; echo "  A view with rewrites (v_order_kpis): BEFORE (GoogleSQL)"; bar
show bigquery.view.sales.v_order_kpis source_sql
bar; echo "  AFTER (Spark SQL on AIDP) — REVIEW: two rewrites carry a caveat"; bar
cat "$OUT/migrated/views/sales.v_order_kpis.sql"

bar; echo "  A blocked view (v_all_app_events) — never partially translated"; bar
cat "$OUT/migrated/views/logs.v_all_app_events.sql"

bar; echo "  Artifacts at $OUT/"; bar
echo "  → $OUT/plan.md                (approval document)"
echo "  → $OUT/migrated/report.md     (migration report, before/after per asset)"

bar; echo "  PUBLISH (dry run: contacts nothing)"; bar
python3 -m gcp_aidp.cli publish "$OUT/migrated" --prefix demo --cluster-key "<your-cluster-key>" | sed -n '1,3p;/and create/,$p'

