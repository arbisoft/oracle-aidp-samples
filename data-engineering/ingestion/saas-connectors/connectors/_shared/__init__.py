"""Shared infrastructure for every connector in this repo.

Uploaded once to an AIDP workspace path alongside each connector's own
module (e.g. ``jira.py``), following the same pattern Oracle's own
`oracle-ai-data-platform-workbench-spark-connectors` plugin uses: one shared
package, referenced by every connector-specific file via ``sys.path``.
"""
