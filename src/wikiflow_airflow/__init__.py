"""Airflow-specific glue.

Separate from the `wikiflow` package on purpose. That package is deliberately free of
Airflow imports so its logic can be unit-tested without a scheduler - a rule enforced by
a test. Anything that genuinely needs Airflow types, like Asset definitions shared
between DAGs, lives here instead.

This is importable from DAGs because /opt/airflow/src is on PYTHONPATH. The dags folder
itself is NOT on sys.path under Airflow 3.3's bundle loader, so a shared module cannot
simply sit next to the DAGs.
"""
