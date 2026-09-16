"""wikiflow - incremental ingestion of Wikimedia hourly pageviews.

Everything in this package is deliberately free of Airflow imports. DAGs under dags/
are thin wrappers that call into here; all real logic lives in plain, importable
modules. That separation is what lets the unit tests run in milliseconds on Windows
without installing Airflow at all, and it makes the logic testable independently of
the orchestrator.
"""

__version__ = "0.1.0"
