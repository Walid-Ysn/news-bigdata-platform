"""
Gouvernance des données — Traçabilité et documentation du pipeline
Fonctionnalités :
  - Data lineage (traçabilité source → bronze → silver → gold → DW)
  - Catalogue de métadonnées
  - Logging des opérations
  - Journal de gouvernance
"""

import json
import logging
import os
import sqlite3
import uuid
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger("Governance")

GOVERNANCE_DB = os.getenv("GOVERNANCE_DB", "/tmp/governance.db")


# ─────────────────────────────────────────────
# Governance Database
# ─────────────────────────────────────────────

class GovernanceDB:
    def __init__(self, db_path: str = GOVERNANCE_DB):
        self.db_path = db_path
        self._conn = None

    def _connect(self):
        if not self._conn:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._init_schema()
        return self._conn

    def _init_schema(self):
        stmts = [
            """CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id       TEXT PRIMARY KEY,
                dag_id       TEXT,
                start_time   TEXT,
                end_time     TEXT,
                status       TEXT,
                articles_in  INTEGER,
                articles_out INTEGER,
                notes        TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS lineage_events (
                event_id     TEXT PRIMARY KEY,
                run_id       TEXT,
                timestamp    TEXT,
                layer        TEXT,
                operation    TEXT,
                input_ref    TEXT,
                output_ref   TEXT,
                record_count INTEGER,
                metadata     TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS data_catalog (
                dataset_id   TEXT PRIMARY KEY,
                name         TEXT,
                layer        TEXT,
                description  TEXT,
                schema_json  TEXT,
                owner        TEXT,
                created_at   TEXT,
                updated_at   TEXT,
                record_count INTEGER,
                tags         TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS quality_log (
                log_id       TEXT PRIMARY KEY,
                run_id       TEXT,
                checked_at   TEXT,
                total        INTEGER,
                passed       INTEGER,
                score        REAL,
                report_json  TEXT
            )""",
        ]
        conn = self._conn
        for stmt in stmts:
            conn.execute(stmt)
        conn.commit()

    def execute(self, sql: str, params=None):
        conn = self._connect()
        conn.execute(sql, params or [])
        conn.commit()

    def query(self, sql: str, params=None) -> List[Dict]:
        conn = self._connect()
        cur = conn.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ─────────────────────────────────────────────
# Pipeline Run Tracker
# ─────────────────────────────────────────────

class PipelineRun:
    def __init__(self, dag_id: str = "news_pipeline_batch", db: GovernanceDB = None):
        self.run_id = str(uuid.uuid4())
        self.dag_id = dag_id
        self.start_time = datetime.utcnow().isoformat()
        self.db = db or GovernanceDB()
        self._register()

    def _register(self):
        self.db.execute(
            "INSERT INTO pipeline_runs (run_id, dag_id, start_time, status) VALUES (?,?,?,?)",
            [self.run_id, self.dag_id, self.start_time, "running"]
        )
        logger.info(f"[Governance] Pipeline run started: {self.run_id}")

    def complete(self, articles_in: int = 0, articles_out: int = 0, notes: str = ""):
        end_time = datetime.utcnow().isoformat()
        self.db.execute(
            """UPDATE pipeline_runs
               SET end_time=?, status=?, articles_in=?, articles_out=?, notes=?
               WHERE run_id=?""",
            [end_time, "success", articles_in, articles_out, notes, self.run_id]
        )
        logger.info(f"[Governance] Run {self.run_id} completed: {articles_out} articles out")

    def fail(self, error: str):
        self.db.execute(
            "UPDATE pipeline_runs SET end_time=?, status=?, notes=? WHERE run_id=?",
            [datetime.utcnow().isoformat(), "failed", str(error)[:500], self.run_id]
        )
        logger.error(f"[Governance] Run {self.run_id} failed: {error}")


# ─────────────────────────────────────────────
# Data Lineage
# ─────────────────────────────────────────────

class DataLineage:
    def __init__(self, run_id: str, db: GovernanceDB = None):
        self.run_id = run_id
        self.db = db or GovernanceDB()

    def log(
        self,
        layer: str,
        operation: str,
        input_ref: str,
        output_ref: str,
        record_count: int,
        metadata: Dict = None,
    ):
        self.db.execute(
            """INSERT INTO lineage_events
               (event_id, run_id, timestamp, layer, operation, input_ref, output_ref, record_count, metadata)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            [
                str(uuid.uuid4()), self.run_id, datetime.utcnow().isoformat(),
                layer, operation, input_ref, output_ref, record_count,
                json.dumps(metadata or {}),
            ]
        )
        logger.info(f"[Lineage] {layer}.{operation}: {input_ref} → {output_ref} ({record_count} records)")

    def get_lineage(self) -> List[Dict]:
        return self.db.query(
            "SELECT * FROM lineage_events WHERE run_id=? ORDER BY timestamp",
            [self.run_id]
        )


# ─────────────────────────────────────────────
# Data Catalog
# ─────────────────────────────────────────────

CATALOG_ENTRIES = [
    {
        "dataset_id": "bronze_articles",
        "name": "Articles Bruts (Bronze)",
        "layer": "bronze",
        "description": "Articles de presse collectés par les scrapers, sans transformation. Stockés en JSON dans MinIO.",
        "schema_json": json.dumps({
            "article_id": "string - MD5 de l'URL",
            "title": "string - Titre de l'article",
            "author": "string|null - Auteur",
            "published_at": "string|null - Date de publication ISO 8601",
            "category": "string|null - Catégorie éditoriale",
            "content": "string - Contenu brut (peut contenir du HTML)",
            "source": "string - Nom du site source",
            "url": "string - URL de l'article",
            "language": "string|null - Code langue déclaré",
            "scraped_at": "string - Timestamp du scraping ISO 8601",
            "country": "string - Code pays ISO 2",
        }),
        "owner": "iadata-pipeline",
        "tags": "bronze,raw,scraping",
    },
    {
        "dataset_id": "silver_articles",
        "name": "Articles Nettoyés (Silver)",
        "layer": "silver",
        "description": "Articles nettoyés : HTML supprimé, texte normalisé, langue détectée, date parsée, doublons supprimés.",
        "schema_json": json.dumps({
            "article_id": "string",
            "title": "string - Titre nettoyé",
            "author": "string|null",
            "published_at": "string|null - ISO 8601 normalisé",
            "category": "string",
            "content": "string - Contenu texte pur",
            "content_length": "integer - Nombre de caractères",
            "source": "string",
            "url": "string",
            "language": "string - Code langue détecté",
            "country": "string",
            "scraped_at": "string",
            "processed_at": "string - Timestamp traitement Silver",
            "layer": "string - 'silver'",
        }),
        "owner": "iadata-pipeline",
        "tags": "silver,cleaned,normalized",
    },
    {
        "dataset_id": "gold_articles_per_source",
        "name": "Articles par Source (Gold)",
        "layer": "gold",
        "description": "Nombre d'articles agrégés par source de presse.",
        "schema_json": json.dumps({
            "source": "string",
            "article_count": "integer",
            "computed_at": "string",
        }),
        "owner": "iadata-analytics",
        "tags": "gold,aggregation,source",
    },
    {
        "dataset_id": "gold_top_keywords",
        "name": "Top Mots-Clés (Gold)",
        "layer": "gold",
        "description": "Les mots-clés les plus fréquents dans les articles collectés.",
        "schema_json": json.dumps({
            "keyword": "string",
            "frequency": "integer",
            "computed_at": "string",
        }),
        "owner": "iadata-analytics",
        "tags": "gold,nlp,keywords,trends",
    },
    {
        "dataset_id": "gold_news_trends",
        "name": "Tendances Actualité (Gold)",
        "layer": "gold",
        "description": "Top catégories d'actualité avec exemples de titres et sources.",
        "schema_json": json.dumps({
            "category": "string",
            "article_count": "integer",
            "sample_titles": "array[string]",
            "sources": "array[string]",
            "computed_at": "string",
        }),
        "owner": "iadata-analytics",
        "tags": "gold,trends,news",
    },
]


class DataCatalog:
    def __init__(self, db: GovernanceDB = None):
        self.db = db or GovernanceDB()
        self._seed()

    def _seed(self):
        now = datetime.utcnow().isoformat()
        for entry in CATALOG_ENTRIES:
            self.db.execute(
                """INSERT OR IGNORE INTO data_catalog
                   (dataset_id, name, layer, description, schema_json, owner, created_at, updated_at, tags)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                [
                    entry["dataset_id"], entry["name"], entry["layer"],
                    entry["description"], entry["schema_json"],
                    entry["owner"], now, now, entry["tags"],
                ]
            )

    def update_count(self, dataset_id: str, count: int):
        self.db.execute(
            "UPDATE data_catalog SET record_count=?, updated_at=? WHERE dataset_id=?",
            [count, datetime.utcnow().isoformat(), dataset_id]
        )

    def list_datasets(self) -> List[Dict]:
        return self.db.query("SELECT * FROM data_catalog ORDER BY layer, name")

    def get_dataset(self, dataset_id: str) -> Optional[Dict]:
        rows = self.db.query("SELECT * FROM data_catalog WHERE dataset_id=?", [dataset_id])
        return rows[0] if rows else None

    def print_catalog(self):
        datasets = self.list_datasets()
        print(f"\n{'=' * 60}")
        print(f"CATALOGUE DE DONNÉES ({len(datasets)} datasets)")
        print(f"{'=' * 60}")
        for ds in datasets:
            print(f"\n[{ds['layer'].upper()}] {ds['name']}")
            print(f"  ID          : {ds['dataset_id']}")
            print(f"  Description : {ds['description'][:80]}...")
            print(f"  Owner       : {ds['owner']}")
            print(f"  Tags        : {ds['tags']}")
            if ds.get("record_count"):
                print(f"  Records     : {ds['record_count']:,}")


# ─────────────────────────────────────────────
# Quality Log
# ─────────────────────────────────────────────

class QualityLogger:
    def __init__(self, db: GovernanceDB = None):
        self.db = db or GovernanceDB()

    def log(self, run_id: str, report: Dict):
        summary = report.get("summary", {})
        self.db.execute(
            "INSERT INTO quality_log (log_id, run_id, checked_at, total, passed, score, report_json) VALUES (?,?,?,?,?,?,?)",
            [
                str(uuid.uuid4()), run_id, summary.get("checked_at", datetime.utcnow().isoformat()),
                summary.get("total", 0), summary.get("passed", 0),
                summary.get("score", 0), json.dumps(report),
            ]
        )

    def get_history(self, limit: int = 10) -> List[Dict]:
        return self.db.query(
            "SELECT log_id, run_id, checked_at, total, passed, score FROM quality_log ORDER BY checked_at DESC LIMIT ?",
            [limit]
        )


if __name__ == "__main__":
    db = GovernanceDB()
    run = PipelineRun(db=db)
    lineage = DataLineage(run.run_id, db=db)

    lineage.log("bronze", "scrape_and_store", "web:hespress.com", "minio://news-datalake/bronze/", 42)
    lineage.log("silver", "transform", "minio://news-datalake/bronze/", "minio://news-datalake/silver/", 38)
    lineage.log("gold", "aggregate", "minio://news-datalake/silver/", "postgres://news_dw/gold_*", 38)

    run.complete(articles_in=42, articles_out=38, notes="Test run OK")

    catalog = DataCatalog(db=db)
    catalog.print_catalog()

    print("\nLineage events:")
    for event in lineage.get_lineage():
        print(f"  {event['layer']}.{event['operation']}: {event['input_ref']} → {event['output_ref']}")
