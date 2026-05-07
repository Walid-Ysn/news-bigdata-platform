"""
Apache Airflow DAG — Orchestration du pipeline complet
Pipeline : Scraping → Ingestion Batch → Bronze → Silver → Gold → DW

Schedule : toutes les heures (batch)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging
import json
import sys
import os

# Ajout du path projet
sys.path.insert(0, "/opt/airflow/project")

logger = logging.getLogger("NewsDAG")

# ─────────────────────────────────────────────
# Default args
# ─────────────────────────────────────────────
default_args = {
    "owner": "iadata",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

# ─────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────
with DAG(
    dag_id="news_pipeline_batch",
    description="Pipeline de collecte et traitement d'articles de presse",
    default_args=default_args,
    schedule_interval="0 * * * *",   # Toutes les heures
    catchup=False,
    max_active_runs=1,
    tags=["news", "bigdata", "medallion", "iadata"],
) as dag:

    # ── Task 1 : Scraping ──────────────────────────────
    def task_scrape(**context):
        from scrapers.scraper import ScraperManager
        manager = ScraperManager(max_per_source=20)
        articles = manager.run_all()
        raw_data = [a.to_dict() for a in articles]

        # Push vers XCom pour la tâche suivante
        context["ti"].xcom_push(key="raw_articles", value=raw_data)
        logger.info(f"Scraped {len(raw_data)} articles total")
        return len(raw_data)

    # ── Task 2 : Ingestion Batch → MinIO ──────────────
    def task_ingest_batch(**context):
        from ingestion.ingestion import BatchIngestion
        raw_data = context["ti"].xcom_pull(key="raw_articles", task_ids="scrape")
        if not raw_data:
            logger.warning("No articles to ingest")
            return 0

        batch = BatchIngestion()
        path = batch.ingest(raw_data)
        context["ti"].xcom_push(key="bronze_path", value=path)
        logger.info(f"Batch ingested {len(raw_data)} articles → {path}")
        return len(raw_data)

    # ── Task 3 : Ingestion Streaming → Kafka ──────────
    def task_ingest_streaming(**context):
        from ingestion.ingestion import StreamingIngestion
        raw_data = context["ti"].xcom_pull(key="raw_articles", task_ids="scrape")
        if not raw_data:
            return 0

        streaming = StreamingIngestion()
        count = streaming.publish_batch(raw_data)
        streaming.close()
        logger.info(f"Published {count} articles to Kafka")
        return count

    # ── Task 4 : Bronze Layer ──────────────────────────
    def task_bronze(**context):
        from medallion.bronze.bronze_layer import BronzeLayer
        raw_data = context["ti"].xcom_pull(key="raw_articles", task_ids="scrape")
        if not raw_data:
            return {}

        bronze = BronzeLayer()
        paths = bronze.store(raw_data)
        context["ti"].xcom_push(key="bronze_paths", value=paths)
        logger.info(f"Bronze stored: {paths}")
        return paths

    # ── Task 5 : Silver Layer ──────────────────────────
    def task_silver(**context):
        from medallion.silver.silver_layer import SilverLayer
        raw_data = context["ti"].xcom_pull(key="raw_articles", task_ids="scrape")
        if not raw_data:
            return []

        silver = SilverLayer()
        silver_articles = silver.transform_batch(raw_data)

        # Sauvegarde locale temporaire pour Gold
        out_path = f"/tmp/silver_{context['ds_nodash']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(silver_articles, f, ensure_ascii=False)

        context["ti"].xcom_push(key="silver_path", value=out_path)
        context["ti"].xcom_push(key="silver_count", value=len(silver_articles))
        logger.info(f"Silver: {len(silver_articles)} articles processed")
        return len(silver_articles)

    # ── Task 6 : Transformation / Enrichissement ───────
    def task_transform(**context):
        from transformation.transform import ArticleTransformer
        silver_path = context["ti"].xcom_pull(key="silver_path", task_ids="silver")
        if not silver_path or not os.path.exists(silver_path):
            return 0

        with open(silver_path, encoding="utf-8") as f:
            silver_articles = json.load(f)

        transformer = ArticleTransformer()
        enriched = transformer.transform_batch(silver_articles)

        enriched_path = f"/tmp/enriched_{context['ds_nodash']}.json"
        with open(enriched_path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)

        context["ti"].xcom_push(key="enriched_path", value=enriched_path)
        logger.info(f"Transformed {len(enriched)} articles")
        return len(enriched)

    # ── Task 7 : Gold Layer ───────────────────────────
    def task_gold(**context):
        from medallion.gold.gold_layer import GoldLayer
        enriched_path = context["ti"].xcom_pull(key="enriched_path", task_ids="transform")
        if not enriched_path or not os.path.exists(enriched_path):
            return {}

        with open(enriched_path, encoding="utf-8") as f:
            articles = json.load(f)

        gold = GoldLayer()
        gold_data = gold.compute_all(articles)

        output_dir = f"/tmp/gold_{context['ds_nodash']}"
        gold.save_all(gold_data, output_dir)
        context["ti"].xcom_push(key="gold_dir", value=output_dir)
        logger.info(f"Gold computed: {list(gold_data.keys())}")
        return output_dir

    # ── Task 8 : Chargement Data Warehouse ────────────
    def task_load_warehouse(**context):
        from warehouse.warehouse import DataWarehouse
        gold_dir = context["ti"].xcom_pull(key="gold_dir", task_ids="gold")
        enriched_path = context["ti"].xcom_pull(key="enriched_path", task_ids="transform")

        dw = DataWarehouse()

        # Charger les articles enrichis
        if enriched_path and os.path.exists(enriched_path):
            with open(enriched_path, encoding="utf-8") as f:
                articles = json.load(f)
            dw.load_articles(articles)

        # Charger les tables Gold
        if gold_dir and os.path.exists(gold_dir):
            for table_file in os.listdir(gold_dir):
                if table_file.endswith(".json"):
                    table_name = table_file.replace(".json", "")
                    with open(f"{gold_dir}/{table_file}", encoding="utf-8") as f:
                        rows = json.load(f)
                    dw.load_table(table_name, rows)

        logger.info("Data Warehouse loaded")

    # ── Task 9 : Qualité des données ──────────────────
    def task_quality_check(**context):
        from quality.quality import DataQuality
        enriched_path = context["ti"].xcom_pull(key="enriched_path", task_ids="transform")
        if not enriched_path or not os.path.exists(enriched_path):
            return {}

        with open(enriched_path, encoding="utf-8") as f:
            articles = json.load(f)

        dq = DataQuality()
        report = dq.run_checks(articles)

        logger.info(f"Quality report: {report['summary']}")
        if report["summary"]["score"] < 70:
            logger.warning("Data quality score below 70%!")
        return report["summary"]

    # ── Définition des tasks ──────────────────────────
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    t_scrape   = PythonOperator(task_id="scrape",            python_callable=task_scrape)
    t_batch    = PythonOperator(task_id="ingest_batch",      python_callable=task_ingest_batch)
    t_stream   = PythonOperator(task_id="ingest_streaming",  python_callable=task_ingest_streaming)
    t_bronze   = PythonOperator(task_id="bronze",            python_callable=task_bronze)
    t_silver   = PythonOperator(task_id="silver",            python_callable=task_silver)
    t_transform= PythonOperator(task_id="transform",         python_callable=task_transform)
    t_gold     = PythonOperator(task_id="gold",              python_callable=task_gold)
    t_dw       = PythonOperator(task_id="load_warehouse",    python_callable=task_load_warehouse)
    t_quality  = PythonOperator(task_id="quality_check",     python_callable=task_quality_check)

    # ── Dépendances ───────────────────────────────────
    start >> t_scrape >> [t_batch, t_stream, t_bronze]
    t_bronze >> t_silver >> t_transform >> t_gold >> t_dw >> t_quality >> end
