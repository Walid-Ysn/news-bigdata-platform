"""
Pipeline principal — Lance tout le pipeline sans Airflow
Utile pour les tests locaux et la démonstration.

Usage:
    python main.py                   # Pipeline complet
    python main.py --scrape-only     # Scraping seulement
    python main.py --mock            # Données mock (pas de scraping réel)
    python main.py --dashboard       # Lance le dashboard uniquement
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/pipeline.log", encoding="utf-8"),
    ]
)
logger = logging.getLogger("Pipeline")


def get_mock_articles(n: int = 50):
    """Génère des articles mock pour les tests."""
    import random, hashlib
    random.seed(42)

    sources_data = [
        ("Hespress", "MA", "fr"),
        ("Akhbarona", "MA", "ar"),
        ("BBC News", "GB", "en"),
        ("Al Jazeera", "QA", "en"),
        ("Reuters", "GB", "en"),
        ("Barlamane", "MA", "ar"),
    ]
    categories = ["Politique", "Economie", "Sport", "Technologie", "Culture", "International"]
    sample_titles = {
        "fr": [
            "Le Maroc renforce ses infrastructures numériques",
            "Réforme économique : nouvelles mesures annoncées",
            "CHAN 2025 : L'équipe nationale se prépare",
            "Investissements étrangers en hausse au Maroc",
            "Éducation : modernisation du système scolaire",
        ],
        "en": [
            "Morocco strengthens digital economy",
            "Middle East tensions continue to rise",
            "Global markets react to economic data",
            "Climate change summit reaches agreement",
            "Technology sector drives job growth",
        ],
        "ar": [
            "المغرب يعزز بنيته التحتية الرقمية",
            "إصلاحات اقتصادية جديدة تُعلن في المغرب",
            "المنتخب الوطني يستعد لبطولة إفريقيا",
            "ارتفاع الاستثمارات الأجنبية في المغرب",
        ],
    }

    articles = []
    for i in range(n):
        source, country, lang = random.choice(sources_data)
        titles = sample_titles.get(lang, sample_titles["fr"])
        title = random.choice(titles) + f" — {i}"
        url = f"https://{source.lower().replace(' ', '')}.com/article-{i}"
        content = (
            "Lorem ipsum dolor sit amet consectetur adipiscing elit. "
            "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
            "Ut enim ad minim veniam quis nostrud exercitation. " * random.randint(5, 20)
        )

        articles.append({
            "article_id": hashlib.md5(url.encode()).hexdigest(),
            "title": title,
            "author": random.choice(["Rédaction", "Ahmed Bennani", "Sara El Idrissi", None]),
            "published_at": f"2026-05-{random.randint(1, 7):02d}T{random.randint(6, 22):02d}:00:00",
            "category": random.choice(categories),
            "content": content,
            "source": source,
            "url": url,
            "language": lang,
            "scraped_at": datetime.utcnow().isoformat(),
            "country": country,
        })

    return articles


def run_pipeline(use_mock: bool = False, scrape_only: bool = False):
    """Exécute le pipeline complet."""
    from governance.governance import GovernanceDB, PipelineRun, DataLineage, DataCatalog, QualityLogger

    db = GovernanceDB()
    run = PipelineRun(db=db)
    lineage = DataLineage(run.run_id, db=db)

    try:
        # ── Étape 1 : Scraping ─────────────────────────────────────
        logger.info("=" * 50)
        logger.info("ÉTAPE 1 : Scraping")
        logger.info("=" * 50)

        if use_mock:
            raw_articles = get_mock_articles(50)
            logger.info(f"[MOCK] {len(raw_articles)} articles générés")
        else:
            from scrapers.scraper import ScraperManager
            manager = ScraperManager(max_per_source=15)
            scraped = manager.run_all()
            raw_articles = [a.to_dict() for a in scraped]
            logger.info(f"Scraped {len(raw_articles)} articles from all sources")

        lineage.log("scraping", "web_scrape", "web:news_sites", "memory:raw_articles", len(raw_articles))

        if scrape_only:
            print(json.dumps(raw_articles[:2], ensure_ascii=False, indent=2))
            run.complete(len(raw_articles), len(raw_articles), "scrape-only mode")
            return

        # ── Étape 2 : Ingestion Batch ──────────────────────────────
        logger.info("\n" + "=" * 50)
        logger.info("ÉTAPE 2 : Ingestion Batch → MinIO")
        logger.info("=" * 50)

        from ingestion.ingestion import BatchIngestion
        batch = BatchIngestion()
        batch_path = batch.ingest(raw_articles)
        lineage.log("ingestion", "batch_store", "memory:raw_articles", batch_path, len(raw_articles))

        # ── Étape 3 : Bronze Layer ─────────────────────────────────
        logger.info("\n" + "=" * 50)
        logger.info("ÉTAPE 3 : Bronze Layer")
        logger.info("=" * 50)

        from medallion.bronze.bronze_layer import BronzeLayer
        bronze = BronzeLayer()
        bronze_paths = bronze.store(raw_articles)
        lineage.log("bronze", "store_raw", batch_path, str(bronze_paths), len(raw_articles))

        # ── Étape 4 : Silver Layer ─────────────────────────────────
        logger.info("\n" + "=" * 50)
        logger.info("ÉTAPE 4 : Silver Layer — Nettoyage")
        logger.info("=" * 50)

        from medallion.silver.silver_layer import SilverLayer
        silver = SilverLayer()
        silver_articles = silver.transform_batch(raw_articles)
        lineage.log("silver", "clean_transform", "bronze:raw_articles", "memory:silver_articles", len(silver_articles))

        # ── Étape 5 : Transformation / Enrichissement ──────────────
        logger.info("\n" + "=" * 50)
        logger.info("ÉTAPE 5 : Transformation & Enrichissement")
        logger.info("=" * 50)

        from transformation.transform import ArticleTransformer
        transformer = ArticleTransformer()
        enriched = transformer.transform_batch(silver_articles)
        lineage.log("transformation", "enrich", "memory:silver_articles", "memory:enriched", len(enriched))

        # ── Étape 6 : Gold Layer ───────────────────────────────────
        logger.info("\n" + "=" * 50)
        logger.info("ÉTAPE 6 : Gold Layer — Agrégations")
        logger.info("=" * 50)

        from medallion.gold.gold_layer import GoldLayer
        gold = GoldLayer()
        gold_data = gold.compute_all(enriched)
        gold_dir = "/tmp/gold_output"
        gold.save_all(gold_data, gold_dir)
        lineage.log("gold", "aggregate", "memory:enriched", gold_dir, len(enriched))

        # ── Étape 7 : Data Warehouse ───────────────────────────────
        logger.info("\n" + "=" * 50)
        logger.info("ÉTAPE 7 : Chargement Data Warehouse")
        logger.info("=" * 50)

        from warehouse.warehouse import DataWarehouse
        dw = DataWarehouse()
        dw.load_articles(enriched)
        for table_name, rows in gold_data.items():
            dw.load_table(table_name, rows)
        lineage.log("warehouse", "load", gold_dir, "postgres://news_dw", len(enriched))

        # ── Étape 8 : Qualité des données ──────────────────────────
        logger.info("\n" + "=" * 50)
        logger.info("ÉTAPE 8 : Contrôle Qualité")
        logger.info("=" * 50)

        from quality.quality import DataQuality
        dq = DataQuality()
        report = dq.run_checks(enriched)
        print(dq.generate_report(report, "/tmp/quality_report.txt"))

        ql = QualityLogger(db=db)
        ql.log(run.run_id, report)

        # ── Étape 9 : Gouvernance ──────────────────────────────────
        logger.info("\n" + "=" * 50)
        logger.info("ÉTAPE 9 : Gouvernance & Catalogue")
        logger.info("=" * 50)

        catalog = DataCatalog(db=db)
        catalog.update_count("bronze_articles", len(raw_articles))
        catalog.update_count("silver_articles", len(silver_articles))
        catalog.print_catalog()

        # ── Résumé ─────────────────────────────────────────────────
        run.complete(
            articles_in=len(raw_articles),
            articles_out=len(enriched),
            notes=f"Quality score: {report['summary']['score']}%",
        )

        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE TERMINÉ AVEC SUCCÈS")
        logger.info(f"  Articles collectés   : {len(raw_articles)}")
        logger.info(f"  Articles après Silver: {len(silver_articles)}")
        logger.info(f"  Articles enrichis    : {len(enriched)}")
        logger.info(f"  Score qualité        : {report['summary']['score']}%")
        logger.info(f"  Tables Gold          : {list(gold_data.keys())}")
        logger.info("=" * 60)

        return {
            "raw": len(raw_articles),
            "silver": len(silver_articles),
            "enriched": len(enriched),
            "quality_score": report["summary"]["score"],
        }

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        run.fail(str(e))
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="News Big Data Pipeline")
    parser.add_argument("--mock", action="store_true", help="Utiliser des données mock")
    parser.add_argument("--scrape-only", action="store_true", help="Scraping seulement")
    parser.add_argument("--dashboard", action="store_true", help="Lancer le dashboard")
    args = parser.parse_args()

    if args.dashboard:
        from visualization.dashboard import create_app
        app = create_app()
        if app:
            app.run(debug=True, host="0.0.0.0", port=8050)
    else:
        results = run_pipeline(use_mock=args.mock, scrape_only=args.scrape_only)
        if results:
            print(f"\n✅ Pipeline OK — {results['enriched']} articles traités — Score qualité: {results['quality_score']}%")
