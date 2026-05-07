"""
Transformation des données — Python / PySpark
Nettoie, normalise et enrichit les articles Silver avant chargement Gold.
"""

import json
import logging
import os
import re
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger("Transformation")


# ─────────────────────────────────────────────
# Pure Python Transformer (sans Spark)
# ─────────────────────────────────────────────

class ArticleTransformer:
    """
    Transformation légère en Python pur.
    Utilisée dans les environnements sans Spark.
    """

    def enrich(self, article: Dict) -> Dict:
        content = article.get("content", "")
        title = article.get("title", "")

        article["word_count"] = len(content.split()) if content else 0
        article["char_count"] = len(content) if content else 0
        article["title_word_count"] = len(title.split()) if title else 0

        # Estimation du temps de lecture (200 mots/min)
        article["reading_time_min"] = round(article["word_count"] / 200, 1)

        # Semaine ISO et mois pour le partitionnement
        pub = article.get("published_at")
        if pub:
            try:
                dt = datetime.fromisoformat(pub[:19])
                article["pub_year"] = dt.year
                article["pub_month"] = dt.month
                article["pub_day"] = dt.day
                article["pub_weekday"] = dt.strftime("%A")
                article["pub_hour"] = dt.hour
            except Exception:
                pass

        # Indicateur article long
        article["is_long_form"] = article["word_count"] > 800

        article["transformed_at"] = datetime.utcnow().isoformat()
        return article

    def transform_batch(self, articles: List[Dict]) -> List[Dict]:
        enriched = [self.enrich(dict(art)) for art in articles]
        logger.info(f"[Transformer] Enriched {len(enriched)} articles")
        return enriched


# ─────────────────────────────────────────────
# PySpark Transformer
# ─────────────────────────────────────────────

class SparkTransformer:
    """
    Transformation distribuée avec PySpark.
    Requiert un cluster Spark ou spark-submit local.
    """

    def __init__(self, app_name: str = "NewsTransformer"):
        self.app_name = app_name
        self._spark = None

    def _get_spark(self):
        if self._spark:
            return self._spark
        try:
            from pyspark.sql import SparkSession
            self._spark = (
                SparkSession.builder
                .appName(self.app_name)
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.driver.memory", "2g")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session created")
            return self._spark
        except ImportError:
            logger.error("PySpark not installed. Use ArticleTransformer instead.")
            return None

    def transform(self, input_path: str, output_path: str):
        """
        Lit les articles Silver depuis JSON, applique les transformations
        et écrit le résultat en Parquet (format optimisé pour le DW).
        """
        spark = self._get_spark()
        if not spark:
            return False

        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType, FloatType

        df = spark.read.json(input_path)

        # Calcul du nombre de mots
        df = df.withColumn("word_count", F.size(F.split(F.col("content"), r"\s+")))

        # Temps de lecture estimé
        df = df.withColumn("reading_time_min",
                           (F.col("word_count") / 200).cast(FloatType()))

        # Extraction de la date de publication
        df = df.withColumn("published_at_ts",
                           F.to_timestamp("published_at", "yyyy-MM-dd'T'HH:mm:ss"))
        df = df.withColumn("pub_year", F.year("published_at_ts"))
        df = df.withColumn("pub_month", F.month("published_at_ts"))
        df = df.withColumn("pub_day", F.dayofmonth("published_at_ts"))
        df = df.withColumn("pub_weekday", F.date_format("published_at_ts", "EEEE"))

        # Article long
        df = df.withColumn("is_long_form", F.col("word_count") > 800)

        # Timestamp de transformation
        df = df.withColumn("transformed_at", F.current_timestamp())

        # Ecriture en Parquet partitionné
        (df.write
           .mode("overwrite")
           .partitionBy("pub_year", "pub_month", "source")
           .parquet(output_path))

        logger.info(f"[SparkTransformer] Written to {output_path}")
        return True

    def compute_aggregations(self, silver_path: str, gold_output: str):
        """Calcule les agrégations analytiques avec Spark SQL."""
        spark = self._get_spark()
        if not spark:
            return False

        from pyspark.sql import functions as F

        df = spark.read.parquet(silver_path) if silver_path.endswith(".parquet") \
             else spark.read.json(silver_path)

        df.createOrReplaceTempView("articles")

        agg_queries = {
            "articles_per_source": """
                SELECT source, COUNT(*) as article_count,
                       AVG(word_count) as avg_word_count
                FROM articles
                GROUP BY source
                ORDER BY article_count DESC
            """,
            "articles_per_day": """
                SELECT DATE(published_at_ts) as date,
                       COUNT(*) as article_count
                FROM articles
                WHERE published_at_ts IS NOT NULL
                GROUP BY DATE(published_at_ts)
                ORDER BY date DESC
            """,
            "articles_per_category": """
                SELECT category, COUNT(*) as article_count,
                       COUNT(DISTINCT source) as source_count
                FROM articles
                GROUP BY category
                ORDER BY article_count DESC
            """,
            "articles_per_language": """
                SELECT language, COUNT(*) as article_count,
                       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                FROM articles
                GROUP BY language
                ORDER BY article_count DESC
            """,
        }

        os.makedirs(gold_output, exist_ok=True)
        for name, query in agg_queries.items():
            result_df = spark.sql(query)
            out = f"{gold_output}/{name}"
            result_df.write.mode("overwrite").json(out)
            logger.info(f"[SparkTransformer] {name} written to {out}")

        return True

    def stop(self):
        if self._spark:
            self._spark.stop()


# ─────────────────────────────────────────────
# ETL Pipeline
# ─────────────────────────────────────────────

class ETLPipeline:
    """
    Orchestre la transformation complète :
    Bronze JSON → Silver transformé → Gold agrégations
    """

    def __init__(self, use_spark: bool = False):
        self.use_spark = use_spark
        self.transformer = SparkTransformer() if use_spark else ArticleTransformer()

    def run(self, silver_articles: List[Dict]) -> List[Dict]:
        if self.use_spark:
            logger.info("Using Spark for transformation")
            # Spark path nécessite fichiers sur HDFS/MinIO
            return silver_articles
        else:
            return self.transformer.transform_batch(silver_articles)


if __name__ == "__main__":
    mock = [
        {
            "article_id": "test1",
            "title": "Réforme économique au Maroc",
            "content": "Le gouvernement annonce une vaste réforme économique. " * 50,
            "source": "Hespress",
            "category": "Economie",
            "language": "fr",
            "country": "MA",
            "published_at": "2026-05-06T09:00:00",
        }
    ]

    pipeline = ETLPipeline(use_spark=False)
    result = pipeline.run(mock)
    print(json.dumps(result[0], ensure_ascii=False, indent=2))
