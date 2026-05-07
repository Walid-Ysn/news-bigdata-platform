"""
Ingestion Module
- Batch ingestion : sauvegarde JSON → MinIO
- Streaming ingestion : envoi article par article → Kafka
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import List, Dict

logger = logging.getLogger("Ingestion")

# ─────────────────────────────────────────────
# Batch Ingestion via MinIO
# ─────────────────────────────────────────────
class BatchIngestion:
    """
    Stocke les articles bruts dans MinIO (Data Lake - couche Bronze).
    Chaque run crée un fichier JSON partitionné par date.
    """

    def __init__(self, minio_endpoint: str = None, access_key: str = None, secret_key: str = None):
        self.endpoint = minio_endpoint or os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket = "news-datalake"
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                from minio import Minio
                self._client = Minio(
                    self.endpoint,
                    access_key=self.access_key,
                    secret_key=self.secret_key,
                    secure=False,
                )
                self._ensure_bucket()
            except ImportError:
                logger.warning("minio package not installed. Using local file fallback.")
        return self._client

    def _ensure_bucket(self):
        client = self._client
        if client and not client.bucket_exists(self.bucket):
            client.make_bucket(self.bucket)
            logger.info(f"Bucket '{self.bucket}' created.")

    def ingest(self, articles: List[Dict]) -> str:
        now = datetime.utcnow()
        partition = now.strftime("year=%Y/month=%m/day=%d/hour=%H")
        filename = f"bronze/{partition}/articles_{now.strftime('%Y%m%d_%H%M%S')}.json"
        content = json.dumps(articles, ensure_ascii=False, indent=2).encode("utf-8")

        client = self._get_client()
        if client:
            from io import BytesIO
            client.put_object(
                self.bucket,
                filename,
                BytesIO(content),
                length=len(content),
                content_type="application/json",
            )
            path = f"minio://{self.bucket}/{filename}"
        else:
            # Fallback local pour les tests
            os.makedirs(f"/tmp/datalake/bronze/{partition}", exist_ok=True)
            local_path = f"/tmp/datalake/{filename}"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(articles, ensure_ascii=False, indent=2))
            path = local_path

        logger.info(f"[BatchIngestion] {len(articles)} articles → {path}")
        return path


# ─────────────────────────────────────────────
# Streaming Ingestion via Kafka
# ─────────────────────────────────────────────
class StreamingIngestion:
    """
    Publie chaque article comme événement Kafka.
    Topic : news-articles-raw
    """

    TOPIC = "news-articles-raw"

    def __init__(self, bootstrap_servers: str = None):
        self.servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self._producer = None

    def _get_producer(self):
        if self._producer is None:
            try:
                from kafka import KafkaProducer
                self._producer = KafkaProducer(
                    bootstrap_servers=self.servers,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                )
                logger.info(f"Kafka producer connected to {self.servers}")
            except Exception as e:
                logger.warning(f"Kafka not available: {e}. Events will be logged only.")
        return self._producer

    def publish(self, article: Dict) -> bool:
        producer = self._get_producer()
        article_id = article.get("article_id", "unknown")
        article["ingested_at"] = datetime.utcnow().isoformat()
        article["ingestion_mode"] = "streaming"

        if producer:
            try:
                future = producer.send(
                    self.TOPIC,
                    key=article_id,
                    value=article,
                )
                future.get(timeout=10)
                return True
            except Exception as e:
                logger.error(f"Failed to publish article {article_id}: {e}")
                return False
        else:
            logger.info(f"[MOCK KAFKA] Article published: {article_id} | {article.get('title', '')[:60]}")
            return True

    def publish_batch(self, articles: List[Dict], delay: float = 0.1):
        success = 0
        for article in articles:
            if self.publish(article):
                success += 1
            time.sleep(delay)

        if self._producer:
            self._producer.flush()

        logger.info(f"[StreamingIngestion] Published {success}/{len(articles)} articles to Kafka")
        return success

    def close(self):
        if self._producer:
            self._producer.close()


# ─────────────────────────────────────────────
# Kafka Consumer (lecture depuis le topic)
# ─────────────────────────────────────────────
class NewsConsumer:
    """
    Consomme les articles depuis Kafka et les passe à un handler.
    Utilisation : brancher sur le pipeline Silver.
    """

    TOPIC = "news-articles-raw"

    def __init__(self, group_id: str = "silver-processor", bootstrap_servers: str = None):
        self.servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.group_id = group_id

    def consume(self, handler_fn, max_messages: int = None):
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                self.TOPIC,
                bootstrap_servers=self.servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            logger.info(f"Consuming from {self.TOPIC}")
            count = 0
            for message in consumer:
                handler_fn(message.value)
                count += 1
                if max_messages and count >= max_messages:
                    break
            consumer.close()
        except Exception as e:
            logger.error(f"Consumer error: {e}")


# ─────────────────────────────────────────────
# Ingestion Pipeline (combine batch + streaming)
# ─────────────────────────────────────────────
class IngestionPipeline:
    def __init__(self):
        self.batch = BatchIngestion()
        self.streaming = StreamingIngestion()

    def run(self, articles: List[Dict], mode: str = "both"):
        results = {}

        if mode in ("batch", "both"):
            path = self.batch.ingest(articles)
            results["batch_path"] = path

        if mode in ("streaming", "both"):
            success = self.streaming.publish_batch(articles)
            results["streaming_published"] = success

        return results


if __name__ == "__main__":
    # Test avec des données mock
    mock_articles = [
        {
            "article_id": "abc123",
            "title": "Test article from Hespress",
            "author": "Rédaction",
            "published_at": "2026-05-06T10:00:00",
            "category": "Politique",
            "content": "Ceci est le contenu de l'article de test pour valider le pipeline d'ingestion.",
            "source": "Hespress",
            "url": "https://fr.hespress.com/test-article",
            "language": "fr",
            "scraped_at": datetime.utcnow().isoformat(),
            "country": "MA",
        }
    ]
    pipeline = IngestionPipeline()
    results = pipeline.run(mock_articles, mode="batch")
    print(json.dumps(results, indent=2))
