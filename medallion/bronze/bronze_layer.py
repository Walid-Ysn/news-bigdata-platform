"""
Couche Bronze — Stockage brut des articles dans MinIO
Les données arrivent telles quelles depuis le scraper, sans transformation.
"""

import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional
from io import BytesIO

logger = logging.getLogger("Bronze")


class BronzeLayer:
    """
    Responsabilité unique : persister les articles bruts dans MinIO
    sous forme de fichiers JSON partitionnés par source et par date.

    Structure dans MinIO :
        news-datalake/
          bronze/
            source=Hespress/
              year=2026/month=05/day=06/
                articles_20260506_1400.json
            source=BBC News/
              ...
    """

    BUCKET = "news-datalake"

    def __init__(self):
        self.endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self._client = None

    def _client_or_none(self):
        if self._client:
            return self._client
        try:
            from minio import Minio
            c = Minio(self.endpoint, access_key=self.access_key, secret_key=self.secret_key, secure=False)
            if not c.bucket_exists(self.BUCKET):
                c.make_bucket(self.BUCKET)
            self._client = c
            return c
        except Exception as e:
            logger.warning(f"MinIO unavailable ({e}), falling back to local FS")
            return None

    def _object_key(self, source: str, dt: datetime) -> str:
        safe_source = source.replace(" ", "_")
        partition = dt.strftime("year=%Y/month=%m/day=%d")
        timestamp = dt.strftime("%Y%m%d_%H%M%S")
        return f"bronze/source={safe_source}/{partition}/articles_{timestamp}.json"

    def store(self, articles: List[Dict]) -> Dict[str, str]:
        """
        Regroupe les articles par source et les stocke séparément.
        Retourne un dict source → chemin de stockage.
        """
        by_source: Dict[str, List[Dict]] = {}
        for art in articles:
            src = art.get("source", "unknown")
            by_source.setdefault(src, []).append(art)

        now = datetime.utcnow()
        paths = {}
        client = self._client_or_none()

        for source, arts in by_source.items():
            key = self._object_key(source, now)
            payload = json.dumps(arts, ensure_ascii=False, indent=2).encode("utf-8")

            if client:
                client.put_object(
                    self.BUCKET, key, BytesIO(payload),
                    length=len(payload), content_type="application/json"
                )
                path = f"minio://{self.BUCKET}/{key}"
            else:
                local = f"/tmp/datalake/{key}"
                os.makedirs(os.path.dirname(local), exist_ok=True)
                with open(local, "wb") as f:
                    f.write(payload)
                path = local

            paths[source] = path
            logger.info(f"[Bronze] {source}: {len(arts)} articles → {path}")

        return paths

    def list_objects(self, prefix: str = "bronze/") -> List[str]:
        client = self._client_or_none()
        if not client:
            return []
        try:
            from minio import Minio
            objects = client.list_objects(self.BUCKET, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except Exception as e:
            logger.error(f"list_objects failed: {e}")
            return []

    def read(self, object_key: str) -> Optional[List[Dict]]:
        client = self._client_or_none()
        if not client:
            return None
        try:
            resp = client.get_object(self.BUCKET, object_key)
            data = json.loads(resp.read().decode("utf-8"))
            return data
        except Exception as e:
            logger.error(f"read failed for {object_key}: {e}")
            return None
