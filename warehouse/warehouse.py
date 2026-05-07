"""
Data Warehouse — Stockage analytique avec PostgreSQL
Schéma en étoile :
  - Fait : fact_articles
  - Dimensions : dim_source, dim_category, dim_date, dim_language, dim_country
"""

import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger("DataWarehouse")


# ─────────────────────────────────────────────
# DDL — Schéma en étoile
# ─────────────────────────────────────────────

DDL_STATEMENTS = [
    # Dimensions
    """
    CREATE TABLE IF NOT EXISTS dim_source (
        source_id   SERIAL PRIMARY KEY,
        source_name VARCHAR(100) UNIQUE NOT NULL,
        country     VARCHAR(10),
        language    VARCHAR(10),
        created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_category (
        category_id   SERIAL PRIMARY KEY,
        category_name VARCHAR(100) UNIQUE NOT NULL,
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id     INTEGER PRIMARY KEY,   -- YYYYMMDD
        full_date   DATE NOT NULL,
        year        INTEGER,
        month       INTEGER,
        day         INTEGER,
        weekday     VARCHAR(20),
        quarter     INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_language (
        language_id   SERIAL PRIMARY KEY,
        language_code VARCHAR(10) UNIQUE NOT NULL,
        language_name VARCHAR(50)
    )
    """,
    # Table de faits
    """
    CREATE TABLE IF NOT EXISTS fact_articles (
        article_id      VARCHAR(64) PRIMARY KEY,
        title           TEXT NOT NULL,
        author          VARCHAR(200),
        url             TEXT,
        content_length  INTEGER,
        word_count      INTEGER,
        reading_time    FLOAT,
        is_long_form    BOOLEAN,
        published_at    TIMESTAMP,
        scraped_at      TIMESTAMP,
        date_id         INTEGER REFERENCES dim_date(date_id),
        source_id       INTEGER REFERENCES dim_source(source_id),
        category_id     INTEGER REFERENCES dim_category(category_id),
        language_id     INTEGER REFERENCES dim_language(language_id),
        loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Tables analytiques Gold
    """
    CREATE TABLE IF NOT EXISTS gold_articles_per_source (
        source          VARCHAR(100),
        article_count   INTEGER,
        avg_word_count  FLOAT,
        computed_at     TIMESTAMP,
        PRIMARY KEY (source, computed_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gold_articles_per_day (
        date            DATE,
        article_count   INTEGER,
        computed_at     TIMESTAMP,
        PRIMARY KEY (date, computed_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gold_articles_per_category (
        category        VARCHAR(100),
        article_count   INTEGER,
        computed_at     TIMESTAMP,
        PRIMARY KEY (category, computed_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gold_top_keywords (
        keyword         VARCHAR(100),
        frequency       INTEGER,
        computed_at     TIMESTAMP,
        PRIMARY KEY (keyword, computed_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gold_news_trends (
        category        VARCHAR(100),
        article_count   INTEGER,
        sample_titles   TEXT,
        sources         TEXT,
        computed_at     TIMESTAMP,
        PRIMARY KEY (category, computed_at)
    )
    """,
]

LANGUAGE_NAMES = {
    "fr": "Français", "en": "English", "ar": "العربية",
    "es": "Español", "de": "Deutsch", "unknown": "Unknown",
}


# ─────────────────────────────────────────────
# DataWarehouse class
# ─────────────────────────────────────────────

class DataWarehouse:
    """
    Interface vers le Data Warehouse.
    Utilise PostgreSQL en production, SQLite en local/test.
    """

    def __init__(self):
        self.db_type = os.getenv("DW_TYPE", "sqlite")   # "postgres" | "sqlite"
        self.pg_dsn = os.getenv(
            "DW_DSN",
            "postgresql://dwuser:dwpassword@postgres:5432/news_dw"
        )
        self.sqlite_path = os.getenv("SQLITE_PATH", "/tmp/news_dw.db")
        self._conn = None

    # ── Connexion ────────────────────────────────────

    def _connect(self):
        if self._conn:
            return self._conn
        if self.db_type == "postgres":
            try:
                import psycopg2
                self._conn = psycopg2.connect(self.pg_dsn)
                logger.info("Connected to PostgreSQL")
            except Exception as e:
                logger.warning(f"PostgreSQL failed ({e}), falling back to SQLite")
                self.db_type = "sqlite"
        if self.db_type == "sqlite":
            self._conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
            logger.info(f"Connected to SQLite: {self.sqlite_path}")
        return self._conn

    def _cursor(self):
        return self._connect().cursor()

    def _execute(self, sql: str, params=None):
        conn = self._connect()
        cur = conn.cursor()
        try:
            cur.execute(sql, params or [])
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"SQL error: {e}\nSQL: {sql[:200]}")
        finally:
            cur.close()

    def _adapt_ddl(self, sql: str) -> str:
        """Adapte le DDL PostgreSQL pour SQLite."""
        if self.db_type == "sqlite":
            sql = sql.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
            sql = sql.replace("REFERENCES dim_date(date_id)", "")
            sql = sql.replace("REFERENCES dim_source(source_id)", "")
            sql = sql.replace("REFERENCES dim_category(category_id)", "")
            sql = sql.replace("REFERENCES dim_language(language_id)", "")
            sql = sql.replace("TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TEXT DEFAULT CURRENT_TIMESTAMP")
        return sql

    # ── Initialisation ───────────────────────────────

    def init_schema(self):
        self._connect()
        for stmt in DDL_STATEMENTS:
            self._execute(self._adapt_ddl(stmt))
        self._seed_languages()
        logger.info("Schema initialized")

    def _seed_languages(self):
        for code, name in LANGUAGE_NAMES.items():
            try:
                self._execute(
                    "INSERT OR IGNORE INTO dim_language (language_code, language_name) VALUES (?, ?)",
                    [code, name]
                ) if self.db_type == "sqlite" else self._execute(
                    "INSERT INTO dim_language (language_code, language_name) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    [code, name]
                )
            except Exception:
                pass

    # ── Chargement articles ──────────────────────────

    def _get_or_create_dim(self, table: str, name_col: str, name_val: str) -> Optional[int]:
        conn = self._connect()
        cur = conn.cursor()
        try:
            if self.db_type == "sqlite":
                cur.execute(f"SELECT rowid FROM {table} WHERE {name_col} = ?", [name_val])
                row = cur.fetchone()
                if row:
                    return row[0]
                cur.execute(f"INSERT OR IGNORE INTO {table} ({name_col}) VALUES (?)", [name_val])
                conn.commit()
                cur.execute(f"SELECT rowid FROM {table} WHERE {name_col} = ?", [name_val])
                row = cur.fetchone()
                return row[0] if row else None
            else:
                cur.execute(f"SELECT source_id FROM {table} WHERE {name_col} = %s", [name_val])
                row = cur.fetchone()
                if row:
                    return row[0]
                cur.execute(
                    f"INSERT INTO {table} ({name_col}) VALUES (%s) "
                    f"ON CONFLICT ({name_col}) DO NOTHING RETURNING source_id",
                    [name_val]
                )
                conn.commit()
                cur.execute(f"SELECT source_id FROM {table} WHERE {name_col} = %s", [name_val])
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            cur.close()

    def _ensure_date_dim(self, date_str: Optional[str]) -> Optional[int]:
        if not date_str:
            return None
        try:
            dt = datetime.fromisoformat(date_str[:10])
            date_id = int(dt.strftime("%Y%m%d"))
            conn = self._connect()
            cur = conn.cursor()
            if self.db_type == "sqlite":
                cur.execute("SELECT date_id FROM dim_date WHERE date_id = ?", [date_id])
            else:
                cur.execute("SELECT date_id FROM dim_date WHERE date_id = %s", [date_id])
            if not cur.fetchone():
                vals = (
                    date_id, dt.date().isoformat(), dt.year, dt.month, dt.day,
                    dt.strftime("%A"), (dt.month - 1) // 3 + 1
                )
                if self.db_type == "sqlite":
                    cur.execute(
                        "INSERT OR IGNORE INTO dim_date VALUES (?,?,?,?,?,?,?)", vals
                    )
                else:
                    cur.execute(
                        "INSERT INTO dim_date VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", vals
                    )
                conn.commit()
            cur.close()
            return date_id
        except Exception as e:
            logger.debug(f"Date dim error: {e}")
            return None

    def load_articles(self, articles: List[Dict]):
        self.init_schema()
        loaded = 0
        for art in articles:
            try:
                source_id   = self._get_or_create_dim("dim_source", "source_name", art.get("source", "unknown"))
                category_id = self._get_or_create_dim("dim_category", "category_name", art.get("category", "Non classé"))
                date_id     = self._ensure_date_dim(art.get("published_at"))

                p = "?" if self.db_type == "sqlite" else "%s"
                self._execute(f"""
                    INSERT OR IGNORE INTO fact_articles
                      (article_id, title, author, url, content_length,
                       word_count, reading_time, is_long_form, published_at,
                       scraped_at, date_id, source_id, category_id)
                    VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})
                """, [
                    art.get("article_id"), art.get("title"), art.get("author"),
                    art.get("url"), art.get("content_length") or art.get("char_count"),
                    art.get("word_count"), art.get("reading_time_min"),
                    art.get("is_long_form", False), art.get("published_at"),
                    art.get("scraped_at"), date_id, source_id, category_id,
                ])
                loaded += 1
            except Exception as e:
                logger.debug(f"Article insert error: {e}")

        logger.info(f"[DW] Loaded {loaded}/{len(articles)} articles")

    def load_table(self, table_name: str, rows: List[Dict]):
        """Charge une table Gold générique."""
        if not rows:
            return
        computed_at = datetime.utcnow().isoformat()
        for row in rows:
            row["computed_at"] = row.get("computed_at", computed_at)

        table_map = {
            "articles_per_source":   self._load_per_source,
            "articles_per_day":      self._load_per_day,
            "articles_per_category": self._load_per_category,
            "top_keywords":          self._load_keywords,
            "news_trends":           self._load_trends,
        }
        loader = table_map.get(table_name)
        if loader:
            loader(rows)

    def _load_per_source(self, rows):
        p = "?" if self.db_type == "sqlite" else "%s"
        for r in rows:
            self._execute(
                f"INSERT OR IGNORE INTO gold_articles_per_source VALUES ({p},{p},{p},{p})",
                [r.get("source"), r.get("article_count"), r.get("avg_word_count"), r.get("computed_at")]
            )

    def _load_per_day(self, rows):
        p = "?" if self.db_type == "sqlite" else "%s"
        for r in rows:
            self._execute(
                f"INSERT OR IGNORE INTO gold_articles_per_day VALUES ({p},{p},{p})",
                [r.get("date"), r.get("article_count"), r.get("computed_at")]
            )

    def _load_per_category(self, rows):
        p = "?" if self.db_type == "sqlite" else "%s"
        for r in rows:
            self._execute(
                f"INSERT OR IGNORE INTO gold_articles_per_category VALUES ({p},{p},{p})",
                [r.get("category"), r.get("article_count"), r.get("computed_at")]
            )

    def _load_keywords(self, rows):
        p = "?" if self.db_type == "sqlite" else "%s"
        for r in rows:
            self._execute(
                f"INSERT OR IGNORE INTO gold_top_keywords VALUES ({p},{p},{p})",
                [r.get("keyword"), r.get("frequency"), r.get("computed_at")]
            )

    def _load_trends(self, rows):
        p = "?" if self.db_type == "sqlite" else "%s"
        for r in rows:
            self._execute(
                f"INSERT OR IGNORE INTO gold_news_trends VALUES ({p},{p},{p},{p},{p})",
                [
                    r.get("category"), r.get("article_count"),
                    json.dumps(r.get("sample_titles", []), ensure_ascii=False),
                    json.dumps(r.get("sources", []), ensure_ascii=False),
                    r.get("computed_at"),
                ]
            )

    def query(self, sql: str) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        return rows

    def close(self):
        if self._conn:
            self._conn.close()


if __name__ == "__main__":
    dw = DataWarehouse()
    dw.init_schema()
    test_articles = [
        {
            "article_id": "test_001",
            "title": "Test article DW",
            "author": "Walid",
            "url": "https://example.com/test",
            "content_length": 500,
            "word_count": 100,
            "reading_time_min": 0.5,
            "is_long_form": False,
            "published_at": "2026-05-06T10:00:00",
            "scraped_at": "2026-05-06T10:05:00",
            "source": "Hespress",
            "category": "Economie",
        }
    ]
    dw.load_articles(test_articles)
    rows = dw.query("SELECT * FROM fact_articles")
    print(json.dumps(rows, indent=2, default=str))
    dw.close()
