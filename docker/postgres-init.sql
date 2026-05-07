-- PostgreSQL init script
-- Crée les bases de données pour Airflow et le Data Warehouse

-- Base Airflow
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Data Warehouse
CREATE DATABASE news_dw;
CREATE USER dwuser WITH PASSWORD 'dwpassword';
GRANT ALL PRIVILEGES ON DATABASE news_dw TO dwuser;

-- Connexion à news_dw pour créer le schéma
\c news_dw;

-- Schéma dimensions
CREATE TABLE IF NOT EXISTS dim_source (
    source_id   SERIAL PRIMARY KEY,
    source_name VARCHAR(100) UNIQUE NOT NULL,
    country     VARCHAR(10),
    language    VARCHAR(10),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_id   SERIAL PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE NOT NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id   INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year      INTEGER,
    month     INTEGER,
    day       INTEGER,
    weekday   VARCHAR(20),
    quarter   INTEGER
);

CREATE TABLE IF NOT EXISTS dim_language (
    language_id   SERIAL PRIMARY KEY,
    language_code VARCHAR(10) UNIQUE NOT NULL,
    language_name VARCHAR(50)
);

-- Seed langues
INSERT INTO dim_language (language_code, language_name) VALUES
    ('fr', 'Français'),
    ('en', 'English'),
    ('ar', 'العربية'),
    ('unknown', 'Unknown')
ON CONFLICT DO NOTHING;

-- Table de faits
CREATE TABLE IF NOT EXISTS fact_articles (
    article_id     VARCHAR(64) PRIMARY KEY,
    title          TEXT NOT NULL,
    author         VARCHAR(200),
    url            TEXT,
    content_length INTEGER,
    word_count     INTEGER,
    reading_time   FLOAT,
    is_long_form   BOOLEAN DEFAULT FALSE,
    published_at   TIMESTAMP,
    scraped_at     TIMESTAMP,
    date_id        INTEGER REFERENCES dim_date(date_id),
    source_id      INTEGER REFERENCES dim_source(source_id),
    category_id    INTEGER REFERENCES dim_category(category_id),
    language_id    INTEGER REFERENCES dim_language(language_id),
    loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_fact_source    ON fact_articles(source_id);
CREATE INDEX IF NOT EXISTS idx_fact_category  ON fact_articles(category_id);
CREATE INDEX IF NOT EXISTS idx_fact_date      ON fact_articles(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_published ON fact_articles(published_at);

-- Tables analytiques Gold
CREATE TABLE IF NOT EXISTS gold_articles_per_source (
    source        VARCHAR(100),
    article_count INTEGER,
    avg_word_count FLOAT,
    computed_at   TIMESTAMP,
    PRIMARY KEY (source, computed_at)
);

CREATE TABLE IF NOT EXISTS gold_articles_per_day (
    date          DATE,
    article_count INTEGER,
    computed_at   TIMESTAMP,
    PRIMARY KEY (date, computed_at)
);

CREATE TABLE IF NOT EXISTS gold_articles_per_category (
    category      VARCHAR(100),
    article_count INTEGER,
    computed_at   TIMESTAMP,
    PRIMARY KEY (category, computed_at)
);

CREATE TABLE IF NOT EXISTS gold_top_keywords (
    keyword       VARCHAR(100),
    frequency     INTEGER,
    computed_at   TIMESTAMP,
    PRIMARY KEY (keyword, computed_at)
);

CREATE TABLE IF NOT EXISTS gold_news_trends (
    category      VARCHAR(100),
    article_count INTEGER,
    sample_titles TEXT,
    sources       TEXT,
    computed_at   TIMESTAMP,
    PRIMARY KEY (category, computed_at)
);

-- Vue analytique : articles par source et jour
CREATE OR REPLACE VIEW v_articles_daily_by_source AS
SELECT
    d.full_date,
    s.source_name,
    COUNT(f.article_id) AS article_count,
    AVG(f.word_count) AS avg_words
FROM fact_articles f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_source s ON f.source_id = s.source_id
GROUP BY d.full_date, s.source_name
ORDER BY d.full_date DESC, article_count DESC;

-- Vue : distribution des langues
CREATE OR REPLACE VIEW v_language_distribution AS
SELECT
    l.language_name,
    l.language_code,
    COUNT(f.article_id) AS article_count,
    ROUND(COUNT(f.article_id) * 100.0 / SUM(COUNT(f.article_id)) OVER(), 2) AS percentage
FROM fact_articles f
JOIN dim_language l ON f.language_id = l.language_id
GROUP BY l.language_name, l.language_code
ORDER BY article_count DESC;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dwuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dwuser;
