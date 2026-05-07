# 📰 News Big Data Platform

**Projet : Architecture de données — EMSI Casablanca**
**Filière IADATA — AU 2025/2026**

Plateforme Big Data de collecte, traitement et analyse automatique d'articles de presse à partir de sources marocaines et internationales.

---

## Architecture globale

```
Sources Web
    │
    ▼
[Scrapers Python]           ← BeautifulSoup / Scrapy
    │
    ├──► Batch Ingestion    → MinIO (Data Lake)
    └──► Streaming          → Kafka (events)
              │
              ▼
    ┌─────────────────────────────────────────┐
    │         Architecture Médaillon          │
    │                                         │
    │  [Bronze] Articles bruts JSON           │
    │      ↓                                  │
    │  [Silver] Nettoyage + NLP               │
    │      ↓                                  │
    │  [Gold] Agrégations analytiques         │
    └─────────────────────────────────────────┘
              │
              ▼
    [Data Warehouse PostgreSQL]
              │
              ▼
    [Grafana / Dash Dashboard]

    ⟳ Orchestration : Apache Airflow (toutes les heures)
    ✓ Qualité : Tests complétude / validité / cohérence
    📋 Gouvernance : Lineage + Catalogue de données
```

---

## Structure du projet

```
news-bigdata-platform/
│
├── scrapers/
│   └── scraper.py              # Scrapers Hespress, Akhbarona, BBC, Al Jazeera
│
├── ingestion/
│   └── ingestion.py            # Batch (MinIO) + Streaming (Kafka)
│
├── medallion/
│   ├── bronze/
│   │   └── bronze_layer.py     # Stockage brut dans MinIO
│   ├── silver/
│   │   └── silver_layer.py     # Nettoyage, normalisation, détection langue
│   └── gold/
│       └── gold_layer.py       # Agrégations : tendances, keywords, stats
│
├── transformation/
│   └── transform.py            # ETL Python / PySpark
│
├── orchestration/
│   └── dags/
│       └── news_pipeline_dag.py    # Airflow DAG (schedule toutes les heures)
│
├── warehouse/
│   └── warehouse.py            # Data Warehouse PostgreSQL (schéma en étoile)
│
├── visualization/
│   └── dashboard.py            # Dashboard Plotly Dash
│
├── quality/
│   └── quality.py              # Contrôles qualité (complétude, validité, cohérence)
│
├── governance/
│   └── governance.py           # Lineage, Catalogue, Journal des runs
│
├── docker/
│   └── postgres-init.sql       # Init PostgreSQL DW + Airflow
│
├── main.py                     # Pipeline runner (sans Airflow)
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

---

## Démarrage rapide

### Prérequis
- Docker & Docker Compose
- Python 3.11+
- 8 GB RAM minimum (pour la stack complète)

### 1. Lancer avec Docker Compose

```bash
git clone https://github.com/Walid-Ysn/news-bigdata-platform    
cd news-bigdata-platform

```

Services démarrés :

| Service       | URL                   | Credentials         |
|---------------|-----------------------|---------------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Airflow       | http://localhost:8080 | admin / admin123    |
| Grafana       | http://localhost:3000 | admin / admin123    |
| PostgreSQL    | localhost:5432        | dwuser / dwpassword |
| Kafka         | localhost:29092       | —                   |

### 2. Test local rapide (sans Docker)

```bash
pip install -r requirements.txt

# Pipeline complet avec données mock
python main.py --mock

# Scraping réel (connexion internet requise)
python main.py

# Dashboard uniquement
python main.py --dashboard
# → http://localhost:8050
```

---

## Pipeline en détail

### Étape 1 — Scraping

Sources supportées :

| Source       | Pays | Langue | Module         |
|--------------|------|--------|----------------|
| Hespress     | MA   | FR     | HespressScraper |
| Akhbarona    | MA   | AR     | AkhbaronaScraper |
| BBC News     | GB   | EN     | BBCScraper      |
| Al Jazeera   | QA   | EN     | AlJazeeraScraper |

Données collectées : `article_id`, `title`, `author`, `published_at`, `category`, `content`, `source`, `url`, `language`, `scraped_at`, `country`

### Étape 2 — Ingestion

- **Batch** : Scraping programmé toutes les heures → fichiers JSON dans MinIO
  - Partitionnement : `bronze/source=X/year=Y/month=M/day=D/`
- **Streaming** : Chaque article publié → événement Kafka (topic `news-articles-raw`)

### Étape 3 — Architecture Médaillon

**Bronze** : Données brutes, aucune modification, conservation de l'historique complet.

**Silver** : Nettoyage et normalisation :
- Suppression des balises HTML
- Normalisation Unicode (NFC)
- Compression des espaces
- Détection automatique de la langue (`langdetect` + heuristiques)
- Normalisation des dates en ISO 8601
- Déduplication par `article_id`
- Rejet des articles sans titre ou contenu < 100 caractères

**Gold** : Tables analytiques :
- `articles_per_source` — count par source
- `articles_per_day` — timeline quotidienne
- `articles_per_category` — tendances par thème
- `articles_per_country` — répartition géographique
- `language_distribution` — distribution FR/AR/EN
- `top_keywords` — mots-clés les plus fréquents
- `news_trends` — Top 10 catégories avec exemples

### Étape 4 — Transformation

Enrichissement Python / PySpark :
- Calcul `word_count`, `char_count`, `reading_time_min`
- Extraction `pub_year`, `pub_month`, `pub_day`, `pub_weekday`
- Flag `is_long_form` (> 800 mots)

### Étape 5 — Orchestration (Airflow)

DAG `news_pipeline_batch` :
- Schedule : `0 * * * *` (toutes les heures)
- Tasks : `scrape → [ingest_batch, ingest_streaming, bronze] → silver → transform → gold → load_warehouse → quality_check`
- Max active runs : 1 (pas de chevauchement)
- Retry : 2 tentatives, délai 5 min

### Étape 6 — Data Warehouse

Schéma en étoile PostgreSQL :

```
dim_source ──┐
dim_category─┤
dim_date ────┼──► fact_articles
dim_language─┘

Vues : v_articles_daily_by_source, v_language_distribution
```

### Étape 7 — Visualisation

Dashboard Plotly Dash sur `http://localhost:8050` :
- Timeline des articles par jour
- Répartition par source (bar chart horizontal)
- Tendances par catégorie
- Top mots-clés
- Distribution des langues (pie chart)
- KPIs : total articles, sources actives, catégories

### Étape 8 — Qualité des données

Tests automatiques sur chaque batch :

| Dimension   | Test                         | Seuil      |
|-------------|------------------------------|------------|
| Complétude  | Titre présent                | Obligatoire |
| Complétude  | Contenu > 100 chars / 30 mots | Obligatoire |
| Complétude  | Date présente                | Warning    |
| Complétude  | URL présente et valide       | Obligatoire |
| Validité    | Date non dans le futur       | Warning    |
| Validité    | Source connue                | Warning    |
| Cohérence   | Pas de doublon (article_id)  | Obligatoire |

Score qualité = articles 100% valides / total × 100
docker-compose up -d
### Étape 9 — Gouvernance

- **Data Lineage** : chaque transformation enregistrée (source, destination, nombre d'enregistrements, timestamp)
- **Catalogue de données** : description, schéma, owner, tags pour chaque dataset
- **Journal des runs** : historique de chaque exécution du pipeline
- **Quality Log** : historique des scores qualité

---

## Technologies utilisées

| Couche           | Technologie                    |
|------------------|-------------------------------|
| Scraping         | Python, BeautifulSoup, Scrapy |
| Ingestion Batch  | MinIO (S3-compatible)         |
| Ingestion Stream | Apache Kafka                  |
| Data Lake        | MinIO                         |
| Traitement       | Python, PySpark               |
| Orchestration    | Apache Airflow                |
| Data Warehouse   | PostgreSQL                    |
| Visualisation    | Plotly Dash, Grafana          |
| Conteneurisation | Docker, Docker Compose        |
| Langues NLP      | langdetect                    |

---

## Livrables

- [x] `scrapers/scraper.py` — Web scraping multi-sources
- [x] `ingestion/ingestion.py` — Batch + Streaming
- [x] `medallion/bronze/` — Data Lake Bronze
- [x] `medallion/silver/` — Nettoyage Silver
- [x] `medallion/gold/` — Agrégations Gold
- [x] `transformation/transform.py` — ETL Python/Spark
- [x] `orchestration/dags/news_pipeline_dag.py` — Airflow DAG
- [x] `warehouse/warehouse.py` — Data Warehouse PostgreSQL
- [x] `visualization/dashboard.py` — Dashboard Dash
- [x] `quality/quality.py` — Qualité des données
- [x] `governance/governance.py` — Gouvernance & Lineage
- [x] `Dockerfile`
- [x] `docker-compose.yml`
- [x] `README.md`

---

*EMSI Casablanca — Filière IADATA — Architecture de données 2025/2026*
*Date limite : 10 Mai 2026*
