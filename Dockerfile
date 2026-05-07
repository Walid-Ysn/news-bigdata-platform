# ─────────────────────────────────────────────────────────
# News Big Data Platform — Dockerfile
# Image Python pour le pipeline de scraping et traitement
# ─────────────────────────────────────────────────────────

FROM python:3.11-slim

LABEL maintainer="EMSI IADATA"
LABEL description="News Big Data Pipeline"
LABEL version="1.0"

# Variables d'environnement
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

# Dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Répertoire de travail
WORKDIR /app

# Dépendances Python
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Code source
COPY . .

# Créer les répertoires de données
RUN mkdir -p /tmp/datalake/bronze /tmp/datalake/silver /tmp/datalake/gold

# Exposer le port pour le dashboard (si lancé en mode web)
EXPOSE 8050

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8050/health')" || exit 0

# Commande par défaut : lancer le pipeline complet
CMD ["python", "-m", "scrapers.scraper"]
