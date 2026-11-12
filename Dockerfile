# Une seule image pour les trois services Dagster (webserver, démon, code) :
# même version de Dagster partout, un seul build.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    GAZETTE_DATA_DIR=/data \
    GAZETTE_RAPPORTS_DIR=/rapports \
    PYTHONPATH=/opt/gazette \
    HF_HOME=/opt/models \
    TZ=Europe/Paris

WORKDIR /opt/gazette

# torch en version CPU (index PyTorch déclaré dans requirements.txt) : pas de CUDA dans l'image.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Le modèle d'embeddings est téléchargé une fois, ici, à la version figée dans la config :
# les runs n'ont ensuite plus besoin du réseau.
COPY config ./config
COPY gazette ./gazette
RUN python -m gazette.themes.telecharger
ENV HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1

COPY dagster.yaml workspace.yaml ${DAGSTER_HOME}/
COPY pyproject.toml ./
COPY docs/sources.csv ./docs/sources.csv
COPY tests ./tests

# Le commit du code, inscrit dans chaque run de regroupement (`runs.version_code`).
ARG GAZETTE_VERSION_CODE
ENV GAZETTE_VERSION_CODE=${GAZETTE_VERSION_CODE}

RUN mkdir -p /data /rapports /opt/dagster/storage
