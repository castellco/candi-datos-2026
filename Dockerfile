# ============================================================
# Dockerfile — CandiDATOS 2026
# Para Hugging Face Spaces (puerto 7860 obligatorio)
# Base: rocker/shiny-verse (incluye tidyverse, sin instalar por separado)
# ============================================================

FROM rocker/shiny-verse:4.4.1

# Dependencias de sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

# REQUERIMIENTO OFICIAL DE HF SPACES:
# Usar la versión de desarrollo de httpuv para evitar timeouts
RUN install2.r --error remotes \
    && Rscript -e "remotes::install_github('rstudio/httpuv')"

# Paquetes R adicionales (tidyverse ya viene en rocker/shiny-verse)
RUN install2.r --error \
    bslib \
    plotly \
    DT \
    duckdb \
    DBI \
    glue \
    janitor \
    && rm -rf /tmp/downloaded_packages

# Copiar la app
WORKDIR /app
COPY app.R    /app/app.R
COPY data/    /app/data/
COPY assets/  /app/assets/

# HF Spaces requiere puerto 7860
EXPOSE 7860

CMD ["R", "--quiet", "-e", "shiny::runApp('/app', host='0.0.0.0', port=7860)"]
