# ============================================================
# Dockerfile — CandiDATOS 2026 para Hugging Face Spaces
# ============================================================
FROM rocker/shiny-verse:4.4.1

RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev libssl-dev libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

# httpuv dev — requerimiento oficial de HF para evitar timeouts
RUN Rscript -e "install.packages('remotes', repos='https://cloud.r-project.org')" \
 && Rscript -e "remotes::install_github('rstudio/httpuv')"

# Paquetes adicionales (tidyverse + readr ya vienen en shiny-verse)
RUN Rscript -e "install.packages(c('bslib','plotly','DT'), repos='https://cloud.r-project.org')"

WORKDIR /app
COPY app.R  /app/app.R
COPY data/  /app/data/

EXPOSE 7860

CMD ["R", "--quiet", "-e", "shiny::runApp('/app', host='0.0.0.0', port=7860)"]
