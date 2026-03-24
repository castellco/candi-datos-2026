FROM rocker/r-ver:4.4.3

ENV DEBIAN_FRONTEND=noninteractive

ARG QUARTO_VERSION=1.6.42

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gdebi-core \
    ca-certificates \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libfontconfig1-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    && rm -rf /var/lib/apt/lists/*

# Quarto
RUN wget -q "https://github.com/quarto-dev/quarto-cli/releases/download/v${QUARTO_VERSION}/quarto-${QUARTO_VERSION}-linux-amd64.deb" -O /tmp/quarto.deb \
    && gdebi -n /tmp/quarto.deb \
    && rm -f /tmp/quarto.deb

WORKDIR /app

COPY . /app

# Instalar solo los paquetes necesarios (no tidyverse completo)
RUN R -q -e "install.packages(c( \
    'shiny', \
    'bslib', \
    'dplyr', \
    'tidyr', \
    'plotly', \
    'DT', \
    'duckdb', \
    'DBI', \
    'glue', \
    'scales', \
    'stringr', \
    'janitor', \
    'htmltools' \
  ), repos='https://cloud.r-project.org', Ncpus=max(1, parallel::detectCores()-1))"

EXPOSE 8080

CMD ["sh", "-c", "quarto serve /app/index.qmd --host 0.0.0.0 --port ${PORT:-8080} --no-browser"]