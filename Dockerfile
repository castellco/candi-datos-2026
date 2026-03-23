FROM ghcr.io/quarto-dev/quarto-r:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libmagick++-dev \
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

WORKDIR /app
COPY . /app

RUN R -q -e "install.packages(c('shiny','bslib','dplyr','ggplot2','plotly','DT','duckdb','DBI','glue','scales','stringr','ggtext','tidyverse','readxl','janitor','ggborderline','patchwork','magick'), repos='https://cloud.r-project.org', Ncpus=max(1, parallel::detectCores()-1))"

EXPOSE 8080

CMD ["sh", "-c", "quarto serve /app/index.qmd --host 0.0.0.0 --port ${PORT:-8080} --no-browser"]
