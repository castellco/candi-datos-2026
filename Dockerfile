FROM rocker/r-ver:4.3.1

# Dependencias del sistema
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    curl \
    pandoc \
    && rm -rf /var/lib/apt/lists/*

# Instalar Quarto
RUN curl -LO https://quarto.org/download/latest/quarto-linux-amd64.deb \
    && dpkg -i quarto-linux-amd64.deb \
    && rm quarto-linux-amd64.deb

# Instalar paquetes R (LOS QUE USA TU APP)
RUN R -e "install.packages(c( \
  'shiny','bslib','dplyr','ggplot2','plotly','DT','duckdb','DBI', \
  'glue','scales','stringr','janitor','tidyr','readr' \
), repos='https://cloud.r-project.org')"

# Copiar proyecto
WORKDIR /app
COPY . /app

# Puerto (Railway usa PORT dinámico)
EXPOSE 3838

# Comando de arranque (CLAVE)
CMD ["Rscript", "-e", "port <- as.numeric(Sys.getenv('PORT', '3838')); quarto::quarto_serve('index.qmd', host='0.0.0.0', port=port)"]