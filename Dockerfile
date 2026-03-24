# Imagen base con R
FROM rocker/r-ver:4.3.1

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar Quarto
RUN curl -LO https://quarto.org/download/latest/quarto-linux-amd64.deb \
    && dpkg -i quarto-linux-amd64.deb \
    && rm quarto-linux-amd64.deb

# Instalar paquetes R (ajusta si falta alguno)
RUN R -e "install.packages(c('shiny','quarto','dplyr','readr','ggplot2', 'duckdb', 'bslib', 'plotly', 'DT', 'DBI', 'glue', 'scales', 'stringr', 'janitor', 'tidyr'), repos='https://cloud.r-project.org')"

# Copiar tu app
WORKDIR /app
COPY . /app

# Exponer puerto (Railway usa PORT dinámico)
EXPOSE 3838

# Comando de arranque
CMD ["Rscript", "-e", "port <- as.numeric(Sys.getenv('PORT', '3838')); quarto::quarto_serve('index.qmd', host='0.0.0.0', port=port)"]