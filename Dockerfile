# Dockerfile
FROM rocker/shiny:4.2.2

RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev libssl-dev libxml2-dev libfontconfig1 fonts-liberation \
    libfreetype6-dev libpng-dev libjpeg-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /srv/shiny-server
COPY . /srv/shiny-server

# Instala CRAN listados en packages.txt
COPY packages.txt /tmp/packages.txt
RUN R -e "pkg <- readLines('/tmp/packages.txt'); pkg <- pkg[nzchar(pkg)]; if (length(pkg)) install.packages(pkg, repos='https://cloud.r-project.org')"

# Ejecuta install.R si existe (GitHub/Bioconductor extra)
RUN if [ -f /srv/shiny-server/install.R ]; then Rscript /srv/shiny-server/install.R ; fi

EXPOSE 8080
CMD ["R","-e","shiny::runApp('/srv/shiny-server', host='0.0.0.0', port=as.integer(Sys.getenv('PORT','8080')) )"]