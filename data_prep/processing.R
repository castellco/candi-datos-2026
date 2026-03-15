# setwd('./candi-datos-2026')
# here::here()

# librerías ---------------------------------------------------------------
library(duckdb)
library(glue)
library(DBI)
library(kableExtra)


# conexión con ddbb duckdb ------------------------------------------------

con <- dbConnect(duckdb(), "candi-datos.duckdb")

# cargar tablas a la bbdd duckdb ------------------------------------------
tablas <- c(
  "candidatos",
  "estudios_universitarios",
  "estudios_posgrado",
  "estudios_tecnicos",
  "estudios_no_universitarios",
  "experiencia_laboral",
  "cargos_partidarios",
  "eleccion_popular",
  "sentencias",
  "bienes_muebles_inmuebles",
  "informacion_adicional"
)


for (tabla in tablas) {
  dbExecute(
    con,
    glue(
      "CREATE OR REPLACE TABLE {tabla} AS
      SELECT * FROM read_csv_auto('data/{tabla}.csv', normalize_names=true)"
    )
  )
}

# listar todas las tablas cargadas ----------------------------------------
tablas_db <- dbGetQuery(con, "SHOW TABLES")
print(tablas_db)


# N candidatos ------------------------------------------------------------

### Nivel nacional --------------------------------------------------------
nro_candidatos <- dbGetQuery(
  con,
  "
  SELECT COUNT (DISTINCT dni) FROM candidatos
  "
)

print(paste("Cantidad de candidatos en estas elecciones:", nro_candidatos))


### Diputados -------------------------------------------------------------
nro_candidatos_diputados <- dbGetQuery(
  con,
  "
  SELECT COUNT (DISTINCT dni) FROM candidatos
  WHERE cargo_al_que_postula = 'Diputados'
  "
)

print(paste(
  "Cantidad de candidatos que postulan como Diputados:",
  nro_candidatos_diputados
))


### Senadores -------------------------------------------------------------
nro_candidatos_senadores <- dbGetQuery(
  con,
  "
  SELECT COUNT (DISTINCT dni) FROM candidatos
  WHERE cargo_al_que_postula = 'Senadores'
  "
)

print(paste(
  "Cantidad de candidatos que postulan como Senadores:",
  nro_candidatos_senadores
))


### Parlamento Andino -----------------------------------------------------
nro_candidatos_andino <- dbGetQuery(
  con,
  "
  SELECT COUNT (DISTINCT dni) FROM candidatos
  WHERE 
  "
)


# candidatos con sentencias -----------------------------------------------

nro_candidatos_sentencias <- dbGetQuery(
  con,
  "SELECT COUNT(DISTINCT dni) AS n
   FROM sentencias"
)
nro_candidatos_sentencias


#  ------------------------------------------------------------------------
View(dbGetQuery(con, "SELECT * FROM candidatos LIMIT 5"))
View(dbGetQuery(con, "SELECT * FROM sentencias LIMIT 5"))
