library(quarto)

port <- as.numeric(Sys.getenv("PORT"))

quarto::quarto_serve(
  input = "index.qmd",
  host = "0.0.0.0",
  port = port
)