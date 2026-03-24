library(tidyverse)
library(readxl)
library(janitor)
library(stringr)
library(ggtext)
library(ggborderline)
library(scales)

# systemfonts::system_fonts() |> View()

# fuente ------------------------------------------------------------------
font_family = "Cabin"
caption_sep = " ■ "

linewidth_theme = 0.2

# textos ------------------------------------------------------------------
# created_by <- 
# textos ------------------------------------------------------------------
twitter_icon <- "&#xe61b"
twitter_username <- "@castellco_"

social_caption <- glue::glue(
  "<span style='font-family:\"Cabin\";'>{twitter_icon};</span><span style='color: #383838'>{twitter_username}</span>"
)

set_textos <- function(titulo, subtitulo) {
  assign("titulo", titulo, envir = .GlobalEnv)
  assign("subtitulo", subtitulo, envir = .GlobalEnv)
}

set_caption <- function() {
  paste0(nota, 
        caption_sep, 
        # created_by, 
        "**Gráfico**: ",
        social_caption,
        caption_sep, 
        "**Fuente**: ", 
        source)
}

# para el pipe de ggplot
add_labs <- function() {
    labs(
      title = titulo,
      subtitle = subtitulo
    )
}



# función para añadir anotaciones -------------------------------
add_text_annotations <- function(x, y, label) {
  annotate(
    geom = "richtext",
    x = x,
    y = y,
    label = label,
    family = font_family,
    size = 26 / .pt,
    color = contraste,
    fill = background,
    label.color = background,
    hjust = 0,
    vjust = 0.5
  )
}


# argumentos --------------------------------------------------------------
linewidth_anotaciones = 1
arrow_anotaciones = arrow(length = unit(0.5, 'cm'), type = "closed")

