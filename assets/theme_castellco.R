source("assets/functions.R")

# Paleta de colores
gris_claro       <- "#464743"
rojo             <- "#be2e1c"
background_oscuro <- "#222831"
blanco           <- "#f9ffee"
amarillo <-  "#f9ffee"

# Constantes del tema
font_family     <- "Cabin"
linewidth_theme <- 0.3

# ── Tema principal ────────────────────────────────────────────────────────────
#
# rayas:  "verticales" (grids en eje x) | "horizontales" (grids en eje y)
# fondo:  "oscuro" | "claro"

theme_castellco <- function(base_size = 11, rayas = "verticales", fondo = "oscuro", ...) {

  bg    <- if (fondo == "oscuro") background_oscuro else "#ffffff"
  fg    <- if (fondo == "oscuro") blanco            else "#222222"
  grid  <- if (fondo == "oscuro") gris_claro        else "#dddddd"

  grid_x <- if (rayas == "verticales")   ggplot2::element_line(color = grid, linewidth = linewidth_theme, linetype = "solid")
             else                         ggplot2::element_blank()

  grid_y <- if (rayas == "horizontales") ggplot2::element_line(color = grid, linewidth = linewidth_theme, linetype = "solid")
             else                         ggplot2::element_blank()

  ggplot2::theme_bw(base_size = base_size) +
    ggplot2::theme(
      text             = ggplot2::element_text(family = font_family, color = fg),
      plot.background  = ggplot2::element_rect(fill = bg, colour = NA),
      panel.background = ggplot2::element_rect(fill = bg, colour = NA),
      plot.margin      = grid::unit(c(1.0, 0.8, 0.8, 0.9), "cm"),

      plot.title = ggtext::element_textbox_simple(
        color      = fg,
        size       = base_size + 2,
        face       = "bold",
        lineheight = 1.05,
        margin     = ggplot2::margin(b = 6)
      ),
      plot.title.position = "plot",

      plot.subtitle = ggtext::element_textbox_simple(
        color      = fg,
        size       = base_size,
        lineheight = 1.1,
        margin     = ggplot2::margin(b = 8)
      ),

      plot.caption = ggtext::element_textbox_simple(
        color  = fg,
        size   = base_size - 3,
        margin = ggplot2::margin(t = 8, b = 2)
      ),
      plot.caption.position = "plot",

      panel.grid.minor   = ggplot2::element_blank(),
      panel.grid.major.x = grid_x,
      panel.grid.major.y = grid_y,
      panel.border       = ggplot2::element_blank(),

      legend.background = ggplot2::element_rect(fill = bg, colour = NA),
      legend.key        = ggplot2::element_rect(fill = bg, colour = NA),
      legend.title      = ggplot2::element_text(family = font_family, color = fg, size = base_size - 2),
      legend.text       = ggplot2::element_text(family = font_family, color = fg, size = base_size - 2),

      strip.background = ggplot2::element_blank(),
      strip.text       = ggplot2::element_text(face = "bold", color = fg, size = base_size - 2),

      axis.title  = ggplot2::element_blank(),
      axis.text.x = ggplot2::element_text(color = fg, size = base_size - 2),
      axis.text.y = ggplot2::element_text(color = fg, size = base_size - 2),
      axis.ticks  = ggplot2::element_blank(),

      ...
    )
}
