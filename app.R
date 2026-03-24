# ============================================================
# CandiDATOS 2026 — app.R
# Convertido desde index.qmd para deployment en shinyapps.io
# ============================================================

library(shiny)
library(bslib)
library(dplyr)
library(ggplot2)
library(plotly)
library(DT)
library(duckdb)
library(DBI)
library(glue)
library(scales)
library(stringr)
library(janitor)
library(tidyr)

options(scipen = 999)

# ── Paleta de colores (inline, sin depender de theme_castellco.R) ─────────────
rojo             <- "#be2e1c"
blanco           <- "#ffffff"
amarillo         <- "#febd17"
background_oscuro <- "#2b2b2b"
gris_claro       <- "#464743"

# ── Helpers ───────────────────────────────────────────────────────────────────
oscuro <- function(p, t = 80) {
  p |> layout(
    paper_bgcolor = background_oscuro,
    plot_bgcolor  = background_oscuro,
    font = list(color = blanco, family = "Cabin", size = 12),
    xaxis = list(color = blanco, gridcolor = gris_claro, zerolinecolor = gris_claro,
                 tickfont = list(family = "Cabin"), titlefont = list(family = "Cabin")),
    yaxis = list(color = blanco, gridcolor = "transparent",
                 tickfont = list(family = "Cabin", size = 13),
                 tickmode = "linear", tick0 = 0, dtick = 1,
                 automargin = TRUE, title = ""),
    legend = list(font = list(color = blanco, family = "Cabin")),
    margin = list(l = 5, r = 15, t = t, b = 5)
  )
}

titulo_plotly <- function(texto, subtitulo = NULL) {
  txt <- if (!is.null(subtitulo))
    paste0("<b>", texto, "</b><br><sup>", subtitulo, "</sup>")
  else
    paste0("<b>", texto, "</b>")
  list(text = txt, font = list(color = blanco, family = "Cabin", size = 18),
       x = 0.5, xanchor = "center")
}

n_label <- function(d) paste0("N = ", comma(n_distinct(d$dni)), " candidatos")

parsear_soles <- function(x) {
  suppressWarnings(as.numeric(str_replace_all(str_remove_all(x, "S/|\\s"), ",", "")))
}

crear_labels_unicos <- function(nombres, largo = 25) {
  truncados <- str_trunc(nombres, largo)
  dupes <- duplicated(truncados) | duplicated(truncados, fromLast = TRUE)
  if (any(dupes)) {
    idx_list <- split(seq_along(truncados)[dupes], truncados[dupes])
    for (grp in idx_list) {
      for (k in seq_along(grp)) {
        truncados[grp[k]] <- paste0(str_trunc(nombres[grp[k]], largo - 4), " (", k, ")")
      }
    }
  }
  setNames(truncados, nombres)
}

DT_OPCIONES <- list(
  pageLength = 15, scrollX = TRUE, dom = "frtip",
  rowCallback = JS("
    function(row, data) {
      $(row).css({'background-color': '#383838', 'color': '#ffffff'});
      $('td', row).css({'border-color': '#464743'});
    }
  "),
  initComplete = JS("
    function(settings, json) {
      var api = this.api();
      $(api.table().header()).css({
        'background-color': '#464743',
        'color': '#ffffff',
        'border-bottom': '2px solid #be2e1c'
      });
      $(api.table().node()).css('background-color', '#383838');
      $(api.table().container()).find('.dataTables_info, .dataTables_paginate, .paginate_button')
        .css({'color': '#ffffff', 'background-color': '#383838'});
    }
  "),
  drawCallback = JS("
    function(settings) {
      $('.dataTables_paginate, .dataTables_info').css({'color': '#ffffff'});
      $('.paginate_button').css({'color': '#ffffff !important'});
    }
  "),
  language = list(url = "//cdn.datatables.net/plug-ins/1.10.11/i18n/Spanish.json")
)

# ── Carga de datos (una sola vez al iniciar) ──────────────────────────────────
con <- dbConnect(duckdb())

tablas <- c(
  "candidatos", "estudios_universitarios", "estudios_posgrado",
  "estudios_tecnicos", "estudios_no_universitarios", "experiencia_laboral",
  "cargos_partidarios", "eleccion_popular", "sentencias",
  "bienes_muebles_inmuebles", "informacion_adicional", "partidos"
)

for (tabla in tablas) {
  invisible(dbExecute(con, glue(
    "CREATE OR REPLACE TABLE {tabla} AS
     SELECT * FROM read_csv_auto('data/{tabla}.csv', normalize_names = true)"
  )))
}

candidatos <- dbGetQuery(con, "SELECT * FROM candidatos") |>
  clean_names() |>
  mutate(
    total_ingresos_num = parsear_soles(total_ingresos),
    rem_publico_num    = parsear_soles(rem_publico),
    rem_privado_num    = parsear_soles(rem_privado)
  )

sentencias_df  <- dbGetQuery(con, "SELECT * FROM sentencias") |> clean_names()
exp_laboral_raw <- dbGetQuery(con, "SELECT * FROM experiencia_laboral") |> clean_names()

exp_anos_por_dni <- exp_laboral_raw |>
  mutate(
    anio_inicio = suppressWarnings(as.numeric(str_extract(exp_laboral_periodo, "^\\d{4}"))),
    anio_fin    = suppressWarnings(as.numeric(str_extract(exp_laboral_periodo, "\\d{4}$"))),
    anos = if_else(!is.na(anio_inicio) & !is.na(anio_fin),
                   as.numeric(anio_fin - anio_inicio + 1), NA_real_)
  ) |>
  filter(!is.na(anos), anos >= 0, anos <= 60) |>
  group_by(dni) |>
  summarise(total_anos_exp = sum(anos, na.rm = TRUE), .groups = "drop")

partidos_list <- sort(unique(candidatos$partido[!is.na(candidatos$partido)]))
sexos_list    <- sort(unique(candidatos$sexo[!is.na(candidatos$sexo)]))
deps_list     <- sort(unique(candidatos$departamento[!is.na(candidatos$departamento)]))
cargos_list   <- sort(unique(candidatos$cargo_al_que_postula[!is.na(candidatos$cargo_al_que_postula)]))

cargos_presidenciales <- cargos_list[
  str_detect(cargos_list, regex("president|vicepres", ignore_case = TRUE))
]

label_partido <- crear_labels_unicos(partidos_list)

dbDisconnect(con, shutdown = TRUE)

# ── CSS personalizado ─────────────────────────────────────────────────────────
custom_css <- "
  body, .bslib-page-dashboard { background-color: #2b2b2b !important; color: #ffffff; }
  .navbar, .navbar-default { background-color: #1a1a1a !important; border-bottom: 3px solid #be2e1c; }
  .navbar-brand, .navbar-nav > li > a { color: #ffffff !important; font-family: 'Cabin', sans-serif; }
  .nav-link.active { border-bottom: 3px solid #be2e1c !important; color: #febd17 !important; }
  .card { background-color: #383838 !important; border: 1px solid #464743; }
  .card-header { background-color: #464743 !important; color: #ffffff; }
  .sidebar { background-color: #1e1e1e !important; border-right: 2px solid #be2e1c; }
  .form-select, .form-control { background-color: #383838 !important; color: #ffffff !important; border-color: #464743 !important; }
  .form-check-label { color: #ffffff !important; }
  .form-check-input:checked { background-color: #be2e1c !important; border-color: #be2e1c !important; }
  h1, h2, h3, h4, h5 { font-family: 'Cabin', sans-serif; color: #ffffff; }
  .value-box { font-family: 'Cabin', sans-serif; }
  .small-text { font-size: 0.8rem; color: #aaaaaa; }
  a { color: #febd17 !important; }
  @import url('https://fonts.googleapis.com/css2?family=Cabin:wght@400;600;700&display=swap');
"

# ── UI ────────────────────────────────────────────────────────────────────────
ui <- page_navbar(
  title = tags$span(
    tags$b("CandiDATOS"),
    tags$span(" · Elecciones Generales del Perú 2026",
              style = "font-size: 0.85em; color: #aaaaaa;")
  ),
  theme = bs_theme(
    bootswatch = "litera",
    bg = background_oscuro, fg = blanco,
    primary = rojo, secondary = gris_claro,
    base_font = font_google("Cabin")
  ),
  fillable = FALSE,
  tags$head(tags$style(HTML(custom_css))),

  # ── Sidebar compartido ──────────────────────────────────────────────────────
  sidebar = sidebar(
    width = 260,
    bg = "#1e1e1e",
    tags$p("Información declarada por los/as candidatos/as.",
           style = "color: #aaaaaa; font-size: 0.85rem;"),
    tags$hr(style = "border-color: #be2e1c;"),
    tags$b("Filtra por:", style = "color: #ffffff;"),
    br(),
    selectInput("dep", "Departamento",
                choices = c("Todos" = "__ALL__", deps_list), selected = "__ALL__"),
    selectInput("partido", "Partido",
                choices = c("Todos" = "__ALL__", partidos_list), selected = "__ALL__"),
    selectInput("cargo", "Cargo",
                choices = c("Todos" = "__ALL__", cargos_list), selected = "__ALL__"),
    checkboxGroupInput("sexo", "Sexo",
                       choices = sexos_list, selected = sexos_list, inline = TRUE),
    radioButtons("sentencia_filter", "Sentencias",
                 choices = c("Todos" = "todos", "Con sentencias" = "con", "Sin sentencias" = "sin"),
                 selected = "todos", inline = TRUE),
    tags$hr(style = "border-color: #464743;"),
    tags$p(HTML("<b>Fuente:</b> <a href='https://votoinformado.jne.gob.pe' target='_blank'>JNE — Voto Informado</a><br>Datos al 1 de marzo de 2026"),
           style = "color: #aaaaaa; font-size: 0.8rem;"),
    tags$p(HTML("Extracción y visualización:<br><b>Carolina Cornejo Castellano</b><br>
                 <a href='https://x.com/castellco_' target='_blank'>X</a> ·
                 <a href='https://www.linkedin.com/in/cornejocastellano' target='_blank'>LinkedIn</a> ·
                 <a href='https://github.com/castellco' target='_blank'>GitHub</a>"),
           style = "color: #aaaaaa; font-size: 0.8rem;")
  ),

  # ── TAB 1: Por partido ──────────────────────────────────────────────────────
  nav_panel(
    "Por partido",
    layout_columns(
      col_widths = rep(2, 6),
      uiOutput("vb_total_p"),
      uiOutput("vb_diputados_p"),
      uiOutput("vb_senadores_p"),
      uiOutput("vb_andino_p"),
      uiOutput("vb_presidencia_p"),
      uiOutput("vb_sentencias_p")
    ),
    br(),
    layout_columns(
      col_widths = c(6, 6),
      card(plotlyOutput("plot_sentencias_partido", height = "520px")),
      card(plotlyOutput("plot_exp_laboral",        height = "520px"))
    ),
    layout_columns(
      col_widths = c(6, 6),
      card(plotlyOutput("plot_ingresos_partido", height = "520px")),
      card(plotlyOutput("plot_ingreso_tipo",     height = "520px"))
    ),
    card(plotlyOutput("plot_sexo_partido", height = "520px")),
    card(
      card_header("Resumen por partido"),
      DTOutput("tabla_partidos", width = "100%")
    )
  ),

  # ── TAB 2: Por candidatos ───────────────────────────────────────────────────
  nav_panel(
    "Por candidatos",
    layout_columns(
      col_widths = rep(2, 6),
      uiOutput("vb_total_c"),
      uiOutput("vb_diputados_c"),
      uiOutput("vb_senadores_c"),
      uiOutput("vb_andino_c"),
      uiOutput("vb_presidencia_c"),
      uiOutput("vb_partidos_c")
    ),
    br(),
    layout_columns(
      col_widths = c(6, 6),
      card(plotlyOutput("plot_ranking_candidatos_sent", height = "600px")),
      card(plotlyOutput("plot_ranking_sentencias",      height = "500px"))
    ),
    layout_columns(
      col_widths = c(6, 6),
      card(plotlyOutput("plot_ranking_ingresos_cand", height = "600px")),
      card(plotlyOutput("plot_treemap",               height = "500px"))
    ),
    card(plotlyOutput("plot_sin_trayectoria", height = "500px")),
    card(
      card_header(uiOutput("tabla_caption")),
      DTOutput("tabla_candidatos", width = "100%")
    )
  )
)

# ── SERVER ────────────────────────────────────────────────────────────────────
server <- function(input, output, session) {

  # Datos filtrados
  fc <- reactive({
    req(input$sexo)
    d <- candidatos |> filter(sexo %in% input$sexo)
    if (!identical(input$dep,     "__ALL__")) d <- d |> filter(departamento == input$dep)
    if (!identical(input$partido, "__ALL__")) d <- d |> filter(partido == input$partido)
    if (!identical(input$cargo,   "__ALL__")) d <- d |> filter(cargo_al_que_postula == input$cargo)
    if (input$sentencia_filter == "con") d <- d |> filter(sentencias == 1)
    if (input$sentencia_filter == "sin") d <- d |> filter(sentencias == 0 | is.na(sentencias))
    d
  })

  fc_uniq <- reactive(fc() |> distinct(dni, .keep_all = TRUE))

  partido_click <- reactiveVal(NULL)

  observeEvent(event_data("plotly_click", source = "src_ingresos"), {
    cl <- event_data("plotly_click", source = "src_ingresos")
    if (!is.null(cl$customdata)) partido_click(cl$customdata)
  })
  observeEvent(event_data("plotly_click", source = "src_sentencias"), {
    cl <- event_data("plotly_click", source = "src_sentencias")
    if (!is.null(cl$customdata)) partido_click(cl$customdata)
  })

  fc_tabla <- reactive({
    d <- fc_uniq()
    if (!is.null(partido_click())) d <- d |> filter(partido == partido_click())
    d
  })

  # Helper vbox
  vbox <- function(title, value) {
    bslib::value_box(title = title, value = value,
                     theme = value_box_theme(bg = rojo, fg = blanco))
  }

  # Value boxes — tab partido
  output$vb_total_p     <- renderUI(vbox("Candidatos",      n_distinct(fc()$dni)))
  output$vb_diputados_p <- renderUI(vbox("Diputados/as",    n_distinct(fc()$dni[fc()$cargo_al_que_postula == "Diputados"])))
  output$vb_senadores_p <- renderUI(vbox("Senadores/as",    n_distinct(fc()$dni[fc()$cargo_al_que_postula == "Senadores"])))
  output$vb_andino_p    <- renderUI(vbox("Parlamento Andino", n_distinct(fc()$dni[fc()$cargo_al_que_postula == "Parlamento Andino"])))
  output$vb_presidencia_p <- renderUI(vbox("Presidencia",   n_distinct(fc()$dni[fc()$cargo_al_que_postula %in% cargos_presidenciales])))
  output$vb_sentencias_p  <- renderUI(vbox("Con sentencias", n_distinct(fc()$dni[fc()$sentencias == 1])))

  # Value boxes — tab candidatos
  output$vb_total_c     <- renderUI(vbox("Candidatos",      n_distinct(fc()$dni)))
  output$vb_diputados_c <- renderUI(vbox("Diputados/as",    n_distinct(fc()$dni[fc()$cargo_al_que_postula == "Diputados"])))
  output$vb_senadores_c <- renderUI(vbox("Senadores/as",    n_distinct(fc()$dni[fc()$cargo_al_que_postula == "Senadores"])))
  output$vb_andino_c    <- renderUI(vbox("Parlamento Andino", n_distinct(fc()$dni[fc()$cargo_al_que_postula == "Parlamento Andino"])))
  output$vb_presidencia_c <- renderUI(vbox("Presidencia",   n_distinct(fc()$dni[fc()$cargo_al_que_postula %in% cargos_presidenciales])))
  output$vb_partidos_c  <- renderUI(vbox("Partidos",        n_distinct(fc()$partido)))

  # ── 1. Ingreso promedio por partido ─────────────────────────────────────────
  output$plot_ingresos_partido <- renderPlotly({
    d <- fc_uniq() |> filter(!is.na(total_ingresos_num))
    validate(need(nrow(d) > 0, "Sin datos de ingresos para la selección actual."))

    datos <- d |>
      summarise(promedio = mean(total_ingresos_num), total = sum(total_ingresos_num),
                n_cand = n_distinct(dni), .by = partido) |>
      arrange(promedio) |>
      mutate(etiqueta = dollar(promedio, prefix = "S/ ", big.mark = ",", accuracy = 1))

    plot_ly(datos,
      x = ~promedio, y = ~reorder(partido, promedio),
      customdata = ~partido, source = "src_ingresos",
      type = "bar", orientation = "h", marker = list(color = rojo),
      text = ~paste0(partido, "<br>Promedio: ", etiqueta,
                     "<br>Candidatos: ", n_cand,
                     "<br>Total: ", dollar(total, prefix = "S/ ", big.mark = ",", accuracy = 1)),
      hoverinfo = "text",
      texttemplate = ~etiqueta, textposition = "outside", cliponaxis = FALSE,
      textfont = list(color = blanco, size = 12, family = "Cabin")
    ) |> layout(
      title = titulo_plotly("Ingresos promedio por partido", paste0("En S/ · ", n_label(d))),
      xaxis = list(color = blanco, gridcolor = gris_claro, tickformat = ",.0f",
                   tickprefix = "S/ ", tickfont = list(family = "Cabin")),
      yaxis = list(color = blanco, tickfont = list(family = "Cabin", size = 12),
                   tickmode = "linear", tick0 = 0, dtick = 1, automargin = TRUE, title = ""),
      paper_bgcolor = background_oscuro, plot_bgcolor = background_oscuro,
      font = list(color = blanco, family = "Cabin"),
      margin = list(l = 5, r = 15, t = 90, b = 5)
    ) |> event_register("plotly_click")
  })

  # ── 2. Sentencias por partido ────────────────────────────────────────────────
  output$plot_sentencias_partido <- renderPlotly({
    d <- fc_uniq()
    datos <- d |>
      summarise(n = sum(sentencias == 1, na.rm = TRUE), total = n_distinct(dni), .by = partido) |>
      filter(n > 0) |> arrange(n)
    validate(need(nrow(datos) > 0, "Sin candidatos con sentencias en la selección."))

    plot_ly(datos,
      x = ~n, y = ~reorder(partido, n),
      customdata = ~partido, source = "src_sentencias",
      type = "bar", orientation = "h", marker = list(color = rojo),
      text = ~paste0(partido, "<br>Con sentencias: ", n, " de ", total),
      hoverinfo = "text",
      texttemplate = ~as.character(n), textposition = "outside", cliponaxis = FALSE,
      textfont = list(color = blanco, size = 12, family = "Cabin")
    ) |> layout(
      title = titulo_plotly("Candidatos con sentencias por partido", n_label(d)),
      xaxis = list(color = blanco, gridcolor = gris_claro, tickfont = list(family = "Cabin")),
      yaxis = list(color = blanco, tickfont = list(family = "Cabin", size = 12),
                   tickmode = "linear", tick0 = 0, dtick = 1, automargin = TRUE, title = ""),
      paper_bgcolor = background_oscuro, plot_bgcolor = background_oscuro,
      font = list(color = blanco, family = "Cabin"),
      margin = list(l = 5, r = 15, t = 90, b = 5)
    ) |> event_register("plotly_click")
  })

  # ── 3. Origen de ingresos público vs privado (stacked 100%) ─────────────────
  output$plot_ingreso_tipo <- renderPlotly({
    base <- fc_uniq() |>
      summarise(
        publico = sum(rem_publico_num, na.rm = TRUE),
        privado = sum(rem_privado_num, na.rm = TRUE),
        .by = partido
      ) |>
      filter(publico + privado > 0) |>
      mutate(total = publico + privado,
             pct_pub  = round(publico / total * 100, 1),
             pct_priv = round(privado / total * 100, 1)) |>
      arrange(pct_pub)
    validate(need(nrow(base) > 0, "Sin datos de remuneraciones en la selección."))

    largo <- base |>
      pivot_longer(cols = c(pct_pub, pct_priv), names_to = "tipo", values_to = "pct") |>
      mutate(tipo_label = if_else(tipo == "pct_pub", "Público", "Privado"),
             tooltip = paste0(partido, "<br>", tipo_label, ": ", pct, "%"))

    plot_ly(largo |> filter(tipo == "pct_pub"),
      x = ~pct, y = ~partido, name = "Público",
      type = "bar", orientation = "h", marker = list(color = rojo),
      text = ~tooltip, hoverinfo = "text",
      texttemplate = ~paste0(pct, "%"), textposition = "auto",
      textfont = list(color = blanco, size = 12, family = "Cabin")
    ) |>
    add_trace(
      data = largo |> filter(tipo == "pct_priv"),
      x = ~pct, y = ~partido, name = "Privado",
      marker = list(color = amarillo),
      text = ~tooltip, hoverinfo = "text",
      texttemplate = ~paste0(pct, "%"), textposition = "auto",
      textfont = list(color = background_oscuro, size = 12, family = "Cabin")
    ) |> layout(
      barmode = "stack",
      title = titulo_plotly("Origen de ingresos por partido",
                            paste0("% público vs. privado · ", n_label(fc_uniq()))),
      xaxis = list(color = blanco, gridcolor = gris_claro, ticksuffix = "%",
                   range = c(0, 100), tickfont = list(family = "Cabin")),
      yaxis = list(color = blanco, tickfont = list(family = "Cabin", size = 12),
                   tickmode = "linear", tick0 = 0, dtick = 1, automargin = TRUE, title = "",
                   categoryorder = "array",
                   categoryarray = base$partido[order(base$pct_pub)]),
      legend = list(orientation = "h", x = 0.5, xanchor = "center", y = 1.08,
                    font = list(color = blanco, family = "Cabin")),
      paper_bgcolor = background_oscuro, plot_bgcolor = background_oscuro,
      font = list(color = blanco, family = "Cabin"),
      margin = list(l = 5, r = 15, t = 110, b = 5)
    )
  })

  # ── 4. Distribución por sexo por partido (stacked 100%) ──────────────────────
  output$plot_sexo_partido <- renderPlotly({
    base <- fc_uniq() |>
      filter(!is.na(sexo)) |>
      count(partido, sexo, name = "n") |>
      complete(partido, sexo, fill = list(n = 0)) |>
      group_by(partido) |>
      mutate(total = sum(n),
             pct = if_else(total > 0, round(n / total * 100, 1), 0)) |>
      ungroup()
    validate(need(nrow(base) > 0, "Sin datos para mostrar."))

    plot_ly(base |> filter(sexo == "Femenino"),
      x = ~pct, y = ~partido, name = "Femenino",
      type = "bar", orientation = "h", marker = list(color = rojo),
      text = ~paste0(partido, "<br>Femenino: ", pct, "%"),
      hoverinfo = "text",
      texttemplate = ~paste0(pct, "%"), textposition = "auto",
      textfont = list(color = blanco, size = 12, family = "Cabin")
    ) |>
    add_trace(
      data = base |> filter(sexo == "Masculino"),
      x = ~pct, y = ~partido, name = "Masculino",
      marker = list(color = blanco),
      text = ~paste0(partido, "<br>Masculino: ", pct, "%"),
      hoverinfo = "text",
      texttemplate = ~paste0(pct, "%"), textposition = "auto",
      textfont = list(color = background_oscuro, size = 12, family = "Cabin")
    ) |> layout(
      barmode = "stack",
      title = titulo_plotly("Distribución por sexo por partido",
                            paste0("% candidatos · ordenado por % femenino · ", n_label(fc_uniq()))),
      xaxis = list(color = blanco, gridcolor = gris_claro, ticksuffix = "%",
                   range = c(0, 100), tickfont = list(family = "Cabin")),
      yaxis = list(color = blanco, tickfont = list(family = "Cabin", size = 12),
                   tickmode = "linear", tick0 = 0, dtick = 1, automargin = TRUE, title = "",
                   categoryorder = "array",
                   categoryarray = (base |> filter(sexo == "Femenino") |> arrange(pct))$partido),
      legend = list(orientation = "h", x = 0.5, xanchor = "center", y = 1.08,
                    font = list(color = blanco, family = "Cabin")),
      paper_bgcolor = background_oscuro, plot_bgcolor = background_oscuro,
      font = list(color = blanco, family = "Cabin"),
      margin = list(l = 5, r = 15, t = 110, b = 5)
    )
  })

  # ── 5. Años de experiencia laboral por partido ───────────────────────────────
  output$plot_exp_laboral <- renderPlotly({
    d <- fc_uniq() |> left_join(exp_anos_por_dni, by = "dni") |> filter(!is.na(total_anos_exp))
    validate(need(nrow(d) > 0, "Sin datos de experiencia laboral."))

    datos <- d |>
      summarise(promedio_exp = mean(total_anos_exp), n_cand = n_distinct(dni), .by = partido) |>
      arrange(promedio_exp) |>
      mutate(etiqueta = round(promedio_exp, 1))

    plot_ly(datos,
      x = ~promedio_exp, y = ~reorder(partido, promedio_exp),
      type = "bar", orientation = "h", marker = list(color = rojo),
      text = ~paste0(partido, "<br>Promedio: ", etiqueta, " años<br>Candidatos: ", n_cand),
      hoverinfo = "text",
      texttemplate = ~paste0(etiqueta, " años"), textposition = "outside", cliponaxis = FALSE,
      textfont = list(color = blanco, size = 12, family = "Cabin")
    ) |> layout(
      title = titulo_plotly("Promedio de años de experiencia laboral por partido",
                            paste0(n_label(d), " con experiencia declarada")),
      xaxis = list(color = blanco, gridcolor = gris_claro, ticksuffix = " años",
                   tickfont = list(family = "Cabin")),
      yaxis = list(color = blanco, tickfont = list(family = "Cabin", size = 12),
                   tickmode = "linear", tick0 = 0, dtick = 1, automargin = TRUE, title = ""),
      paper_bgcolor = background_oscuro, plot_bgcolor = background_oscuro,
      font = list(color = blanco, family = "Cabin"),
      margin = list(l = 5, r = 80, t = 90, b = 5)
    )
  })

  # ── 6. Tabla resumen por partido ─────────────────────────────────────────────
  output$tabla_partidos <- DT::renderDT({
    d <- fc_uniq() |> left_join(exp_anos_por_dni, by = "dni")
    d |>
      summarise(
        Candidatos            = n_distinct(dni),
        Hombres               = n_distinct(dni[sexo == "Masculino"]),
        Mujeres               = n_distinct(dni[sexo == "Femenino"]),
        `Sin ed. básica`      = sum(tiene_educacion_basica == 0 | is.na(tiene_educacion_basica)),
        `Sin estudios sup.`   = sum(
          (ed_universitaria == 0 | is.na(ed_universitaria)) &
          (ed_tecnica == 0 | is.na(ed_tecnica)) &
          (ed_no_univ == 0 | is.na(ed_no_univ)) &
          (ed_posgrado == 0 | is.na(ed_posgrado))
        ),
        `Prom. años exp.`     = round(mean(total_anos_exp, na.rm = TRUE), 1),
        `Con sentencias`      = sum(sentencias == 1, na.rm = TRUE),
        `Ingreso promedio`    = mean(total_ingresos_num, na.rm = TRUE),
        .by = partido
      ) |>
      arrange(desc(`Ingreso promedio`)) |>
      rename(Partido = partido) |>
      DT::datatable(rownames = FALSE,
                    options = modifyList(DT_OPCIONES, list(pageLength = -1, paging = FALSE))) |>
      DT::formatCurrency("Ingreso promedio", currency = "S/ ", interval = 3, mark = ",", digits = 0)
  })

  # ── 7. Ranking candidatos con más sentencias ──────────────────────────────────
  output$plot_ranking_candidatos_sent <- renderPlotly({
    dnis_fc <- unique(fc()$dni)
    datos <- sentencias_df |>
      filter(dni %in% dnis_fc) |>
      group_by(dni) |>
      summarise(
        n_sent  = n(),
        materias = paste(unique(sentencia_materia[!is.na(sentencia_materia)]), collapse = "<br>· "),
        fallos   = paste(sentencia_fallo[!is.na(sentencia_fallo)], collapse = "<br>· "),
        .groups = "drop"
      ) |>
      left_join(candidatos |> distinct(dni, .keep_all = TRUE) |>
                  select(dni, nombre, partido, cargo_al_que_postula), by = "dni") |>
      arrange(desc(n_sent)) |>
      slice_head(n = 20) |>
      mutate(
        label_eje = paste0(str_trunc(nombre, 25), " (", str_trunc(partido, 12), ")"),
        tooltip = paste0("<b>", nombre, "</b><br>", partido, "<br>", cargo_al_que_postula,
                         "<br><br><b>", n_sent, " sentencia(s)</b>",
                         "<br><b>Materias:</b><br>· ", materias,
                         "<br><b>Fallos:</b><br>· ", fallos)
      ) |>
      arrange(n_sent)
    validate(need(nrow(datos) > 0, "Sin candidatos con sentencias en la selección."))

    plot_ly(datos,
      x = ~n_sent, y = ~reorder(label_eje, n_sent),
      type = "bar", orientation = "h", marker = list(color = rojo),
      text = ~tooltip, hoverinfo = "text",
      texttemplate = ~as.character(n_sent), textposition = "outside", cliponaxis = FALSE,
      textfont = list(color = blanco, size = 12, family = "Cabin")
    ) |> layout(
      title = titulo_plotly("Candidatos/as con más sentencias",
                            "Top 20 · cursor encima para más info"),
      xaxis = list(color = blanco, gridcolor = gris_claro, dtick = 1,
                   tickfont = list(family = "Cabin")),
      yaxis = list(color = blanco, tickfont = list(family = "Cabin", size = 12),
                   tickmode = "linear", tick0 = 0, dtick = 1, automargin = TRUE, title = ""),
      paper_bgcolor = background_oscuro, plot_bgcolor = background_oscuro,
      font = list(color = blanco, family = "Cabin"),
      margin = list(l = 5, r = 15, t = 90, b = 5)
    )
  })

  # ── 8. Ranking candidatos por ingresos ────────────────────────────────────────
  output$plot_ranking_ingresos_cand <- renderPlotly({
    d <- fc_uniq() |>
      filter(!is.na(total_ingresos_num), total_ingresos_num > 0) |>
      arrange(desc(total_ingresos_num)) |>
      slice_head(n = 25)
    validate(need(nrow(d) > 0, "Sin datos de ingresos."))

    label_n <- n_label(d)
    datos <- d |>
      mutate(etiqueta = dollar(total_ingresos_num, prefix = "S/ ", big.mark = ",", accuracy = 1),
             label_eje = str_trunc(nombre, 30)) |>
      arrange(total_ingresos_num)

    plot_ly(datos,
      x = ~total_ingresos_num, y = ~reorder(label_eje, total_ingresos_num),
      type = "bar", orientation = "h", marker = list(color = blanco),
      text = ~paste0(nombre, "<br>", partido, "<br>", cargo_al_que_postula,
                     "<br>Ingresos: ", etiqueta),
      hoverinfo = "text",
      texttemplate = ~etiqueta, textposition = "outside", cliponaxis = FALSE,
      textfont = list(color = blanco, size = 12, family = "Cabin")
    ) |> layout(
      title = titulo_plotly("Candidatos con mayores ingresos declarados",
                            paste0("Top 25 · ", label_n)),
      xaxis = list(color = blanco, gridcolor = gris_claro, tickformat = ",.0f",
                   tickprefix = "S/ ", tickfont = list(family = "Cabin")),
      yaxis = list(color = blanco, tickfont = list(family = "Cabin", size = 12),
                   tickmode = "linear", tick0 = 0, dtick = 1, automargin = TRUE, title = ""),
      paper_bgcolor = background_oscuro, plot_bgcolor = background_oscuro,
      font = list(color = blanco, family = "Cabin"),
      margin = list(l = 5, r = 15, t = 90, b = 5)
    )
  })

  # ── 9. Ranking sentencias por materia ────────────────────────────────────────
  output$plot_ranking_sentencias <- renderPlotly({
    dnis_fc <- unique(fc()$dni)
    datos <- sentencias_df |>
      filter(dni %in% dnis_fc, !is.na(sentencia_materia)) |>
      count(sentencia_materia, name = "n") |>
      arrange(n) |>
      slice_tail(n = 10)
    validate(need(nrow(datos) > 0, "Sin sentencias en la selección actual."))

    plot_ly(datos,
      x = ~n, y = ~reorder(sentencia_materia, n),
      type = "bar", orientation = "h", marker = list(color = rojo),
      text = ~paste0(sentencia_materia, ": ", n, " sentencias"),
      hoverinfo = "text",
      texttemplate = ~as.character(n), textposition = "outside", cliponaxis = FALSE,
      textfont = list(color = blanco, size = 12, family = "Cabin")
    ) |> layout(
      title = titulo_plotly("Sentencias por materia", paste0("Top 10 · ", sum(datos$n), " en la selección")),
      xaxis = list(color = blanco, gridcolor = gris_claro, tickfont = list(family = "Cabin")),
      yaxis = list(color = blanco, tickfont = list(family = "Cabin", size = 12),
                   tickmode = "linear", tick0 = 0, dtick = 1, automargin = TRUE, title = ""),
      paper_bgcolor = background_oscuro, plot_bgcolor = background_oscuro,
      font = list(color = blanco, family = "Cabin"),
      margin = list(l = 5, r = 15, t = 90, b = 5)
    )
  })

  # ── 10. Treemap ingreso por sexo ──────────────────────────────────────────────
  output$plot_treemap <- renderPlotly({
    d <- fc_uniq() |> filter(!is.na(total_ingresos_num), !is.na(sexo))
    validate(need(nrow(d) > 0, "Sin datos para mostrar."))

    datos <- d |>
      summarise(promedio = mean(total_ingresos_num), n = n_distinct(dni), .by = sexo) |>
      mutate(label = paste0("<b>", sexo, "</b><br>",
                            dollar(promedio, prefix = "S/ ", big.mark = ",", accuracy = 1),
                            "<br>", comma(n), " candidatos"))

    plot_ly(data = datos,
      labels = ~sexo, parents = ~"", values = ~promedio,
      type = "treemap", text = ~label, hoverinfo = "text", textinfo = "text",
      marker = list(colors = c(rojo, blanco),
                    line = list(color = background_oscuro, width = 3)),
      pathbar = list(visible = FALSE)
    ) |> layout(
      title = list(text = paste0("<b>Ingreso promedio por sexo</b><br><sup>", n_label(d), "</sup>"),
                   font = list(color = blanco, family = "Cabin", size = 18),
                   x = 0.5, xanchor = "center"),
      paper_bgcolor = background_oscuro,
      font = list(color = blanco, family = "Cabin"),
      margin = list(l = 5, r = 5, t = 90, b = 5)
    )
  })

  # ── 11. Sin trayectoria · mayores ingresos ────────────────────────────────────
  output$plot_sin_trayectoria <- renderPlotly({
    datos <- fc_uniq() |>
      filter((exp_laboral == 0 | is.na(exp_laboral)),
             (cargos_partidarios == 0 | is.na(cargos_partidarios)),
             (eleccion_popular == 0 | is.na(eleccion_popular)),
             !is.na(total_ingresos_num), total_ingresos_num > 0) |>
      arrange(desc(total_ingresos_num)) |>
      slice_head(n = 10) |>
      mutate(etiqueta  = dollar(total_ingresos_num, prefix = "S/ ", big.mark = ",", accuracy = 1),
             label_eje = str_trunc(nombre, 30)) |>
      arrange(total_ingresos_num)
    validate(need(nrow(datos) > 0, "No hay candidatos sin trayectoria declarada en la selección."))

    plot_ly(datos,
      x = ~total_ingresos_num, y = ~reorder(label_eje, total_ingresos_num),
      type = "bar", orientation = "h", marker = list(color = rojo),
      text = ~paste0(nombre, "<br>", partido, "<br>", cargo_al_que_postula,
                     "<br>Ingresos: ", etiqueta),
      hoverinfo = "text",
      texttemplate = ~etiqueta, textposition = "outside", cliponaxis = FALSE,
      textfont = list(color = blanco, size = 12, family = "Cabin")
    ) |> layout(
      title = titulo_plotly(
        "Sin trayectoria declarada, pero con altos ingresos",
        "Top 10 sin exp. laboral, cargos partidarios ni elección popular ante el JNE"
      ),
      xaxis = list(color = blanco, gridcolor = gris_claro, tickformat = ",.0f",
                   tickprefix = "S/ ", tickfont = list(family = "Cabin")),
      yaxis = list(color = blanco, tickfont = list(family = "Cabin", size = 12),
                   tickmode = "linear", tick0 = 0, dtick = 1, automargin = TRUE, title = ""),
      paper_bgcolor = background_oscuro, plot_bgcolor = background_oscuro,
      font = list(color = blanco, family = "Cabin"),
      margin = list(l = 5, r = 15, t = 100, b = 5)
    )
  })

  # ── 12. Tabla de candidatos ───────────────────────────────────────────────────
  output$tabla_caption <- renderUI({
    activo <- partido_click()
    if (!is.null(activo))
      tags$span(
        tags$span(paste0("Filtrado por: ", activo),
                  style = "color: #febd17; font-style: italic;"),
        tags$span(" · Cambia el filtro 'Partido' en la barra lateral para ver todos",
                  style = "color: #aaaaaa; font-size: 0.85rem;")
      )
    else
      tags$span("Tabla de candidatos", style = "color: #ffffff;")
  })

  output$tabla_candidatos <- DT::renderDT({
    d <- fc_tabla() |>
      transmute(
        Nombre         = nombre,
        DNI            = dni,
        Cargo          = cargo_al_que_postula,
        Partido        = partido,
        Departamento   = departamento,
        Sexo           = sexo,
        `Ed. Básica`   = tiene_educacion_basica,
        `Ed. Técnica`  = ed_tecnica,
        `Ed. No Univ.` = ed_no_univ,
        `Ed. Univ.`    = ed_universitaria,
        `Ed. Posgrado` = ed_posgrado,
        `Exp. Laboral` = exp_laboral,
        `Cargos Part.` = cargos_partidarios,
        `Elec. Popular`= eleccion_popular,
        Sentencias     = sentencias,
        Bienes         = bienes,
        `Total Ingresos` = total_ingresos_num,
        Ficha = ifelse(!is.na(url) & url != "",
                       sprintf('<a href="%s" target="_blank" style="color:#febd17">Ver ficha</a>', url), "—")
      )
    DT::datatable(d, escape = FALSE, rownames = FALSE,
                  options = DT_OPCIONES) |>
      DT::formatCurrency("Total Ingresos", currency = "S/ ", interval = 3, mark = ",", digits = 0)
  })

  # Mantener outputs activos aunque no estén visibles
  outputOptions(output, "plot_ingresos_partido",       suspendWhenHidden = FALSE)
  outputOptions(output, "plot_sentencias_partido",     suspendWhenHidden = FALSE)
  outputOptions(output, "plot_ingreso_tipo",           suspendWhenHidden = FALSE)
  outputOptions(output, "plot_sexo_partido",           suspendWhenHidden = FALSE)
  outputOptions(output, "plot_exp_laboral",            suspendWhenHidden = FALSE)
  outputOptions(output, "plot_ranking_candidatos_sent",suspendWhenHidden = FALSE)
  outputOptions(output, "plot_ranking_ingresos_cand",  suspendWhenHidden = FALSE)
  outputOptions(output, "plot_ranking_sentencias",     suspendWhenHidden = FALSE)
  outputOptions(output, "plot_treemap",                suspendWhenHidden = FALSE)
  outputOptions(output, "plot_sin_trayectoria",        suspendWhenHidden = FALSE)
  outputOptions(output, "tabla_partidos",              suspendWhenHidden = FALSE)
  outputOptions(output, "tabla_candidatos",            suspendWhenHidden = FALSE)
}

shinyApp(ui, server)
