# setwd('./candi-datos-2026')
# here::here()

# librerías ---------------------------------------------------------------

library(shiny)
library(shinydashboard)
library(dplyr)
library(ggplot2)
library(DT)
library(duckdb)
library(glue)
library(DBI)
library(kableExtra)

# Procesamiento -----------------------------------------------------------

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

print(
  paste(
    "Cantidad de candidatos que postulan como Diputados:", 
    nro_candidatos_diputados
    )
  )


### Senadores -------------------------------------------------------------
nro_candidatos_senadores <- dbGetQuery(
  con, 
  "
  SELECT COUNT (DISTINCT dni) FROM candidatos
  WHERE cargo_al_que_postula = 'Senadores'
  "
)

print(
  paste(
    "Cantidad de candidatos que postulan como Senadores:", 
    nro_candidatos_senadores
  )
)


### Parlamento Andino -----------------------------------------------------
nro_candidatos_andino <- dbGetQuery(
  con,
  "
  SELECT COUNT (DISTINCT dni) FROM candidatos
  WHERE cargo_al_que_postula = 'Parlamento Andino'
  "
)

print(
  paste(
    "Cantidad de candidatos que postulan al Parlamento Andino:",
    nro_candidatos_andino
  )
)


# candidatos con sentencias -----------------------------------------------
nro_candidatos_sentencias <- dbGetQuery(
  con,
  "
  SELECT COUNT (DISTINCT dni) FROM candidatos
  WHERE sentencias = 1
  "
)

print(
  paste("Total de sentencias:", nro_candidatos_sentencias$total_sentencias)
)


# Promedio de ingresos ----------------------------------------------------
Los 

S/ 24,000.00
S/ 43,206.00
S/ 0.00



#  ------------------------------------------------------------------------
# View(dbGetQuery(con, "SELECT * FROM candidatos LIMIT 5"))
# View(dbGetQuery(con, "SELECT * FROM sentencias LIMIT 5"))


# Cargar datos desde DuckDB -----------------------------------------------

candidatos <- dbGetQuery(con, "SELECT * FROM candidatos")
partidos_list <- sort(unique(candidatos$partido))

# UI ----------------------------------------------------------------------

# UI del Dashboard
ui <- dashboardPage(
  skin = "blue",
  
  # Header
  dashboardHeader(
    title = div(
      img(src = "https://www.jne.gob.pe/assets/img/LOGOTIPO_JNE.png", 
          height = "45px", style = "margin-right: 10px;"),
      "CandiDatos 2026 - Análisis de Candidaturas"
    ),
    titleWidth = 500
  ),
  
  # Sidebar
  dashboardSidebar(
    width = 250,
    sidebarMenu(
      menuItem(
        "Dashboard Principal",
        tabName = "dashboard",
        icon = icon("chart-bar")
      ),
      menuItem(
        "Datos Detallados",
        tabName = "detalles",
        icon = icon("table")
      )
    ),
    h4("Filtros", style = "margin-top: 30px; margin-left: 15px; font-weight: bold;"),
    selectizeInput(
      inputId = "partido_filter",
      label = "Partido Político",
      choices = c("Todos" = "*", partidos_list),
      selected = "*",
      multiple = FALSE
    ),
    
    # Estadísticas en el sidebar
    div(
      style = "background-color: #ecf0f1; padding: 15px; margin-top: 30px; border-radius: 5px;",
      h4("Resumen Rápido", style = "margin-top: 0;"),
      htmlOutput("sidebar_stats")
    )
  ),
  
  # Body
  dashboardBody(
    tabItems(
      # TAB 1: Dashboard Principal
      tabItem(
        tabName = "dashboard",
        fluidRow(
          # Tarjeta: Total de Candidatos
          column(
            width = 4,
            valueBoxOutput("total_candidatos")
          ),
          # Tarjeta: Mujeres
          column(
            width = 4,
            valueBoxOutput("candidatas_mujeres")
          ),
          # Tarjeta: Hombres
          column(
            width = 4,
            valueBoxOutput("candidatos_hombres")
          )
        ),
        
        fluidRow(
          # Gráfico: Distribución por Género
          column(
            width = 6,
            box(
              title = "Distribución por Género",
              status = "primary",
              solidHeader = TRUE,
              width = NULL,
              collapsible = TRUE,
              plotOutput("plot_genero", height = "350px")
            )
          ),
          # Tabla: Top 10 Partidos
          column(
            width = 6,
            box(
              title = "Top 10 Partidos por Candidatos",
              status = "info",
              solidHeader = TRUE,
              width = NULL,
              collapsible = TRUE,
              DT::dataTableOutput("tabla_partidos")
            )
          )
        ),
        
        fluidRow(
          # Gráfico: Candidatos por Género y Partido
          column(
            width = 12,
            box(
              title = "Candidatos por Género según Tipo de Cargo",
              status = "success",
              solidHeader = TRUE,
              width = NULL,
              collapsible = TRUE,
              plotOutput("plot_cargo_genero", height = "400px")
            )
          )
        )
      ),
      
      # TAB 2: Datos Detallados
      tabItem(
        tabName = "detalles",
        fluidRow(
          column(
            width = 12,
            box(
              title = "Candidatos Registrados (DNI Único)",
              status = "primary",
              solidHeader = TRUE,
              width = NULL,
              collapsible = FALSE,
              DT::dataTableOutput("tabla_candidatos")
            )
          )
        )
      )
    )
  )
)

# Server
server <- function(input, output, session) {
  
  # Datos reactivos filtrados
  datos_filtrados <- reactive({
    if (input$partido_filter == "*") {
      candidatos
    } else {
      candidatos %>% filter(partido == input$partido_filter)
    }
  })
  
  # Datos únicos por DNI
  datos_unicos <- reactive({
    datos_filtrados() %>%
      distinct(DNI, .keep_all = TRUE)
  })
  
  # Métricas principales
  total_candidatos <- reactive({
    nrow(datos_unicos())
  })
  
  mujeres <- reactive({
    datos_unicos() %>%
      filter(Sexo == "Femenino") %>%
      nrow()
  })
  
  hombres <- reactive({
    datos_unicos() %>%
      filter(Sexo == "Masculino") %>%
      nrow()
  })
  
  porcentaje_mujeres <- reactive({
    if (total_candidatos() == 0) {
      0
    } else {
      round((mujeres() / total_candidatos()) * 100, 1)
    }
  })
  
  # Value Boxes
  output$total_candidatos <- renderValueBox({
    valueBox(
      value = format(total_candidatos(), big.mark = ","),
      subtitle = "Candidatos Totales (DNI Único)",
      icon = icon("users"),
      color = "red"
    )
  })
  
  output$candidatas_mujeres <- renderValueBox({
    valueBox(
      value = format(mujeres(), big.mark = ","),
      subtitle = paste0("Mujeres (", porcentaje_mujeres(), "%)"),
      icon = icon("venus"),
      color = "aqua"
    )
  })
  
  output$candidatos_hombres <- renderValueBox({
    valueBox(
      value = format(hombres(), big.mark = ","),
      subtitle = paste0("Hombres (", 100 - porcentaje_mujeres(), "%)"),
      icon = icon("mars"),
      color = "maroon"
    )
  })
  
  # Estadísticas en sidebar
  output$sidebar_stats <- renderUI({
    HTML(paste0(
      "<p><strong>Filtro Actual:</strong><br>",
      if (input$partido_filter == "*") "Todos los partidos" else input$partido_filter,
      "</p>",
      "<hr style='margin: 10px 0;'>",
      "<p><strong>Total:</strong> ", format(total_candidatos(), big.mark = ","), "</p>",
      "<p><strong>Mujeres:</strong> ", format(mujeres(), big.mark = ","), " (", porcentaje_mujeres(), "%)</p>",
      "<p><strong>Hombres:</strong> ", format(hombres(), big.mark = ","), " (", 100 - porcentaje_mujeres(), "%)</p>"
    ))
  })
  
  # Gráfico: Distribución por Género (Pie Chart)
  output$plot_genero <- renderPlot({
    datos <- data.frame(
      Genero = c("Mujeres", "Hombres"),
      Cantidad = c(mujeres(), hombres()),
      Porcentaje = c(porcentaje_mujeres(), 100 - porcentaje_mujeres())
    )
    
    ggplot(datos, aes(x = "", y = Cantidad, fill = Genero)) +
      geom_bar(stat = "identity", width = 1) +
      coord_polar("y", start = 0) +
      geom_text(aes(label = paste0(Cantidad, "\n(", Porcentaje, "%)")),
                position = position_stack(vjust = 0.5),
                fontface = "bold",
                size = 5) +
      scale_fill_manual(values = c("Mujeres" = "#FF69B4", "Hombres" = "#4169E1")) +
      theme_void() +
      theme(legend.position = "bottom", text = element_text(size = 12))
  })
  
  # Tabla: Top 10 Partidos
  output$tabla_partidos <- renderDT({
    datos_filtrados() %>%
      distinct(dni, .keep_all = TRUE) %>%
      group_by(partido) %>%
      summarise(
        "Candidatos" = n(),
        "Mujeres" = sum(Sexo == "Femenino"),
        "Hombres" = sum(Sexo == "Masculino"),
        .groups = "drop"
      ) %>%
      arrange(desc(Candidatos)) %>%
      head(10) %>%
      datatable(
        options = list(
          pageLength = 10,
          language = list(url = "//cdn.datatables.net/plug-ins/1.10.11/i18n/Spanish.json"),
          dom = "t"
        ),
        rownames = FALSE
      )
  })
  
  # Gráfico: Candidatos por Género y Cargo
  output$plot_cargo_genero <- renderPlot({
    datos_filtrados() %>%
      distinct(dni, .keep_all = TRUE) %>%
      group_by(`Cargo al que postula`, Sexo) %>%
      summarise(Cantidad = n(), .groups = "drop") %>%
      ggplot(aes(x = reorder(`Cargo al que postula`, Cantidad, FUN = sum), 
                 y = Cantidad, 
                 fill = Sexo)) +
      geom_bar(stat = "identity", position = "dodge") +
      coord_flip() +
      scale_fill_manual(values = c("Femenino" = "#FF69B4", "Masculino" = "#4169E1")) +
      labs(
        title = "",
        x = "Cargo",
        y = "Número de Candidatos",
        fill = "Género"
      ) +
      theme_minimal() +
      theme(
        text = element_text(size = 11),
        plot.title = element_text(face = "bold", size = 13),
        axis.title = element_text(face = "bold")
      )
  })
  
  # Tabla: Candidatos Detallados
  output$tabla_candidatos <- renderDT({
    datos_unicos() %>%
      select(
        dni,
        nombre,
        Sexo,
        `Cargo al que postula`,
        partido,
        departamento
      ) %>%
      arrange(nombre) %>%
      datatable(
        options = list(
          pageLength = 25,
          language = list(url = "//cdn.datatables.net/plug-ins/1.10.11/i18n/Spanish.json"),
          scrollX = TRUE,
          columnDefs = list(
            list(width = "80px", targets = 0),
            list(width = "200px", targets = 1)
          )
        ),
        colnames = c("DNI", "Nombre", "Sexo", "Cargo", "Partido", "Departamento"),
        rownames = FALSE
      )
  })
}

shinyApp(ui, server)
