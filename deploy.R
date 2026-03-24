# ============================================================
# deploy.R — Sube la app a shinyapps.io
# Ejecuta este script UNA SOLA VEZ desde RStudio o la consola R
# ============================================================

# 1. Instalar rsconnect si no lo tienes
if (!requireNamespace("rsconnect", quietly = TRUE)) install.packages("rsconnect")

# 2. Conectar tu cuenta (solo la primera vez)
#    Ve a https://www.shinyapps.io → Account → Tokens → Show → Copy to clipboard
#    Luego pega aquí los valores:
rsconnect::setAccountInfo(
  name   = "TU_USUARIO",         # ← tu username de shinyapps.io
  token  = "TU_TOKEN",           # ← pégalo desde el dashboard
  secret = "TU_SECRET"           # ← pégalo desde el dashboard
)

# 3. Deploy (ejecuta desde el directorio raíz del proyecto)
rsconnect::deployApp(
  appDir    = ".",                # directorio donde está app.R
  appName   = "candi-datos-2026",
  appFiles  = c(
    "app.R",
    list.files("data",   full.names = TRUE, recursive = TRUE),
    list.files("assets", full.names = TRUE, recursive = TRUE)
  ),
  forceUpdate = TRUE
)
