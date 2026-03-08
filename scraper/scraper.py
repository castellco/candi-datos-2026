import asyncio
import random
import pandas as pd
from playwright.async_api import async_playwright
import os
import csv
import json
from datetime import datetime

# =============================================================
#  CONFIGURACIÓN
# =============================================================

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

BASE = "body > app-root > div > main > app-hoja-vida > div > div.max-w-5xl.mx-auto.px-4.py-6 > div.space-y-4"

SEL_SIN_CANDIDATOS = (
    "body > app-root > div > main > app-diputados > div > "
    "app-candidatos-presidenciales > section > div > div.text-center.py-10 > p"
)

FUENTES = [
    {
        "url":        "https://votoinformado.jne.gob.pe/diputados",
        "cargo":      "Diputados",
        "tipo":       "con_departamento",
        "prefijo_id": "DIP",
    },
    {
        "url":        "https://votoinformado.jne.gob.pe/senadores",
        "cargo":      "Senadores",
        "tipo":       "sin_departamento",
        "prefijo_id": "SEN",
    },
    {
        "url":        "https://votoinformado.jne.gob.pe/parlamento-andino",
        "cargo":      "Parlamento Andino",
        "tipo":       "sin_departamento",
        "prefijo_id": "PAR",
    },
    {
        "url":        "https://votoinformado.jne.gob.pe/presidente-vicepresidentes",
        "cargo":      None,
        "tipo":       "presidencial",
        "prefijo_id": "PRES",
    },
]

CARGOS_PRESIDENCIALES = [
    "Presidente",
    "Primer Vicepresidente",
    "Segundo Vicepresidente",
]


# =============================================================
#  CONTROL DE VELOCIDAD
#  Pausas entre requests para no gatillar el rate limit del JNE.
#  goto_con_reintento: si hay ERR_CONNECTION_RESET espera y reintenta.
# =============================================================

PAUSA_MIN      = 2.0   # segundos mínimos entre candidatos
PAUSA_MAX      = 5.0   # segundos máximos (valor aleatorio)
PAUSA_RESET    = 90    # segundos de espera tras ERR_CONNECTION_RESET
PAUSA_PARTIDO  = 3.0   # pausa adicional entre partidos
MAX_REINTENTOS = 3     # reintentos antes de registrar error


async def goto_con_reintento(page, url, reintentos=MAX_REINTENTOS):
    """
    Navega a una URL con reintentos automáticos.
    Si hay ERR_CONNECTION_RESET espera PAUSA_RESET segundos y reintenta.
    """
    for intento in range(1, reintentos + 1):
        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
            return True
        except Exception as e:
            msg = str(e)
            es_reset = any(x in msg for x in [
                "ERR_CONNECTION_RESET", "ERR_CONNECTION_REFUSED",
                "ERR_EMPTY_RESPONSE", "net::"
            ])
            if es_reset and intento < reintentos:
                print(f"\n    ⚠ Conexión cortada. Esperando {PAUSA_RESET}s "
                      f"(intento {intento}/{reintentos - 1})...", flush=True)
                await asyncio.sleep(PAUSA_RESET)
            else:
                raise
    return False

# =============================================================
#  CHECKPOINT
#  Guarda qué partidos ya se procesaron para poder retomar
#  una ejecución interrumpida sin repetir trabajo.
#  Formato: conjunto de strings "prefijo|dep_nombre|p_idx"
# =============================================================

CHECKPOINT_FILE = f"{OUTPUT_DIR}/checkpoint.json"

def checkpoint_cargar():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def checkpoint_guardar(completados: set):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(list(completados), f, ensure_ascii=False)

def checkpoint_key(prefijo, dep_nombre, p_idx):
    return f"{prefijo}|{dep_nombre or ''}|{p_idx}"

# =============================================================
#  LOG DE ERRORES (escritura inmediata)
# =============================================================

_timestamp_global = datetime.now().strftime("%Y%m%d_%H%M%S")
_ruta_errores     = f"{OUTPUT_DIR}/errores_{_timestamp_global}.csv"
_campos_error     = [
    "DNI", "nombre", "partido", "departamento", "cargo",
    "id_candidato", "fuente", "p_idx", "c_idx",
    "motivo", "detalle", "timestamp"
]

def _inicializar_log_errores():
    with open(_ruta_errores, "w", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=_campos_error).writeheader()

def registrar_error(id_candidato, fuente, departamento, partido,
                    p_idx, c_idx, motivo, detalle="",
                    nombre=None, dni=None, cargo=None):
    fila = {
        "DNI":          dni or "",
        "nombre":       nombre or "",
        "partido":      partido or "",
        "departamento": departamento or "",
        "cargo":        cargo or "",
        "id_candidato": id_candidato,
        "fuente":       fuente,
        "p_idx":        p_idx,
        "c_idx":        c_idx,
        "motivo":       motivo,
        "detalle":      str(detalle)[:300],
        "timestamp":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(_ruta_errores, "a", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=_campos_error).writerow(fila)
    print(f"    ✗ [{motivo}] registrado en log")

# =============================================================
#  GUARDADO INCREMENTAL
#  Acumula filas en memoria y vuelca a CSV cada N candidatos.
#  Si el script se interrumpe, no se pierde todo.
# =============================================================

GUARDAR_CADA = 50   # vuelca al CSV cada 50 candidatos

_buffers      = {}   # {nombre_tabla: [filas pendientes]}
_rutas_csv    = {}   # {nombre_tabla: ruta del archivo}

def _inicializar_buffers(timestamp):
    tablas = [
        "candidatos", "estudios_tecnicos", "estudios_no_universitarios",
        "estudios_universitarios", "estudios_posgrado", "experiencia_laboral",
        "cargos_partidarios", "eleccion_popular", "sentencias",
        "bienes_muebles_inmuebles", "informacion_adicional",
    ]
    for t in tablas:
        _buffers[t]   = []
        _rutas_csv[t] = f"{OUTPUT_DIR}/{t}_{timestamp}.csv"

def _volcar_buffer(nombre):
    """Escribe las filas pendientes al CSV y vacía el buffer."""
    if not _buffers[nombre]:
        return
    ruta   = _rutas_csv[nombre]
    existe = os.path.exists(ruta)
    df     = pd.DataFrame(_buffers[nombre])
    df.to_csv(ruta, mode="a", header=not existe, index=False, encoding="utf-8-sig")
    _buffers[nombre] = []

def agregar_candidato(datos, tablas):
    """Añade al buffer y vuelca si se alcanzó el límite."""
    _buffers["candidatos"].append(datos)
    for nombre, filas in tablas.items():
        _buffers[nombre].extend(filas)

    if len(_buffers["candidatos"]) >= GUARDAR_CADA:
        for nombre in _buffers:
            _volcar_buffer(nombre)
        print(f"    💾 Guardado incremental ({GUARDAR_CADA} candidatos)")

def volcar_todo():
    """Vuelca todos los buffers al terminar."""
    for nombre in _buffers:
        _volcar_buffer(nombre)


# =============================================================
#  HELPERS DE NAVEGACIÓN
# =============================================================

async def obtener_departamentos(page):
    try:
        await page.wait_for_selector("#departamento", timeout=8000)
        opciones = await page.query_selector_all("#departamento option")
        resultado = []
        for op in opciones:
            texto = (await op.inner_text()).strip()
            valor = await op.get_attribute("value")
            if valor and texto and texto.lower() not in ("seleccione", "todos", ""):
                resultado.append((valor, texto))
        return resultado
    except Exception:
        return []


async def seleccionar_departamento_por_valor(page, valor):
    try:
        await page.wait_for_selector("#departamento", timeout=5000)
        await page.select_option("#departamento", valor)
        await page.wait_for_load_state("networkidle")
        return True
    except Exception:
        return False


async def obtener_n_partidos(page):
    try:
        await page.wait_for_selector(
            "app-candidatos-presidenciales section div.grid div h3", timeout=8000
        )
        elems = await page.query_selector_all(
            "app-candidatos-presidenciales section div.grid div h3"
        )
        return len(elems)
    except Exception:
        return 0


async def seleccionar_partido(page, partido_index):
    try:
        await page.wait_for_selector(
            "app-candidatos-presidenciales section div.grid div h3", timeout=8000
        )
    except Exception:
        return None
    elems = await page.query_selector_all(
        "app-candidatos-presidenciales section div.grid div h3"
    )
    if not elems or partido_index > len(elems):
        return None
    elem           = elems[partido_index - 1]
    nombre_partido = (await elem.inner_text()).strip()
    await elem.click()
    await page.wait_for_load_state("networkidle")
    return nombre_partido


async def partido_tiene_candidatos(page):
    SEL_LISTA = "#idcontainer-lista-candidatos"
    SEL_VACIO = SEL_SIN_CANDIDATOS
    try:
        await page.wait_for_selector(f"{SEL_LISTA}, {SEL_VACIO}", timeout=8000)
    except Exception:
        return False, 0
    vacio = await page.query_selector(SEL_VACIO)
    if vacio and (await vacio.inner_text()).strip():
        return False, 0
    try:
        tarjetas = await page.query_selector_all(f"{SEL_LISTA} > div")
        return len(tarjetas) > 0, len(tarjetas)
    except Exception:
        return False, 0


async def ir_a_ficha_candidato(page, candidato_index):
    selector = f"#idcontainer-lista-candidatos > div:nth-child({candidato_index})"
    try:
        await page.wait_for_selector(selector, timeout=5000)
        tarjeta = await page.query_selector(selector)
        if not tarjeta:
            return False
        await tarjeta.click()
        await page.wait_for_load_state("networkidle")
        return True
    except Exception as e:
        print(f"    Error entrando a candidato {candidato_index}: {e}")
        return False


async def obtener_n_formulas(page):
    try:
        await page.wait_for_selector("#idcontainer-lista-candidatos", timeout=8000)
        items = await page.query_selector_all("#idcontainer-lista-candidatos > div")
        return len(items)
    except Exception:
        return 0


async def ir_a_formula(page, formula_index):
    """
    Lee el nombre del partido desde la tarjeta de la lista
    (ANTES de hacer click, donde el selector es conocido y fiable),
    luego abre la fórmula y devuelve el nombre.
    """
    selector_tarjeta  = f"#idcontainer-lista-candidatos > div:nth-child({formula_index})"
    selector_nombre   = (
        f"#idcontainer-lista-candidatos > div:nth-child({formula_index}) "
        f"> div.content-foto-candi > div.content-foto-organizacion "
        f"> div.content-txt-organizacion"
    )
    try:
        await page.wait_for_selector(selector_tarjeta, timeout=5000)

        # Leer nombre del partido desde la tarjeta (antes del click)
        nombre_partido = None
        elem_nombre = await page.query_selector(selector_nombre)
        if elem_nombre:
            nombre_partido = (await elem_nombre.inner_text()).strip() or None

        # Hacer click para abrir la fórmula
        tarjeta = await page.query_selector(selector_tarjeta)
        if not tarjeta:
            return nombre_partido  # devolver lo que tengamos aunque no abramos
        await tarjeta.click()
        await page.wait_for_load_state("networkidle")

        return nombre_partido or f"Formula_{formula_index}"

    except Exception as e:
        print(f"    Error abriendo fórmula {formula_index}: {e}")
        return None


async def ir_a_candidato_presidencial(page, candidato_index):
    sel = (
        "#container-formula-presidental "
        "> div.container-body-formula-presid "
        "> div.flex.flex-wrap.justify-center.gap-8.mb-12"
        f" > div:nth-child({candidato_index})"
    )
    try:
        await page.wait_for_selector(sel, timeout=5000)
        elem = await page.query_selector(sel)
        if not elem:
            return False
        await elem.click()
        await page.wait_for_load_state("networkidle")
        return True
    except Exception as e:
        print(f"    Error abriendo candidato presidencial {candidato_index}: {e}")
        return False


# =============================================================
#  HELPERS GENERALES
# =============================================================

def si_no_a_binario(texto):
    if texto is None:
        return None
    t = texto.strip().upper()
    if t in ("SÍ", "SI", "S"):
        return 1
    if t == "NO":
        return 0
    return None


async def extraer_valor_bloque(page, selector_bloque):
    try:
        await page.wait_for_selector(selector_bloque, timeout=5000)
        hijos = await page.query_selector_all(f"{selector_bloque} > *")
        if len(hijos) >= 2:
            return (await hijos[1].inner_text()).strip()
        texto = (await page.inner_text(selector_bloque)).strip()
        return texto.split()[-1] if texto else None
    except Exception:
        return None


async def extraer_seccion_tabla(page, id_candidato, sel_tbody, columnas, transformaciones=None):
    if transformaciones is None:
        transformaciones = [None] * len(columnas)
    tbody = await page.query_selector(sel_tbody)
    if tbody is None:
        return 0, []
    filas_html = await tbody.query_selector_all("tr")
    if not filas_html:
        return 0, []
    filas = []
    for fila in filas_html:
        celdas = await fila.query_selector_all("td")
        if len(celdas) < len(columnas):
            continue
        valores = [(await celdas[j].inner_text()).strip() for j in range(len(columnas))]
        if not any(valores):
            continue
        fila_dict = {"id_candidato": id_candidato}
        for col, val, transf in zip(columnas, valores, transformaciones):
            fila_dict[col] = transf(val) if transf is not None else (val if val else None)
        filas.append(fila_dict)
    return (1, filas) if filas else (0, [])


async def extraer_texto_selector(page, selector):
    try:
        await page.wait_for_selector(selector, timeout=3000)
        texto = (await page.inner_text(selector)).strip()
        return texto if texto else None
    except Exception:
        return None


# =============================================================
#  EXTRACCIÓN DE DATOS BÁSICOS
# =============================================================

async def extraer_datos_basicos(page, cargo_override=None):
    datos = {}

    selectores_nombre = [
        "#content-postulacion-comparacion "
        "> div.flex-grow.text-center.md\\:text-left > h2",
        "#div-content-hoja > div.content-cargo-candidato > h3",
        "#content-postulacion-comparacion > div.flex-grow.text-center.md\\:text-left "
        "> div.text-gray-500.font-semibold.mb-2.uppercase.flex.flex-col > span:nth-child(2)",
    ]
    nombre = None
    for sel in selectores_nombre:
        nombre = await extraer_texto_selector(page, sel)
        if nombre:
            break
    datos["nombre"] = nombre

    if cargo_override:
        datos["Cargo al que postula"] = cargo_override
    else:
        SEL_SPANS = (
            "#content-postulacion-comparacion > div.flex-grow.text-center.md\\:text-left "
            "> div.text-gray-500.font-semibold.mb-2.uppercase.flex.flex-col > span"
        )
        cargos = []
        try:
            spans = await page.query_selector_all(SEL_SPANS)
            for span in spans:
                texto = (await span.inner_text()).strip()
                if texto and texto != nombre:
                    cargos.append(texto)
        except Exception:
            pass
        if cargos:
            datos["Cargo al que postula"] = " / ".join(cargos)
        else:
            cargo_pres = await extraer_texto_selector(
                page, "#div-content-hoja > div.content-cargo-candidato > p"
            )
            datos["Cargo al que postula"] = cargo_pres

    sel_info = (
        "#content-postulacion-comparacion "
        "> div.flex-grow.text-center.md\\:text-left "
        "> div.grid.grid-cols-1.md\\:grid-cols-2.gap-x-8.gap-y-2.text-sm.text-gray-600"
    )
    try:
        await page.wait_for_selector(sel_info, timeout=5000)
        texto  = (await page.inner_text(sel_info)).strip()
        lineas = [l.strip() for l in texto.splitlines() if l.strip()]
        for i in range(0, len(lineas) - 1, 2):
            etiqueta = lineas[i]
            valor    = lineas[i + 1]
            if "dni" in etiqueta.lower():
                datos["DNI"] = str(valor).zfill(8)
            elif "sexo" in etiqueta.lower():
                datos["Sexo"] = valor
            elif "nacimiento" in etiqueta.lower():
                datos["Lugar de Nacimiento"] = valor
            elif "domicilio" in etiqueta.lower():
                datos["Lugar de Domicilio"] = valor
            else:
                datos[etiqueta] = valor
    except Exception:
        pass
    return datos


# =============================================================
#  SECCIONES DE HOJA DE VIDA
# =============================================================

async def extraer_educacion_basica(page):
    SEL_EDUC     = f"{BASE} > div:nth-child(1) > div > div > div:nth-child(1)"
    SEL_PRIMARIA = f"{BASE} > div:nth-child(1) > div > div > div:nth-child(2)"
    SEL_SECUND   = f"{BASE} > div:nth-child(1) > div > div > div:nth-child(3)"
    return {
        "Tiene Educacion Basica": si_no_a_binario(await extraer_valor_bloque(page, SEL_EDUC)),
        "Tiene Primaria":         si_no_a_binario(await extraer_valor_bloque(page, SEL_PRIMARIA)),
        "Tiene Secundaria":       si_no_a_binario(await extraer_valor_bloque(page, SEL_SECUND)),
    }

async def extraer_estudios_tecnicos(page, id_candidato):
    return await extraer_seccion_tabla(page, id_candidato,
        f"{BASE} > div:nth-child(2) > div > table > tbody",
        ["Ed Técnica_Centro", "Ed Técnica_Carrera", "Ed Técnica_Concluido"],
        [None, None, si_no_a_binario])

async def extraer_estudios_no_universitarios(page, id_candidato):
    return await extraer_seccion_tabla(page, id_candidato,
        f"{BASE} > div:nth-child(3) > div > table > tbody",
        ["Ed No Univ_Centro", "Ed No Univ_Carrera", "Ed No Univ_Concluido"],
        [None, None, si_no_a_binario])

async def extraer_estudios_universitarios(page, id_candidato):
    return await extraer_seccion_tabla(page, id_candidato,
        f"{BASE} > div:nth-child(4) > div > table > tbody",
        ["Ed Univ_Universidad", "Ed Univ_Concluido", "Ed Univ_Grado", "Ed Univ_Año"],
        [None, si_no_a_binario, None, None])

async def extraer_estudios_posgrado(page, id_candidato):
    return await extraer_seccion_tabla(page, id_candidato,
        f"{BASE} > div:nth-child(5) > div > table > tbody",
        ["Ed Posgrado_Centro", "Ed Posgrado_Especialidad",
         "Ed Posgrado_Concluido", "Ed Posgrado_Grado", "Ed Posgrado_Año"],
        [None, None, si_no_a_binario, None, None])

async def extraer_experiencia_laboral(page, id_candidato):
    return await extraer_seccion_tabla(page, id_candidato,
        f"{BASE} > div:nth-child(6) > div > table > tbody",
        ["Exp Laboral_Centro", "Exp Laboral_Ocupación", "Exp Laboral_Periodo"],
        [None, None, None])

async def extraer_cargos_partidarios(page, id_candidato):
    return await extraer_seccion_tabla(page, id_candidato,
        f"{BASE} > div:nth-child(7) > div > div:nth-child(1) > div.p-0 > table > tbody",
        ["Cargo Partidario_Organización", "Cargo Partidario_Cargo", "Cargo Partidario_Periodo"],
        [None, None, None])

async def extraer_eleccion_popular(page, id_candidato):
    return await extraer_seccion_tabla(page, id_candidato,
        f"{BASE} > div:nth-child(7) > div > div:nth-child(2) > div.p-0 > table > tbody",
        ["Elección Popular_Organización", "Elección Popular_Cargo", "Elección Popular_Periodo"],
        [None, None, None])

async def extraer_sentencias(page, id_candidato):
    tbody = await page.query_selector(f"{BASE} > div:nth-child(8) > div > table > tbody")
    if tbody is None:
        return 0, []
    filas = []
    for fila in await tbody.query_selector_all("tr"):
        m  = await fila.query_selector("td.px-4.py-2.text-sm.text-gray-900")
        f  = await fila.query_selector("td.px-4.py-2.text-sm.text-gray-500")
        vm = (await m.inner_text()).strip() if m else None
        vf = (await f.inner_text()).strip() if f else None
        if vm or vf:
            filas.append({"id_candidato": id_candidato,
                          "Sentencia_Materia": vm, "Sentencia_Fallo": vf})
    return (1, filas) if filas else (0, [])

async def extraer_bienes_rentas(page):
    return {
        "Rem. Público":   await extraer_texto_selector(page, f"{BASE} > div:nth-child(9) > div > div > div:nth-child(1) > span.text-right.font-bold"),
        "Rem. Privado":   await extraer_texto_selector(page, f"{BASE} > div:nth-child(9) > div > div > div:nth-child(2) > span.text-right.font-bold"),
        "Total Ingresos": await extraer_texto_selector(page, f"{BASE} > div:nth-child(9) > div > div > div:nth-child(3) > span.text-right.font-bold"),
    }

async def extraer_bienes_muebles_inmuebles(page, id_candidato):
    tbody = await page.query_selector(f"{BASE} > div:nth-child(10) > div > table > tbody")
    if tbody is None:
        return 0, []
    filas = []
    for fila in await tbody.query_selector_all("tr"):
        celdas = await fila.query_selector_all("td")
        tipo   = (await celdas[0].inner_text()).strip() if celdas else None
        de     = await fila.query_selector("td.px-4.py-2.text-sm.text-gray-500")
        ve     = await fila.query_selector("td.px-4.py-2.text-sm.text-gray-900.text-right")
        desc   = (await de.inner_text()).strip() if de else None
        valor  = (await ve.inner_text()).strip() if ve else None
        if tipo or desc or valor:
            filas.append({"id_candidato": id_candidato,
                          "Bien_Tipo": tipo, "Bien_Descripción": desc, "Bien_Valor": valor})
    return (1, filas) if filas else (0, [])

async def extraer_info_adicional(page, id_candidato):
    contenedor = await page.query_selector(f"{BASE} > div:nth-child(11) > div > div")
    if contenedor is None:
        return 0, []
    filas = []
    for sub in await contenedor.query_selector_all(":scope > div"):
        h      = await sub.query_selector("h4")
        p      = await sub.query_selector("p")
        titulo = (await h.inner_text()).strip() if h else None
        texto  = (await p.inner_text()).strip() if p else None
        if titulo or texto:
            filas.append({"id_candidato": id_candidato,
                          "Info Adicional_Título": titulo,
                          "Info Adicional_Texto":  texto})
    return (1, filas) if filas else (0, [])


# =============================================================
#  EXTRACCIÓN COMPLETA DE UN CANDIDATO (página ya abierta)
# =============================================================

async def scrapear_candidato(page, id_candidato, nombre_partido,
                              departamento=None, cargo_override=None):
    datos = await extraer_datos_basicos(page, cargo_override=cargo_override)
    datos["id_candidato"] = id_candidato
    datos["partido"]      = nombre_partido
    datos["departamento"] = departamento

    datos.update(await extraer_educacion_basica(page))

    # DNI para usar como clave en tablas relacionales
    dni = datos.get("DNI", "")

    ed_tec,      filas_tec      = await extraer_estudios_tecnicos(page, id_candidato)
    ed_no_univ,  filas_no_univ  = await extraer_estudios_no_universitarios(page, id_candidato)
    ed_univ,     filas_univ     = await extraer_estudios_universitarios(page, id_candidato)
    ed_posgrado, filas_posgrado = await extraer_estudios_posgrado(page, id_candidato)
    exp_lab,     filas_lab      = await extraer_experiencia_laboral(page, id_candidato)
    carg_part,   filas_cpart    = await extraer_cargos_partidarios(page, id_candidato)
    elec_pop,    filas_epop     = await extraer_eleccion_popular(page, id_candidato)
    sentencias,  filas_sent     = await extraer_sentencias(page, id_candidato)
    bienes,      filas_bienes   = await extraer_bienes_muebles_inmuebles(page, id_candidato)
    info_ad,     filas_info_ad  = await extraer_info_adicional(page, id_candidato)

    # Insertar DNI como primera columna en todas las tablas relacionales
    todas_las_filas = (
        filas_tec + filas_no_univ + filas_univ + filas_posgrado +
        filas_lab + filas_cpart + filas_epop + filas_sent +
        filas_bienes + filas_info_ad
    )
    for fila in todas_las_filas:
        # Reordenar: DNI primero, luego id_candidato, luego el resto
        contenido = {k: v for k, v in fila.items() if k not in ("DNI", "id_candidato")}
        fila.clear()
        fila["DNI"]          = dni
        fila["id_candidato"] = id_candidato
        fila.update(contenido)

    datos["Ed Técnica"]            = ed_tec
    datos["Ed No Univ"]            = ed_no_univ
    datos["Ed Universitaria"]      = ed_univ
    datos["Ed Posgrado"]           = ed_posgrado
    datos["Exp Laboral"]           = exp_lab
    datos["Cargos Partidarios"]    = carg_part
    datos["Elección Popular"]      = elec_pop
    datos["Sentencias"]            = sentencias
    datos["Bienes"]                = bienes
    datos["Información Adicional"] = info_ad
    datos.update(await extraer_bienes_rentas(page))

    tablas = {
        "estudios_tecnicos":          filas_tec,
        "estudios_no_universitarios": filas_no_univ,
        "estudios_universitarios":    filas_univ,
        "estudios_posgrado":          filas_posgrado,
        "experiencia_laboral":        filas_lab,
        "cargos_partidarios":         filas_cpart,
        "eleccion_popular":           filas_epop,
        "sentencias":                 filas_sent,
        "bienes_muebles_inmuebles":   filas_bienes,
        "informacion_adicional":      filas_info_ad,
    }
    return datos, tablas


# =============================================================
#  NÚCLEO: procesar todos los candidatos de UN partido
#  con un SOLO browser abierto durante todo el partido.
#  Esto elimina ~70% del overhead de la v19.
# =============================================================

async def procesar_partido(browser, url, dep_valor, dep_nombre,
                            p_idx, nombre_partido, total_cand,
                            cargo_override, prefijo, fuente_url):
    """
    Abre una sola page y navega dentro del mismo browser
    para todos los candidatos del partido.
    Devuelve lista de (datos, tablas) exitosos.
    Intenta recuperar nombre y DNI incluso en caso de error
    para que el log sea útil para corrección manual.
    """
    resultados = []
    page = await browser.new_page()

    for c_idx in range(1, total_cand + 1):
        id_candidato = (
            f"{prefijo}_{dep_nombre}_{p_idx:02d}_{c_idx:03d}"
            if dep_nombre
            else f"{prefijo}_{p_idx:02d}_{c_idx:03d}"
        )
        print(f"    [{c_idx}/{total_cand}]", end=" ", flush=True)

        # Datos mínimos para el log en caso de error
        nombre_error = None
        dni_error    = None

        try:
            # Pausa aleatoria entre candidatos para no saturar el servidor
            await asyncio.sleep(random.uniform(PAUSA_MIN, PAUSA_MAX))

            # Navegar desde cero al listado del partido
            await goto_con_reintento(page, url)
            if dep_valor:
                await page.select_option("#departamento", dep_valor)
                await page.wait_for_load_state("networkidle")

            nombre_ok = await seleccionar_partido(page, p_idx)
            if not nombre_ok:
                registrar_error(id_candidato, fuente_url, dep_nombre,
                                nombre_partido, p_idx, c_idx,
                                "partido_no_encontrado",
                                cargo=cargo_override)
                continue

            # Intentar obtener nombre/DNI desde la tarjeta antes de entrar
            try:
                sel_nombre_tarjeta = (
                    f"#idcontainer-lista-candidatos > div:nth-child({c_idx}) "
                    f"> div.content-nombre-candi"
                )
                elem_nombre = await page.query_selector(sel_nombre_tarjeta)
                if elem_nombre:
                    nombre_error = (await elem_nombre.inner_text()).strip() or None
            except Exception:
                pass

            ok = await ir_a_ficha_candidato(page, c_idx)
            if not ok:
                registrar_error(id_candidato, fuente_url, dep_nombre,
                                nombre_partido, p_idx, c_idx,
                                "ficha_no_abre",
                                nombre=nombre_error,
                                cargo=cargo_override)
                continue

            # Intentar recuperar DNI y nombre desde la ficha abierta
            try:
                sel_nombre_ficha = (
                    "#content-postulacion-comparacion "
                    "> div.flex-grow.text-center.md\\:text-left > h2"
                )
                elem = await page.query_selector(sel_nombre_ficha)
                if elem:
                    nombre_error = (await elem.inner_text()).strip() or nombre_error

                sel_info = (
                    "#content-postulacion-comparacion "
                    "> div.flex-grow.text-center.md\\:text-left "
                    "> div.grid.grid-cols-1.md\\:grid-cols-2.gap-x-8.gap-y-2.text-sm.text-gray-600"
                )
                elem_info = await page.query_selector(sel_info)
                if elem_info:
                    texto  = (await elem_info.inner_text()).strip()
                    lineas = [l.strip() for l in texto.splitlines() if l.strip()]
                    for i in range(0, len(lineas) - 1, 2):
                        if "dni" in lineas[i].lower():
                            dni_error = str(lineas[i + 1]).zfill(8)
                            break
            except Exception:
                pass

            datos, tablas = await scrapear_candidato(
                page, id_candidato, nombre_partido,
                departamento=dep_nombre, cargo_override=cargo_override
            )
            resultados.append((datos, tablas))
            print(f"{datos.get('nombre', '?')} | "
                  f"Cargo={datos.get('Cargo al que postula')} | "
                  f"Univ={datos.get('Ed Universitaria')} | "
                  f"Sent={datos.get('Sentencias')}")

        except Exception as e:
            registrar_error(id_candidato, fuente_url, dep_nombre,
                            nombre_partido, p_idx, c_idx,
                            "error_extraccion", detalle=e,
                            nombre=nombre_error, dni=dni_error,
                            cargo=cargo_override)
            print(f"ERROR: {str(e)[:80]}")

    await page.close()
    return resultados


async def procesar_formula_presidencial(browser, url, f_idx, nombre_partido, prefijo):
    """
    Procesa los 3 candidatos de una fórmula presidencial con un solo browser.
    Intenta recuperar nombre y DNI incluso en caso de error.
    """
    resultados = []
    page = await browser.new_page()

    for c_idx, cargo_pres in enumerate(CARGOS_PRESIDENCIALES, start=1):
        id_candidato = f"{prefijo}_{f_idx:02d}_{c_idx}"
        print(f"    [{cargo_pres}]", end=" ", flush=True)

        nombre_error = None
        dni_error    = None

        try:
            await asyncio.sleep(random.uniform(PAUSA_MIN, PAUSA_MAX))
            await goto_con_reintento(page, url)
            partido_ok = await ir_a_formula(page, f_idx)
            if not partido_ok:
                registrar_error(id_candidato, url, None, nombre_partido,
                                f_idx, c_idx, "formula_no_encontrada",
                                cargo=cargo_pres)
                continue

            # Intentar leer nombre del candidato desde la tarjeta en la fórmula
            try:
                sel_nombre_tarjeta = (
                    "#container-formula-presidental "
                    "> div.container-body-formula-presid "
                    "> div.flex.flex-wrap.justify-center.gap-8.mb-12"
                    f" > div:nth-child({c_idx}) .nombre-candidato"
                )
                elem = await page.query_selector(sel_nombre_tarjeta)
                if elem:
                    nombre_error = (await elem.inner_text()).strip() or None
            except Exception:
                pass

            ok = await ir_a_candidato_presidencial(page, c_idx)
            if not ok:
                registrar_error(id_candidato, url, None, nombre_partido,
                                f_idx, c_idx, "ficha_no_abre",
                                nombre=nombre_error, cargo=cargo_pres)
                continue

            # Recuperar nombre y DNI desde la ficha abierta para el log
            try:
                sel_h2 = ("#content-postulacion-comparacion "
                          "> div.flex-grow.text-center.md\\:text-left > h2")
                elem = await page.query_selector(sel_h2)
                if elem:
                    nombre_error = (await elem.inner_text()).strip() or nombre_error

                sel_info = (
                    "#content-postulacion-comparacion "
                    "> div.flex-grow.text-center.md\\:text-left "
                    "> div.grid.grid-cols-1.md\\:grid-cols-2.gap-x-8.gap-y-2.text-sm.text-gray-600"
                )
                elem_info = await page.query_selector(sel_info)
                if elem_info:
                    texto  = (await elem_info.inner_text()).strip()
                    lineas = [l.strip() for l in texto.splitlines() if l.strip()]
                    for i in range(0, len(lineas) - 1, 2):
                        if "dni" in lineas[i].lower():
                            dni_error = str(lineas[i + 1]).zfill(8)
                            break
            except Exception:
                pass

            datos, tablas = await scrapear_candidato(
                page, id_candidato, nombre_partido,
                cargo_override=cargo_pres
            )
            resultados.append((datos, tablas))
            print(f"{datos.get('nombre', '?')} | "
                  f"DNI={datos.get('DNI', '?')} | "
                  f"Cargo={datos.get('Cargo al que postula')}")

        except Exception as e:
            registrar_error(id_candidato, url, None, nombre_partido,
                            f_idx, c_idx, "error_extraccion", detalle=e,
                            nombre=nombre_error, dni=dni_error, cargo=cargo_pres)
            print(f"ERROR: {str(e)[:80]}")

    await page.close()
    return resultados


# =============================================================
#  MODOS DE SCRAPING
# =============================================================

async def scrape_con_departamento(fuente, completados):
    url     = fuente["url"]
    cargo   = fuente["cargo"]
    prefijo = fuente["prefijo_id"]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        departamentos = await obtener_departamentos(page)
        await browser.close()

    print(f"\n{'='*65}")
    print(f"  {cargo} | {len(departamentos)} departamentos")
    print(f"{'='*65}")

    for dep_valor, dep_nombre in departamentos:
        print(f"\n  ── {dep_nombre} ──")

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page    = await browser.new_page()
            await page.goto(url, wait_until="networkidle")
            await seleccionar_departamento_por_valor(page, dep_valor)
            n_partidos = await obtener_n_partidos(page)
            await browser.close()

        for p_idx in range(1, n_partidos + 1):
            ckey = checkpoint_key(prefijo, dep_nombre, p_idx)
            if ckey in completados:
                print(f"  ✓ [{dep_nombre} P{p_idx}] ya procesado, se omite.")
                continue

            # Abrir partido y verificar candidatos
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                page    = await browser.new_page()
                await goto_con_reintento(page, url)
                await seleccionar_departamento_por_valor(page, dep_valor)
                nombre_partido = await seleccionar_partido(page, p_idx)
                if not nombre_partido:
                    await browser.close()
                    continue
                tiene, total_cand = await partido_tiene_candidatos(page)
                await browser.close()

            print(f"\n  Partido {p_idx}/{n_partidos}: {nombre_partido} ({total_cand} cand.)")
            if not tiene:
                print(f"    → Sin candidatos, se omite.")
                completados.add(ckey)
                checkpoint_guardar(completados)
                continue

            # Procesar todos los candidatos del partido con UN browser
            async with async_playwright() as pw:
                browser     = await pw.chromium.launch(headless=True)
                resultados  = await procesar_partido(
                    browser, url, dep_valor, dep_nombre,
                    p_idx, nombre_partido, total_cand,
                    cargo, prefijo, url
                )
                await browser.close()

            for datos, tablas in resultados:
                agregar_candidato(datos, tablas)

            completados.add(ckey)
            checkpoint_guardar(completados)
            # Pausa entre partidos
            await asyncio.sleep(PAUSA_PARTIDO)


async def scrape_sin_departamento(fuente, completados):
    url     = fuente["url"]
    cargo   = fuente["cargo"]
    prefijo = fuente["prefijo_id"]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        n_partidos = await obtener_n_partidos(page)
        await browser.close()

    print(f"\n{'='*65}")
    print(f"  {cargo} | {n_partidos} partidos")
    print(f"{'='*65}")

    for p_idx in range(1, n_partidos + 1):
        ckey = checkpoint_key(prefijo, None, p_idx)
        if ckey in completados:
            print(f"  ✓ [{prefijo} P{p_idx}] ya procesado, se omite.")
            continue

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page    = await browser.new_page()
            await page.goto(url, wait_until="networkidle")
            nombre_partido = await seleccionar_partido(page, p_idx)
            if not nombre_partido:
                await browser.close()
                continue
            tiene, total_cand = await partido_tiene_candidatos(page)
            await browser.close()

        print(f"\n  Partido {p_idx}/{n_partidos}: {nombre_partido} ({total_cand} cand.)")
        if not tiene:
            print(f"    → Sin candidatos, se omite.")
            completados.add(ckey)
            checkpoint_guardar(completados)
            continue

        async with async_playwright() as pw:
            browser    = await pw.chromium.launch(headless=True)
            resultados = await procesar_partido(
                browser, url, None, None,
                p_idx, nombre_partido, total_cand,
                cargo, prefijo, url
            )
            await browser.close()

        for datos, tablas in resultados:
            agregar_candidato(datos, tablas)

        completados.add(ckey)
        checkpoint_guardar(completados)
        await asyncio.sleep(PAUSA_PARTIDO)


async def scrape_presidencial(fuente, completados):
    url     = fuente["url"]
    prefijo = fuente["prefijo_id"]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        n_formulas = await obtener_n_formulas(page)
        await browser.close()

    print(f"\n{'='*65}")
    print(f"  Presidencial | {n_formulas} fórmulas")
    print(f"{'='*65}")

    for f_idx in range(1, n_formulas + 1):
        ckey = checkpoint_key(prefijo, None, f_idx)
        if ckey in completados:
            print(f"  ✓ [PRES F{f_idx}] ya procesado, se omite.")
            continue

        async with async_playwright() as pw:
            browser        = await pw.chromium.launch(headless=True)
            page           = await browser.new_page()
            await page.goto(url, wait_until="networkidle")
            nombre_partido = await ir_a_formula(page, f_idx)
            await browser.close()

        if not nombre_partido:
            continue

        print(f"\n  Fórmula {f_idx}/{n_formulas}: {nombre_partido}")

        async with async_playwright() as pw:
            browser    = await pw.chromium.launch(headless=True)
            resultados = await procesar_formula_presidencial(
                browser, url, f_idx, nombre_partido, prefijo
            )
            await browser.close()

        for datos, tablas in resultados:
            agregar_candidato(datos, tablas)

        completados.add(ckey)
        checkpoint_guardar(completados)
        await asyncio.sleep(PAUSA_PARTIDO)


# =============================================================
#  MAIN
# =============================================================

async def scrape_todos():
    _inicializar_log_errores()
    _inicializar_buffers(_timestamp_global)
    completados = checkpoint_cargar()

    if completados:
        print(f"  ♻ Retomando ejecución anterior ({len(completados)} partidos ya procesados)")

    print(f"  Log de errores : {_ruta_errores}")
    print(f"  Guardado cada  : {GUARDAR_CADA} candidatos")

    for fuente in FUENTES:
        if fuente["tipo"] == "con_departamento":
            await scrape_con_departamento(fuente, completados)
        elif fuente["tipo"] == "sin_departamento":
            await scrape_sin_departamento(fuente, completados)
        elif fuente["tipo"] == "presidencial":
            await scrape_presidencial(fuente, completados)

    # Volcar lo que quede en buffer
    volcar_todo()

    # Resumen final
    print(f"\n{'='*65}")
    print("  RESUMEN FINAL")
    print(f"{'='*65}")

    try:
        df_cand = pd.read_csv(_rutas_csv["candidatos"], encoding="utf-8-sig")
        if "DNI" in df_cand.columns:
            df_cand["DNI"] = df_cand["DNI"].astype(str).str.zfill(8)
            df_cand.to_csv(_rutas_csv["candidatos"], index=False, encoding="utf-8-sig")
        print(f"  Total candidatos : {len(df_cand)}")
        if "Cargo al que postula" in df_cand.columns:
            print("\n  Por cargo:")
            print(df_cand["Cargo al que postula"].value_counts().to_string())
    except Exception:
        pass

    try:
        df_err = pd.read_csv(_ruta_errores, encoding="utf-8-sig")
        if len(df_err) > 0:
            print(f"\n  Errores: {len(df_err)} → {_ruta_errores}")
            print(df_err["motivo"].value_counts().to_string())
        else:
            print("\n  ✓ Sin errores.")
    except Exception:
        pass

    # Limpiar checkpoint si todo terminó bien
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("\n  ✓ Checkpoint eliminado (ejecución completa).")

    print(f"\n  Archivos en: {OUTPUT_DIR}/")


if __name__ == "__main__":
    asyncio.run(scrape_todos())
