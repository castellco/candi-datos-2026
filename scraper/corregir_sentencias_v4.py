"""
corregir_sentencias_v4.py
=========================
ESTRATEGIA FINAL:
  1. Mapeo de partidos: lee el src del <img> en cada tarjeta de partido.
     El partido_id está directamente en la URL del logo:
     "https://sroppublico.jne.gob.pe/Consulta/Simbolo/GetSimbolo/2980"
     → partido_id = "2980", nombre = atributo alt del img.
     NO hace clicks — solo lee atributos. Mucho más rápido y fiable.

  2. Para diputados recorre todos los departamentos para cubrir todos los partidos.
     Para senadores/parlamento andino/presidencial no hace falta.

  3. Con el mapa construido, para cada candidato en candidatos.csv:
     URL directa = "https://votoinformado.jne.gob.pe/hoja-vida/{partido_id}/{dni}"

  4. Extrae sentencias usando posición (td:nth-child) — no clases CSS.

  5. Actualiza SOLO columnas "Sentencias", "url", "partido_id" en candidatos.csv.
     NO toca ninguna otra columna.

  6. Reescribe sentencias.csv completo.

  7. Crea/actualiza partidos.csv con nombre, partido_id, logo_url.

Uso:
    conda activate scraper
    python corregir_sentencias_v4.py

Si ya existe data/mapa_partidos_v4.json lo reutiliza (bórralo para reconstruir).
"""

import asyncio
import random
import json
import os
import csv
import re

import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime

# =============================================================
#  CONFIGURACIÓN
# =============================================================

DATA_DIR   = "data"
CAND_CSV   = os.path.join(DATA_DIR, "candidatos.csv")
SENT_CSV   = os.path.join(DATA_DIR, "sentencias.csv")
PART_CSV   = os.path.join(DATA_DIR, "partidos.csv")
MAPA_FILE  = os.path.join(DATA_DIR, "mapa_partidos_v4.json")
LOG_CSV    = os.path.join(DATA_DIR, f"errores_sent_v4_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

PAUSA_MIN      = 0.5
PAUSA_MAX      = 1.5
PAUSA_RESET    = 60
MAX_REINTENTOS = 3
GUARDAR_CADA   = 100

BASE_JNE   = "https://votoinformado.jne.gob.pe"
LOGO_BASE  = "https://sroppublico.jne.gob.pe/Consulta/Simbolo/GetSimbolo"

# Selector del grid de partidos (funciona en diputados, senadores, parlamento andino)
SEL_GRID_IMGS = "app-candidatos-presidenciales section div.grid div img"

# Selector del grid de fórmulas presidenciales
SEL_PRES_IMGS = "#idcontainer-lista-candidatos div.content-foto-candi img"

# Selector de sentencias
SEL_SENT_TBODY = (
    "body > app-root > div > main > app-hoja-vida > div "
    "> div.max-w-5xl.mx-auto.px-4.py-6 > div.space-y-4 "
    "> div:nth-child(8) > div > table > tbody"
)
SEL_SENT_THEAD = (
    "body > app-root > div > main > app-hoja-vida > div "
    "> div.max-w-5xl.mx-auto.px-4.py-6 > div.space-y-4 "
    "> div:nth-child(8) > div > table > thead"
)

FUENTES = [
    {"url": f"{BASE_JNE}/diputados",                  "tipo": "con_departamento"},
    {"url": f"{BASE_JNE}/senadores",                  "tipo": "sin_departamento"},
    {"url": f"{BASE_JNE}/parlamento-andino",           "tipo": "sin_departamento"},
    {"url": f"{BASE_JNE}/presidente-vicepresidentes",  "tipo": "presidencial"},
]

# =============================================================
#  HELPERS GENERALES
# =============================================================

async def goto_con_reintento(page, url):
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
            return True
        except Exception as e:
            msg = str(e)
            es_reset = any(x in msg for x in [
                "ERR_CONNECTION_RESET", "ERR_CONNECTION_REFUSED",
                "ERR_EMPTY_RESPONSE", "net::",
            ])
            if es_reset and intento < MAX_REINTENTOS:
                print(f"\n    ⚠ Conexión cortada. Esperando {PAUSA_RESET}s...", flush=True)
                await asyncio.sleep(PAUSA_RESET)
            else:
                raise
    return False


def partido_id_desde_src(src: str):
    """Extrae el número al final de la URL del logo."""
    m = re.search(r"/(\d+)\s*$", src.strip())
    return m.group(1) if m else None


def escribir_error(dni, id_candidato, nombre, partido, motivo):
    with open(LOG_CSV, "a", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=[
            "DNI", "id_candidato", "nombre", "partido", "motivo", "timestamp"
        ]).writerow({
            "DNI": dni, "id_candidato": id_candidato,
            "nombre": nombre, "partido": partido,
            "motivo": str(motivo)[:300],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })


# =============================================================
#  PASO 1: CONSTRUIR MAPA partido_nombre → partido_id
#  Lee img src sin hacer ningún click
# =============================================================

async def leer_imgs_de_pagina(page, selector):
    """
    Lee todos los <img> que coincidan con el selector
    y devuelve lista de (nombre, partido_id, logo_url).
    """
    resultados = []
    try:
        await page.wait_for_selector(selector, timeout=8000)
        imgs = await page.query_selector_all(selector)
        for img in imgs:
            src  = (await img.get_attribute("src") or "").strip()
            alt  = (await img.get_attribute("alt") or "").strip()
            pid  = partido_id_desde_src(src)
            if pid and alt:
                resultados.append((alt, pid, src))
    except Exception:
        pass
    return resultados


async def construir_mapa_partidos():
    """
    Visita cada fuente y lee los img sin hacer clicks.
    Para diputados, recorre todos los departamentos.
    Devuelve (mapa: dict nombre_upper→id, info: list de dicts para partidos.csv)
    """
    mapa = {}   # nombre.upper() → partido_id
    info = {}   # partido_id → {nombre, partido_id, logo_url}

    def registrar(nombre, pid, logo):
        mapa[nombre.upper()] = pid
        if pid not in info:
            info[pid] = {"nombre": nombre, "partido_id": pid, "logo_url": logo}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()

        for fuente in FUENTES:
            url  = fuente["url"]
            tipo = fuente["tipo"]
            print(f"\n  Mapeando: {url}")

            await goto_con_reintento(page, url)

            if tipo == "presidencial":
                items = await leer_imgs_de_pagina(page, SEL_PRES_IMGS)
                for nombre, pid, logo in items:
                    registrar(nombre, pid, logo)
                    print(f"    {nombre[:55]:<55} → {pid}")

            elif tipo == "sin_departamento":
                items = await leer_imgs_de_pagina(page, SEL_GRID_IMGS)
                for nombre, pid, logo in items:
                    registrar(nombre, pid, logo)
                    print(f"    {nombre[:55]:<55} → {pid}")

            elif tipo == "con_departamento":
                # Obtener todos los departamentos
                try:
                    await page.wait_for_selector("#departamento", timeout=8000)
                    opciones = await page.query_selector_all("#departamento option")
                    departamentos = []
                    for op in opciones:
                        texto = (await op.inner_text()).strip()
                        valor = await op.get_attribute("value")
                        if valor and texto.lower() not in ("seleccione", "todos", ""):
                            departamentos.append((valor, texto))
                except Exception:
                    departamentos = []

                print(f"    {len(departamentos)} departamentos")

                for dep_valor, dep_nombre in departamentos:
                    await goto_con_reintento(page, url)
                    await page.wait_for_selector("#departamento", timeout=8000)
                    await page.select_option("#departamento", dep_valor)
                    await page.wait_for_load_state("networkidle")

                    items = await leer_imgs_de_pagina(page, SEL_GRID_IMGS)
                    nuevos = 0
                    for nombre, pid, logo in items:
                        if nombre.upper() not in mapa:
                            registrar(nombre, pid, logo)
                            nuevos += 1
                    print(f"    {dep_nombre:<20} → {len(items)} partidos ({nuevos} nuevos)")

        await browser.close()

    return mapa, list(info.values())


# =============================================================
#  PASO 2: EXTRAER SENTENCIAS DE UNA FICHA (URL directa)
# =============================================================

async def extraer_sentencias(page, dni, id_candidato):
    """
    Extrae sentencias por posición (td[0], td[1]) — no por clase CSS.
    Verifica antes que el thead contenga 'MATERIA' para confirmar
    que es la sección correcta y que Angular ya la renderizó.
    """
    try:
        # Esperar renderizado de Angular
        await asyncio.sleep(0.6)

        # Confirmar que la sección de sentencias existe
        thead = await page.query_selector(SEL_SENT_THEAD)
        if thead is None:
            return 0, []
        thead_txt = (await thead.inner_text()).strip().upper()
        if "MATERIA" not in thead_txt:
            return 0, []

        tbody = await page.query_selector(SEL_SENT_TBODY)
        if tbody is None:
            return 0, []

        filas_html = await tbody.query_selector_all("tr")
        if not filas_html:
            return 0, []

        filas = []
        for fila in filas_html:
            celdas = await fila.query_selector_all("td")
            if len(celdas) < 2:
                continue
            materia = (await celdas[0].inner_text()).strip() or None
            fallo   = (await celdas[1].inner_text()).strip() or None
            if materia or fallo:
                filas.append({
                    "DNI":               str(dni).zfill(8),
                    "id_candidato":      id_candidato,
                    "Sentencia_Materia": materia,
                    "Sentencia_Fallo":   fallo,
                })

        return (1, filas) if filas else (0, [])

    except Exception as e:
        print(f"    ⚠ Error sentencias: {e}")
        return 0, []


# =============================================================
#  PASO 3: CORREGIR SENTENCIAS EN candidatos.csv
# =============================================================

async def corregir_sentencias(mapa_partidos):
    df = pd.read_csv(CAND_CSV, dtype=str, encoding="utf-8-sig")
    df["DNI"] = df["DNI"].astype(str).str.zfill(8)

    # Añadir columnas nuevas si no existen
    for col in ("url", "partido_id"):
        if col not in df.columns:
            df[col] = None

    total = len(df)
    print(f"\n  Candidatos a procesar: {total}")

    todas_sentencias = []
    # Guardar valores actualizados por DNI
    actualizaciones = {}   # dni → {Sentencias, url, partido_id}

    # Candidatos cuyo partido no está en el mapa
    sin_mapa = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()

        for i, row in df.iterrows():
            dni          = str(row["DNI"]).zfill(8)
            id_candidato = str(row.get("id_candidato", ""))
            nombre       = str(row.get("nombre", ""))
            partido_raw  = str(row.get("partido", ""))

            print(f"  [{i+1}/{total}] {nombre[:38]:<38}", end=" ", flush=True)

            # ── Buscar partido_id ──
            partido_id = mapa_partidos.get(partido_raw.upper())
            if not partido_id:
                # Búsqueda parcial (por si hay pequeñas diferencias de texto)
                for k, v in mapa_partidos.items():
                    if partido_raw.upper() in k or k in partido_raw.upper():
                        partido_id = v
                        break

            if not partido_id:
                print(f"→ partido no mapeado: {partido_raw[:40]}")
                sin_mapa.append(partido_raw)
                # Mantener valor existente
                actualizaciones[dni] = {
                    "Sentencias": str(row.get("Sentencias", "0") or "0"),
                    "url":        str(row.get("url", "") or ""),
                    "partido_id": "",
                }
                continue

            url_ficha = f"{BASE_JNE}/hoja-vida/{partido_id}/{dni}"

            try:
                await asyncio.sleep(random.uniform(PAUSA_MIN, PAUSA_MAX))
                await goto_con_reintento(page, url_ficha)

                tiene, filas = await extraer_sentencias(page, dni, id_candidato)
                todas_sentencias.extend(filas)

                actualizaciones[dni] = {
                    "Sentencias": str(tiene),
                    "url":        url_ficha,
                    "partido_id": partido_id,
                }

                if tiene:
                    print(f"→ ✓ {len(filas)} sentencia(s)  [{url_ficha}]")
                else:
                    print("→ sin sentencias")

            except Exception as e:
                print(f"→ ERROR: {str(e)[:60]}")
                actualizaciones[dni] = {
                    "Sentencias": str(row.get("Sentencias", "0") or "0"),
                    "url":        url_ficha,
                    "partido_id": partido_id,
                }
                escribir_error(dni, id_candidato, nombre, partido_raw, e)

            # Guardado parcial
            if (i + 1) % GUARDAR_CADA == 0:
                _guardar_parcial(df, actualizaciones, todas_sentencias)
                print(f"\n  💾 Guardado parcial ({i+1}/{total})\n")

        await browser.close()

    return df, actualizaciones, todas_sentencias, sin_mapa


# =============================================================
#  GUARDADO
# =============================================================

def _aplicar_actualizaciones(df, actualizaciones):
    """Aplica el dict de actualizaciones al DataFrame SIN tocar otras columnas."""
    for dni, vals in actualizaciones.items():
        mask = df["DNI"] == str(dni).zfill(8)
        df.loc[mask, "Sentencias"] = vals["Sentencias"]
        df.loc[mask, "url"]        = vals["url"]
        df.loc[mask, "partido_id"] = vals["partido_id"]
    return df


def _guardar_parcial(df, actualizaciones, todas_sentencias):
    df_tmp = _aplicar_actualizaciones(df.copy(), actualizaciones)
    df_tmp.to_csv(
        os.path.join(DATA_DIR, "candidatos_PARCIAL.csv"),
        index=False, encoding="utf-8-sig"
    )
    if todas_sentencias:
        pd.DataFrame(todas_sentencias).to_csv(
            os.path.join(DATA_DIR, "sentencias_PARCIAL.csv"),
            index=False, encoding="utf-8-sig"
        )


def guardar_final(df, actualizaciones, todas_sentencias, info_partidos, sin_mapa):
    print("\n" + "=" * 65)
    print("  GUARDADO FINAL")
    print("=" * 65)

    # ── candidatos.csv: solo Sentencias, url, partido_id ──
    df = _aplicar_actualizaciones(df, actualizaciones)
    df.to_csv(CAND_CSV, index=False, encoding="utf-8-sig")
    print(f"  candidatos.csv  → Sentencias + url + partido_id actualizados")

    # ── sentencias.csv ──
    if todas_sentencias:
        df_sent = pd.DataFrame(todas_sentencias)
        df_sent.to_csv(SENT_CSV, index=False, encoding="utf-8-sig")
        print(f"  sentencias.csv  → {len(df_sent)} filas")
    else:
        pd.DataFrame(columns=["DNI","id_candidato","Sentencia_Materia","Sentencia_Fallo"]
                    ).to_csv(SENT_CSV, index=False, encoding="utf-8-sig")
        print("  sentencias.csv  → 0 filas")

    # ── partidos.csv ──
    if info_partidos:
        df_part = (pd.DataFrame(info_partidos)
                   .drop_duplicates(subset=["partido_id"])
                   .sort_values("nombre"))
        df_part.to_csv(PART_CSV, index=False, encoding="utf-8-sig")
        print(f"  partidos.csv    → {len(df_part)} partidos")

    # ── Resumen ──
    con_sent = sum(1 for v in actualizaciones.values() if v["Sentencias"] == "1")
    sin_sent = sum(1 for v in actualizaciones.values() if v["Sentencias"] == "0")
    no_proc  = len(df) - len(actualizaciones)
    print(f"\n  Con sentencias  : {con_sent}")
    print(f"  Sin sentencias  : {sin_sent}")
    print(f"  No procesados   : {no_proc}")
    if sin_mapa:
        unicos = sorted(set(sin_mapa))
        print(f"\n  Partidos no mapeados ({len(unicos)}):")
        for p in unicos:
            print(f"    · {p}")
    print(f"\n  Log errores     : {LOG_CSV}")

    # Limpiar parciales
    for fname in ["candidatos_PARCIAL.csv", "sentencias_PARCIAL.csv"]:
        ruta = os.path.join(DATA_DIR, fname)
        if os.path.exists(ruta):
            os.remove(ruta)


# =============================================================
#  MAIN
# =============================================================

async def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Inicializar log
    with open(LOG_CSV, "w", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=[
            "DNI","id_candidato","nombre","partido","motivo","timestamp"
        ]).writeheader()

    print("=" * 65)
    print("  CORRECCIÓN DE SENTENCIAS v4")
    print("  Estrategia: img src → partido_id → URL directa")
    print("=" * 65)

    # ── Paso 1: mapa de partidos ──
    if os.path.exists(MAPA_FILE):
        print(f"\n  Cargando mapa desde caché: {MAPA_FILE}")
        with open(MAPA_FILE, "r", encoding="utf-8") as f:
            datos = json.load(f)
        mapa_partidos = datos["mapa"]
        info_partidos = datos["info"]
        print(f"  {len(mapa_partidos)} partidos en caché")
    else:
        print("\n  PASO 1: Construyendo mapa partido → partido_id")
        mapa_partidos, info_partidos = await construir_mapa_partidos()
        with open(MAPA_FILE, "w", encoding="utf-8") as f:
            json.dump({"mapa": mapa_partidos, "info": info_partidos},
                      f, ensure_ascii=False, indent=2)
        print(f"\n  {len(mapa_partidos)} partidos mapeados → {MAPA_FILE}")

    # ── Paso 2: corregir sentencias ──
    print("\n  PASO 2: Visitando fichas y extrayendo sentencias")
    df, actualizaciones, todas_sentencias, sin_mapa = await corregir_sentencias(mapa_partidos)

    # ── Paso 3: guardar ──
    guardar_final(df, actualizaciones, todas_sentencias, info_partidos, sin_mapa)


if __name__ == "__main__":
    asyncio.run(main())
