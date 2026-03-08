"""
corregir_sentencias.py
======================
Re-scrapea SOLO la sección de sentencias para todos los candidatos
ya registrados en candidatos.csv, y corrige:
  - sentencias.csv   → reescrito completo con datos correctos
  - candidatos.csv   → columna "Sentencias" actualizada (0/1)

Uso:
    conda activate scraper
    python corregir_sentencias.py

Los archivos deben estar en la carpeta DATA_DIR definida abajo.
El script usa el campo "id_candidato" para reconstruir la URL de cada
ficha navegando igual que el scraper original.
"""

import asyncio
import random
import pandas as pd
from playwright.async_api import async_playwright
import os
import csv
from datetime import datetime

# =============================================================
#  CONFIGURACIÓN — ajusta si tus rutas son distintas
# =============================================================

DATA_DIR          = "data"
CAND_CSV          = os.path.join(DATA_DIR, "candidatos.csv")
SENT_CSV          = os.path.join(DATA_DIR, "sentencias.csv")
LOG_CSV           = os.path.join(DATA_DIR, f"errores_sentencias_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

PAUSA_MIN         = 0.8    # segundos entre candidatos
PAUSA_MAX         = 2.0
PAUSA_RESET       = 60     # espera tras ERR_CONNECTION_RESET
MAX_REINTENTOS    = 3
GUARDAR_CADA      = 50     # vuelca resultados parciales cada N candidatos

BASE = (
    "body > app-root > div > main > app-hoja-vida > div "
    "> div.max-w-5xl.mx-auto.px-4.py-6 > div.space-y-4"
)
SEL_SENTENCIAS_TBODY = f"{BASE} > div:nth-child(8) > div > table > tbody"

URLS = {
    "DIP":  "https://votoinformado.jne.gob.pe/diputados",
    "SEN":  "https://votoinformado.jne.gob.pe/senadores",
    "PAR":  "https://votoinformado.jne.gob.pe/parlamento-andino",
    "PRES": "https://votoinformado.jne.gob.pe/presidente-vicepresidentes",
}

# =============================================================
#  HELPERS
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
                "ERR_EMPTY_RESPONSE", "net::"
            ])
            if es_reset and intento < MAX_REINTENTOS:
                print(f"\n    ⚠ Conexión cortada. Esperando {PAUSA_RESET}s...", flush=True)
                await asyncio.sleep(PAUSA_RESET)
            else:
                raise
    return False


async def extraer_sentencias(page, dni, id_candidato):
    """
    Extrae sentencias usando posición (td:nth-child) en lugar de
    clases CSS, que es más robusto ante variaciones de Angular.
    Devuelve (tiene_sentencias: int, filas: list[dict])
    """
    try:
        tbody = await page.query_selector(SEL_SENTENCIAS_TBODY)
        if tbody is None:
            return 0, []

        filas_html = await tbody.query_selector_all("tr")
        if not filas_html:
            return 0, []

        filas = []
        for fila in filas_html:
            # Usar posición en lugar de clases CSS — más robusto
            celdas = await fila.query_selector_all("td")
            if len(celdas) < 2:
                continue

            materia = (await celdas[0].inner_text()).strip() or None
            fallo   = (await celdas[1].inner_text()).strip() or None

            if materia or fallo:
                filas.append({
                    "DNI":               dni,
                    "id_candidato":      id_candidato,
                    "Sentencia_Materia": materia,
                    "Sentencia_Fallo":   fallo,
                })

        return (1, filas) if filas else (0, [])

    except Exception as e:
        print(f"    Error extrayendo sentencias: {e}")
        return 0, []


def reconstruir_url_y_params(id_candidato):
    """
    A partir del id_candidato reconstruye:
      - prefijo (DIP/SEN/PAR/PRES)
      - url base
      - departamento (solo para DIP)
      - p_idx (índice del partido)
      - c_idx (índice del candidato dentro del partido)
    """
    partes = id_candidato.split("_")
    prefijo = partes[0]

    if prefijo == "DIP":
        # DIP_DEPARTAMENTO_PP_CCC
        # puede haber departamentos con espacios convertidos en _
        # el formato es: DIP_{DEP}_{p_idx:02d}_{c_idx:03d}
        c_idx   = int(partes[-1])
        p_idx   = int(partes[-2])
        dep     = "_".join(partes[1:-2])
        return prefijo, dep, p_idx, c_idx

    elif prefijo in ("SEN", "PAR"):
        # SEN_PP_CCC
        c_idx = int(partes[-1])
        p_idx = int(partes[-2])
        return prefijo, None, p_idx, c_idx

    elif prefijo == "PRES":
        # PRES_FF_C
        f_idx = int(partes[1])
        c_idx = int(partes[2])
        return prefijo, None, f_idx, c_idx

    return None, None, None, None


async def navegar_a_candidato(page, id_candidato, df_cand_row):
    """
    Navega a la ficha del candidato usando la misma lógica
    que el scraper original.
    Devuelve True si llegó a la ficha, False si falló.
    """
    prefijo, dep, p_idx, c_idx = reconstruir_url_y_params(id_candidato)
    if prefijo is None:
        return False

    url = URLS.get(prefijo)
    if url is None:
        return False

    try:
        await goto_con_reintento(page, url)

        # ── Diputados: seleccionar departamento ──
        if prefijo == "DIP" and dep:
            try:
                await page.wait_for_selector("#departamento", timeout=8000)
                # Buscar la opción cuyo texto coincide con el departamento
                opciones = await page.query_selector_all("#departamento option")
                valor_dep = None
                for op in opciones:
                    texto = (await op.inner_text()).strip().upper()
                    if texto == dep.upper():
                        valor_dep = await op.get_attribute("value")
                        break
                if valor_dep:
                    await page.select_option("#departamento", valor_dep)
                    await page.wait_for_load_state("networkidle")
                else:
                    print(f"    No encontré departamento: {dep}")
                    return False
            except Exception as e:
                print(f"    Error seleccionando departamento {dep}: {e}")
                return False

        # ── Presidencial: navegar a fórmula ──
        if prefijo == "PRES":
            f_idx = p_idx  # en PRES, p_idx es f_idx
            selector_formula = f"#idcontainer-lista-candidatos > div:nth-child({f_idx})"
            try:
                await page.wait_for_selector(selector_formula, timeout=5000)
                tarjeta = await page.query_selector(selector_formula)
                if not tarjeta:
                    return False
                await tarjeta.click()
                await page.wait_for_load_state("networkidle")

                # Abrir el candidato dentro de la fórmula
                sel_cand = (
                    "#container-formula-presidental "
                    "> div.container-body-formula-presid "
                    "> div.flex.flex-wrap.justify-center.gap-8.mb-12"
                    f" > div:nth-child({c_idx})"
                )
                await page.wait_for_selector(sel_cand, timeout=5000)
                elem = await page.query_selector(sel_cand)
                if not elem:
                    return False
                await elem.click()
                await page.wait_for_load_state("networkidle")
                return True
            except Exception as e:
                print(f"    Error navegando a fórmula presidencial: {e}")
                return False

        # ── SEN, PAR, DIP: seleccionar partido y candidato ──
        try:
            await page.wait_for_selector(
                "app-candidatos-presidenciales section div.grid div h3", timeout=8000
            )
            elems = await page.query_selector_all(
                "app-candidatos-presidenciales section div.grid div h3"
            )
            if not elems or p_idx > len(elems):
                print(f"    Partido {p_idx} no encontrado (hay {len(elems)})")
                return False
            await elems[p_idx - 1].click()
            await page.wait_for_load_state("networkidle")
        except Exception as e:
            print(f"    Error seleccionando partido {p_idx}: {e}")
            return False

        # Seleccionar candidato
        selector_cand = f"#idcontainer-lista-candidatos > div:nth-child({c_idx})"
        try:
            await page.wait_for_selector(selector_cand, timeout=5000)
            tarjeta = await page.query_selector(selector_cand)
            if not tarjeta:
                return False
            await tarjeta.click()
            await page.wait_for_load_state("networkidle")
            return True
        except Exception as e:
            print(f"    Error abriendo ficha candidato {c_idx}: {e}")
            return False

    except Exception as e:
        print(f"    Error navegando a {id_candidato}: {e}")
        return False


# =============================================================
#  MAIN
# =============================================================

async def corregir_sentencias():
    print("=" * 65)
    print("  CORRECCIÓN DE SENTENCIAS")
    print("=" * 65)

    # Cargar candidatos
    df_cand = pd.read_csv(CAND_CSV, dtype=str, encoding="utf-8-sig")
    df_cand["DNI"] = df_cand["DNI"].astype(str).str.zfill(8)
    total = len(df_cand)
    print(f"  Candidatos a procesar: {total}")
    print(f"  Log de errores: {LOG_CSV}\n")

    # Inicializar log de errores
    with open(LOG_CSV, "w", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=[
            "DNI", "id_candidato", "nombre", "partido", "motivo", "timestamp"
        ]).writeheader()

    # Acumuladores
    todas_sentencias = []   # filas para sentencias.csv
    # Mapa DNI → tiene_sentencias para actualizar candidatos.csv
    mapa_sentencias  = {}   # {dni: 0 o 1}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()

        for i, row in df_cand.iterrows():
            dni          = str(row.get("DNI", "")).zfill(8)
            id_candidato = str(row.get("id_candidato", ""))
            nombre       = str(row.get("nombre", ""))
            partido      = str(row.get("partido", ""))

            print(f"  [{i+1}/{total}] {nombre[:40]:<40}", end=" ", flush=True)

            await asyncio.sleep(random.uniform(PAUSA_MIN, PAUSA_MAX))

            try:
                ok = await navegar_a_candidato(page, id_candidato, row)

                if not ok:
                    print("→ NO NAVEGÓ")
                    mapa_sentencias[dni] = int(str(row.get("Sentencias", "0")).strip() or 0)
                    with open(LOG_CSV, "a", newline="", encoding="utf-8-sig") as f:
                        csv.DictWriter(f, fieldnames=[
                            "DNI","id_candidato","nombre","partido","motivo","timestamp"
                        ]).writerow({
                            "DNI": dni, "id_candidato": id_candidato,
                            "nombre": nombre, "partido": partido,
                            "motivo": "navegacion_fallida",
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                    continue

                tiene, filas = await extraer_sentencias(page, dni, id_candidato)
                mapa_sentencias[dni] = tiene
                todas_sentencias.extend(filas)

                if tiene:
                    print(f"→ {len(filas)} sentencia(s) ✓")
                else:
                    print("→ sin sentencias")

            except Exception as e:
                print(f"→ ERROR: {str(e)[:60]}")
                mapa_sentencias[dni] = int(str(row.get("Sentencias", "0")).strip() or 0)
                with open(LOG_CSV, "a", newline="", encoding="utf-8-sig") as f:
                    csv.DictWriter(f, fieldnames=[
                        "DNI","id_candidato","nombre","partido","motivo","timestamp"
                    ]).writerow({
                        "DNI": dni, "id_candidato": id_candidato,
                        "nombre": nombre, "partido": partido,
                        "motivo": str(e)[:200],
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })

            # Guardado parcial cada GUARDAR_CADA candidatos
            if (i + 1) % GUARDAR_CADA == 0:
                _guardar_parcial(df_cand, mapa_sentencias, todas_sentencias)
                print(f"\n  💾 Guardado parcial ({i+1}/{total})\n")

        await browser.close()

    # Guardado final
    print("\n" + "=" * 65)
    print("  GUARDADO FINAL")
    print("=" * 65)
    _guardar_final(df_cand, mapa_sentencias, todas_sentencias)


def _guardar_parcial(df_cand, mapa_sentencias, todas_sentencias):
    """Guarda estado intermedio sin sobreescribir los originales todavía."""
    # sentencias parcial
    if todas_sentencias:
        pd.DataFrame(todas_sentencias).to_csv(
            os.path.join(DATA_DIR, "sentencias_PARCIAL.csv"),
            index=False, encoding="utf-8-sig"
        )
    # candidatos parcial
    df_tmp = df_cand.copy()
    df_tmp["Sentencias"] = df_tmp["DNI"].map(
        lambda d: mapa_sentencias.get(str(d).zfill(8), None)
    ).combine_first(df_tmp["Sentencias"])
    df_tmp.to_csv(
        os.path.join(DATA_DIR, "candidatos_PARCIAL.csv"),
        index=False, encoding="utf-8-sig"
    )


def _guardar_final(df_cand, mapa_sentencias, todas_sentencias):
    # ── sentencias.csv ──
    if todas_sentencias:
        df_sent = pd.DataFrame(todas_sentencias)
        df_sent.to_csv(SENT_CSV, index=False, encoding="utf-8-sig")
        print(f"  sentencias.csv → {len(df_sent)} filas")
    else:
        # Dejar el CSV con solo el header si no hay ninguna
        pd.DataFrame(columns=["DNI","id_candidato","Sentencia_Materia","Sentencia_Fallo"]
                    ).to_csv(SENT_CSV, index=False, encoding="utf-8-sig")
        print("  sentencias.csv → 0 filas (ningún candidato tiene sentencias registradas)")

    # ── candidatos.csv: actualizar columna Sentencias ──
    df_cand["Sentencias"] = df_cand["DNI"].apply(
        lambda d: mapa_sentencias.get(str(d).zfill(8), None)
    )
    # Para los que no se pudieron navegar, dejar el valor original
    # (ya están en mapa_sentencias con su valor anterior)
    df_cand.to_csv(CAND_CSV, index=False, encoding="utf-8-sig")
    print(f"  candidatos.csv → columna Sentencias actualizada")

    # Resumen
    con_sent = sum(1 for v in mapa_sentencias.values() if v == 1)
    sin_sent = sum(1 for v in mapa_sentencias.values() if v == 0)
    no_proc  = len(df_cand) - len(mapa_sentencias)
    print(f"\n  Con sentencias    : {con_sent}")
    print(f"  Sin sentencias    : {sin_sent}")
    print(f"  No procesados     : {no_proc}")
    print(f"  Log de errores    : {LOG_CSV}")

    # Limpiar parciales si todo fue bien
    for f in ["sentencias_PARCIAL.csv", "candidatos_PARCIAL.csv"]:
        ruta = os.path.join(DATA_DIR, f)
        if os.path.exists(ruta):
            os.remove(ruta)


if __name__ == "__main__":
    asyncio.run(corregir_sentencias())
