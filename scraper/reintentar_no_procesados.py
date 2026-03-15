"""
reintentar_no_procesados.py
===========================
Lee el log de errores del v4 y reintenta SOLO los candidatos fallidos.
Actualiza candidatos.csv y sentencias.csv sin tocar el resto.

Uso:
    python reintentar_no_procesados.py
    
    Si tienes varios logs de error, edita la variable LOG_ERRORES abajo
    para apuntar al archivo correcto.
"""

import asyncio
import random
import os
import csv
import glob
import json

import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime

# =============================================================
#  CONFIGURACIÓN
# =============================================================

DATA_DIR  = "data"
CAND_CSV  = os.path.join(DATA_DIR, "candidatos.csv")
SENT_CSV  = os.path.join(DATA_DIR, "sentencias.csv")
MAPA_FILE = os.path.join(DATA_DIR, "mapa_partidos_v4.json")

# Toma automáticamente el log de errores más reciente del v4
logs = sorted(glob.glob(os.path.join(DATA_DIR, "errores_sent_v4_*.csv")))
LOG_ERRORES = logs[-1] if logs else None

PAUSA_MIN      = 0.8
PAUSA_MAX      = 2.0
PAUSA_RESET    = 60
MAX_REINTENTOS = 3

BASE_JNE = "https://votoinformado.jne.gob.pe"

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
                "ERR_EMPTY_RESPONSE", "net::",
            ])
            if es_reset and intento < MAX_REINTENTOS:
                print(f"\n    ⚠ Conexión cortada. Esperando {PAUSA_RESET}s...", flush=True)
                await asyncio.sleep(PAUSA_RESET)
            else:
                raise
    return False


async def extraer_sentencias(page, dni, id_candidato):
    try:
        await asyncio.sleep(0.6)
        thead = await page.query_selector(SEL_SENT_THEAD)
        if thead is None:
            return 0, []
        if "MATERIA" not in (await thead.inner_text()).upper():
            return 0, []
        tbody = await page.query_selector(SEL_SENT_TBODY)
        if tbody is None:
            return 0, []
        filas_html = await tbody.query_selector_all("tr")
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
        print(f"    ⚠ Error: {e}")
        return 0, []


# =============================================================
#  MAIN
# =============================================================

async def main():
    print("=" * 65)
    print("  REINTENTO — candidatos no procesados")
    print("=" * 65)

    if not LOG_ERRORES or not os.path.exists(LOG_ERRORES):
        print("  ✗ No se encontró log de errores en data/errores_sent_v4_*.csv")
        return

    print(f"  Log de errores : {LOG_ERRORES}")

    # ── Cargar mapa de partidos ──
    if not os.path.exists(MAPA_FILE):
        print(f"  ✗ No se encontró {MAPA_FILE}. Corre primero corregir_sentencias_v4.py")
        return

    with open(MAPA_FILE, "r", encoding="utf-8") as f:
        mapa_partidos = json.load(f)["mapa"]

    # ── Identificar candidatos no procesados ──
    # Método 1: los que están en el log de errores (excepción durante navegación)
    df_log = pd.read_csv(LOG_ERRORES, dtype=str, encoding="utf-8-sig")
    dnis_en_log = set(df_log["DNI"].astype(str).str.zfill(8).tolist())

    # Método 2: los que en candidatos.csv tienen url o partido_id vacíos
    # (se saltaron silenciosamente porque el partido no estaba en el mapa)
    df_cand_check = pd.read_csv(CAND_CSV, dtype=str, encoding="utf-8-sig")
    df_cand_check["DNI"] = df_cand_check["DNI"].astype(str).str.zfill(8)

    sin_url = df_cand_check[
        df_cand_check["url"].isna() | (df_cand_check["url"] == "") |
        df_cand_check["partido_id"].isna() | (df_cand_check["partido_id"] == "")
    ][["DNI", "id_candidato", "nombre", "partido"]].copy()
    sin_url["motivo"] = "sin_url_o_partido_id"

    dnis_sin_url = set(sin_url["DNI"].tolist())

    # Unir ambos conjuntos (log + sin url), sin duplicar
    df_solo_log = df_log[~df_log["DNI"].astype(str).str.zfill(8).isin(dnis_sin_url)].copy()
    df_reintentar = pd.concat([sin_url, df_solo_log], ignore_index=True)
    df_reintentar["DNI"] = df_reintentar["DNI"].astype(str).str.zfill(8)

    print(f"  Sin url/partido_id en candidatos.csv : {len(sin_url)}")
    print(f"  En log de errores (adicionales)      : {len(df_solo_log)}")
    print(f"  Total a reintentar                   : {len(df_reintentar)}")

    if len(df_reintentar) == 0:
        print("  ✓ No hay candidatos que reintentar.")
        return

    df_log = df_reintentar  # usar este como fuente para el loop

    # ── Cargar datos actuales ──
    df_cand = pd.read_csv(CAND_CSV, dtype=str, encoding="utf-8-sig")
    df_cand["DNI"] = df_cand["DNI"].astype(str).str.zfill(8)

    # Cargar sentencias existentes para no perderlas
    if os.path.exists(SENT_CSV):
        df_sent_existentes = pd.read_csv(SENT_CSV, dtype=str, encoding="utf-8-sig")
    else:
        df_sent_existentes = pd.DataFrame(
            columns=["DNI","id_candidato","Sentencia_Materia","Sentencia_Fallo"]
        )

    nuevas_sentencias = []
    actualizaciones   = {}   # dni → {Sentencias, url, partido_id}
    log_reintento     = os.path.join(DATA_DIR, f"errores_reintento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

    with open(log_reintento, "w", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=[
            "DNI","id_candidato","nombre","partido","motivo","timestamp"
        ]).writeheader()

    total = len(df_log)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()

        for i, row in df_log.iterrows():
            dni          = str(row.get("DNI", "")).zfill(8)
            id_candidato = str(row.get("id_candidato", ""))
            nombre       = str(row.get("nombre", ""))
            partido_raw  = str(row.get("partido", ""))

            print(f"  [{i+1}/{total}] {nombre[:38]:<38}", end=" ", flush=True)

            # Buscar partido_id
            partido_id = mapa_partidos.get(partido_raw.upper())
            if not partido_id:
                for k, v in mapa_partidos.items():
                    if partido_raw.upper() in k or k in partido_raw.upper():
                        partido_id = v
                        break

            if not partido_id:
                print(f"→ partido aún no mapeado: {partido_raw[:40]}")
                continue

            url_ficha = f"{BASE_JNE}/hoja-vida/{partido_id}/{dni}"

            try:
                await asyncio.sleep(random.uniform(PAUSA_MIN, PAUSA_MAX))
                await goto_con_reintento(page, url_ficha)

                tiene, filas = await extraer_sentencias(page, dni, id_candidato)
                nuevas_sentencias.extend(filas)
                actualizaciones[dni] = {
                    "Sentencias": str(tiene),
                    "url":        url_ficha,
                    "partido_id": partido_id,
                }

                if tiene:
                    print(f"→ ✓ {len(filas)} sentencia(s)")
                else:
                    print("→ sin sentencias")

            except Exception as e:
                print(f"→ ERROR: {str(e)[:60]}")
                with open(log_reintento, "a", newline="", encoding="utf-8-sig") as f:
                    csv.DictWriter(f, fieldnames=[
                        "DNI","id_candidato","nombre","partido","motivo","timestamp"
                    ]).writerow({
                        "DNI": dni, "id_candidato": id_candidato,
                        "nombre": nombre, "partido": partido_raw,
                        "motivo": str(e)[:300],
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    })

        await browser.close()

    # ── Guardar ──
    print("\n" + "=" * 65)
    print("  GUARDADO")
    print("=" * 65)

    # Actualizar candidatos.csv
    for dni, vals in actualizaciones.items():
        mask = df_cand["DNI"] == str(dni).zfill(8)
        df_cand.loc[mask, "Sentencias"] = vals["Sentencias"]
        df_cand.loc[mask, "url"]        = vals["url"]
        df_cand.loc[mask, "partido_id"] = vals["partido_id"]

    df_cand.to_csv(CAND_CSV, index=False, encoding="utf-8-sig")
    print(f"  candidatos.csv  → {len(actualizaciones)} candidatos actualizados")

    # Añadir nuevas sentencias a las existentes (sin duplicar por DNI+id_candidato)
    if nuevas_sentencias:
        df_nuevas = pd.DataFrame(nuevas_sentencias)
        # Eliminar de existentes los DNIs que ahora tienen datos frescos
        dnis_actualizados = df_nuevas["DNI"].unique()
        df_sent_filtradas = df_sent_existentes[
            ~df_sent_existentes["DNI"].isin(dnis_actualizados)
        ]
        df_sent_final = pd.concat([df_sent_filtradas, df_nuevas], ignore_index=True)
    else:
        df_sent_final = df_sent_existentes

    df_sent_final.to_csv(SENT_CSV, index=False, encoding="utf-8-sig")
    print(f"  sentencias.csv  → {len(df_sent_final)} filas totales ({len(nuevas_sentencias)} nuevas)")

    con_sent = sum(1 for v in actualizaciones.values() if v["Sentencias"] == "1")
    print(f"\n  Con sentencias  : {con_sent} de {len(actualizaciones)} reintentados")
    print(f"  Log reintento   : {log_reintento}")


if __name__ == "__main__":
    asyncio.run(main())
