#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import re
from urllib3.exceptions import InsecureRequestWarning
from datetime import datetime
from openpyxl import Workbook

# ==========================
# CONFIGURACIÓN API PRTG
# ==========================
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

PRTG_URL = "https://TU.URL.com/api/"
USERNAME = "tu_user"
PASSHASH = "tupasshash"

OUTPUT_XLSX = "informe_disponibilidad.xlsx"

REQUEST_DELAY = 1.0
GET_MAX_RETRIES = 3
GET_RETRY_DELAY = 5


# ==========================
# CONVERTIR HORAS A FORMATO
# ==========================
def formatear_tiempo_en_horas(horas):
    try:
        horas = int(horas)
    except:
        return ""

    dias = horas // 24
    horas_rest = horas % 24
    minutos = (horas * 60) % 60

    return f"{dias} días, {horas_rest} horas, {minutos} minutos"


# ==========================
# REQUEST CON REINTENTOS
# ==========================
def get_data_with_retry(url, params=None, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout, verify=False)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f" Error al consultar {url} (intento {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                time.sleep(GET_RETRY_DELAY)
    return None


# ==========================
# OBTENER SENSORES DEL GRUPO
# ==========================
def get_sensors_by_group(group_id):
    print(f"\nBuscando sensores Ping en el grupo {group_id}...")
    url = f"{PRTG_URL}table.json"
    params = {
        "content": "sensors",
        "columns": "objid,group,device,sensor,status,message,host",
        "id": group_id,
        "username": USERNAME,
        "passhash": PASSHASH
    }

    r = get_data_with_retry(url, params=params, max_retries=GET_MAX_RETRIES, timeout=20)
    if not r:
        print(f"No se pudo obtener información del grupo {group_id}")
        return []

    data = r.json()
    sensores = data.get("sensors", [])

    sensores_ping = [s for s in sensores if "ping" in s["sensor"].lower()]
    print(f"{len(sensores_ping)} sensores Ping encontrados en grupo {group_id}")
    return sensores_ping


# ==========================
# HISTÓRICO + CÁLCULO UPTIME
# ==========================
def get_historic_data(sensor_id, start_date, end_date):

    if "-" in start_date and len(start_date.split("-")) > 3:
        sdate_fmt = start_date
        edate_fmt = end_date
    else:
        sdate_fmt = start_date.replace("/", "-") + "-00-00-00"
        edate_fmt = end_date.replace("/", "-") + "-23-59-59"

    url = f"{PRTG_URL}historicdata.json"
    params = {
        "id": sensor_id,
        "avg": 3600,
        "sdate": sdate_fmt,
        "edate": edate_fmt,
        "username": USERNAME,
        "passhash": PASSHASH
    }

    print(f"\nConsultando históricos de sensor {sensor_id}")
    print(f"        Rango: {sdate_fmt} → {edate_fmt}")

    response = get_data_with_retry(url, params=params, max_retries=GET_MAX_RETRIES, timeout=60)
    if not response:
        print("Sin respuesta del servidor.")
        return None, {}, None

    data = response.json()
    hist = data.get("histdata", [])
    if not hist:
        print("Sensor sin datos en ese rango.")
        return None, {}, None

    json_text = response.text

    uptime_values = []
    latency_values = []

    muestras_totales = len(hist)
    muestras_validas = 0
    muestras_up = 0
    muestras_down = 0
    muestras_omitidas = 0

    for i, point in enumerate(hist):

        if point.get("coverage_raw", 0) < 10000:
            muestras_omitidas += 1
            continue

        dt = point.get("datetime")
        if not dt:
            muestras_omitidas += 1
            continue

        start_idx = json_text.find(f'"{dt}"')
        if start_idx == -1:
            muestras_omitidas += 1
            continue

        if i < len(hist) - 1:
            next_dt = hist[i+1].get("datetime")
            end_idx = json_text.find(f'"{next_dt}"', start_idx)
            if end_idx == -1:
                end_idx = len(json_text)
        else:
            end_idx = len(json_text)

        record_text = json_text[start_idx:end_idx]

        value_raw_matches = re.findall(r'"value_raw"\s*:\s*(".*?"|[0-9]+\.?[0-9]*)', record_text)

        latencia_raw = value_raw_matches[:4]

        tiene_latency = False
        latency_ms = None

        for tok in latencia_raw:
            tok_clean = tok.strip().strip('"')
            if tok_clean == "":
                continue
            try:
                val = float(tok_clean)
                if val >= 0 and val < 50000:
                    tiene_latency = True
                    latency_ms = val if latency_ms is None else latency_ms
                    break
            except:
                continue

        if tiene_latency:
            uptime_values.append(100)
            muestras_up += 1
            muestras_validas += 1
            if latency_ms is not None:
                latency_values.append(latency_ms)
        else:
            uptime_values.append(0)
            muestras_down += 1
            muestras_validas += 1

    estadisticas = {
        "muestras_totales": muestras_totales,
        "muestras_validas": muestras_validas,
        "muestras_up": muestras_up,
        "muestras_down": muestras_down,
        "muestras_omitidas": muestras_omitidas
    }

    disponibilidad = round(sum(uptime_values) / len(uptime_values), 2) if uptime_values else None

    promedio_ms = round(sum(latency_values) / len(latency_values), 2) if latency_values else None

    return disponibilidad, estadisticas, promedio_ms


# ==========================
# MAIN
# ==========================
def main():

    print("\n=== DISPONIBILIDAD PRTG — Exportación a Excel ===\n")

    grupos_input = input("Ingrese los ID de los grupos separados por coma: ")
    grupos = [g.strip() for g in grupos_input.split(",") if g.strip().isdigit()]
    if not grupos:
        print("No ingresó grupos válidos.")
        return

    start_date = input("Fecha inicio: ").strip()
    end_date = input("Fecha fin: ").strip()

    print("\nConsultando sensores...\n")
    todos_sensores = []
    for gid in grupos:
        todos_sensores.extend(get_sensors_by_group(gid))

    print(f"\nTotal sensores Ping: {len(todos_sensores)}\n")

    resultados = []

    for idx, s in enumerate(todos_sensores, start=1):

        sid = s.get("objid")
        estado = s.get("status")

        print(f"\n[{idx}/{len(todos_sensores)}] {s.get('device')} / {s.get('sensor')} — Estado: {estado}")

        disponibilidad, stats, promedio = get_historic_data(sid, start_date, end_date)

        promedio_formateado = f"{promedio} ms" if promedio is not None else ""

        tiempo_legible = formatear_tiempo_en_horas(stats.get("muestras_up", 0))

        resultados.append({
            "Negocio": s.get("group"),
            "Dispositivo": s.get("device"),
            "Sensor": s.get("sensor"),
            "Promedio": promedio_formateado,
            #"SensorID": sid,
            #"Estado Actual": estado,
            "Tiempo de disponibilidad": disponibilidad,
            "Tiempo": tiempo_legible,
            #"Horas Down": stats.get("muestras_down", 0),
            #"Horas Omitidas": stats.get("muestras_omitidas", 0),
            #"Total Horas": stats.get("muestras_totales", 0),
            #"Fecha Inicio": start_date,
            #"Fecha Fin": end_date
        })

        time.sleep(REQUEST_DELAY)

    # ==========================
    # EXPORTAR A EXCEL
    # ==========================
    wb = Workbook()
    ws = wb.active
    ws.title = "Informe_Disponibilidad"

    headers = list(resultados[0].keys())
    ws.append(headers)

    for fila in resultados:
        ws.append(list(fila.values()))

    wb.save(OUTPUT_XLSX)

    print(f"\nArchivo Excel generado: {OUTPUT_XLSX}")
    print("\n=== PROCESO FINALIZADO ===")


if __name__ == "__main__":
    main()
