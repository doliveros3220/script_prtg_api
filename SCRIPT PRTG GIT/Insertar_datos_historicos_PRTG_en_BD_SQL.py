import requests
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import csv
import time
import re
import pyodbc
from urllib3.exceptions import InsecureRequestWarning
from datetime import datetime

# ==========================
# Configuracion y conexion con api de PRTG
# ==========================
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

PRTG_URL = "https://TU.URL.com/api/"
USERNAME = "tu_user"
PASSHASH = "tupasshash"

OUTPUT_FILE = "disponibilidad_ping.csv"

SQL_SERVER = r"tuservidor\SQLEXPRESS"
SQL_DATABASE = "tu_base_de_datos"
SQL_USERNAME = "tu_usuario_de_SQL"
SQL_PASSWORD = "tu_contraseña_de_SQL"
USE_WINDOWS_AUTH = False

REQUEST_DELAY = 1.0
GET_MAX_RETRIES = 3
GET_RETRY_DELAY = 5


# ==========================
# Reintentos de conexion en caso de lentitud en la red
# ==========================
def get_data_with_retry(url, params=None, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout, verify=False)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"    ⚠ Error al consultar {url} (intento {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                time.sleep(GET_RETRY_DELAY)
    return None


# ==========================
# PRTG: sensores y históricos
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
    sensors = data.get("sensors", [])

    sensores_ping = [s for s in sensors if "sensor" in s and "ping" in s["sensor"].lower()]
    print(f"{len(sensores_ping)} sensores Ping encontrados en grupo {group_id}")
    return sensores_ping


def get_historic_data(sensor_id, start_date, end_date):

    try:
        if "-" in start_date and len(start_date.split("-")) > 3:
            sdate_fmt = start_date
            edate_fmt = end_date
        else:
            sdate_fmt = start_date.replace("/", "-") + "-00-00-00"
            edate_fmt = end_date.replace("/", "-") + "-23-59-59"
    except:
        print("Formato de fecha inválido")
        return None, {}

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

    response = None
    for attempt in range(1, GET_MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=60, verify=False)
            resp.raise_for_status()
            response = resp
            break
        except Exception as e:
            print(f"Error al consultar (intento {attempt}/{GET_MAX_RETRIES}): {e}")
            if attempt < GET_MAX_RETRIES:
                time.sleep(GET_RETRY_DELAY)

    if not response:
        print("    ⚠ Sin respuesta del servidor.")
        return None, {}

    data = response.json()
    hist = data.get("histdata", [])
    if not hist:
        print("    ⚠ Sensor sin datos históricos en ese rango.")
        return None, {}

    json_text = response.text

    uptime_values = []
    muestras_validas = 0
    muestras_omitidas = 0
    muestras_up = 0
    muestras_down = 0
    muestras_totales = len(hist)

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
            next_dt = hist[i+1].get("datetime", "")
            end_idx = json_text.find(f'"{next_dt}"', start_idx)
            if end_idx == -1:
                end_idx = len(json_text)
        else:
            end_idx = len(json_text)

        record_text = json_text[start_idx:end_idx]

        value_raw_matches = re.findall(r'"value_raw"\s*:\s*(".*?"|[0-9]+\.?[0-9]*)', record_text)

        latencia_raw = value_raw_matches[:4]

        tiene_latency = False
        for tok in latencia_raw:
            tok_clean = tok.strip().strip('"')
            if tok_clean == "":
                continue
            try:
                float(tok_clean)
                tiene_latency = True
                break
            except:
                continue

        if tiene_latency:
            uptime_values.append(100)
            muestras_up += 1
            muestras_validas += 1
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

    if uptime_values:
        avg = sum(uptime_values) / len(uptime_values)
        return round(avg, 2), estadisticas
    else:
        return None, estadisticas


# ==========================
# Gestion de la BD SQL
# ==========================
def conectar_sql():
    print("\nIntentando conectar a SQL Server...")
    try:
        if USE_WINDOWS_AUTH:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={SQL_SERVER};"
                f"DATABASE={SQL_DATABASE};"
                f"Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={SQL_SERVER};"
                f"DATABASE={SQL_DATABASE};"
                f"UID={SQL_USERNAME};"
                f"PWD={SQL_PASSWORD};"
            )
        conn = pyodbc.connect(conn_str, autocommit=False, timeout=10)
        print("Conexión SQL OK")
        return conn
    except Exception as e:
        print(f"Error conectando a SQL Server: {e}")
        return None


def crear_tabla_si_no_existe(conn):
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Disponibilidad_PRTG' AND xtype='U')
        BEGIN
            CREATE TABLE Disponibilidad_PRTG (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                Grupo NVARCHAR(200),
                Dispositivo NVARCHAR(255),
                Sensor NVARCHAR(255),
                SensorID INT NULL,
                Disponibilidad DECIMAL(5,2) NULL,
                Horas_Up INT,
                Horas_Down INT,
                Horas_Omitidas INT,
                Total_Horas INT,
                Fecha_Inicio NVARCHAR(50),
                Fecha_Fin NVARCHAR(50),
                FechaRegistro DATETIME DEFAULT GETDATE(),
                CONSTRAINT UQ_SensorID_Period UNIQUE (SensorID, Fecha_Inicio, Fecha_Fin)
            )
        END
    """)
    conn.commit()
    print("✔ Tabla SQL verificada / creada")


# ==========================
# VALIDACIÓN DE RANGOS EN BD
# ==========================
def existe_rango_en_bd(conn, sensor_id, fecha_inicio, fecha_fin):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM Disponibilidad_PRTG
        WHERE SensorID = ?
          AND (
                Fecha_Inicio <= ?
            AND Fecha_Fin >= ?
          )
    """, (sensor_id, fecha_fin, fecha_inicio))

    count = cursor.fetchone()[0]
    return count > 0


# ==========================
# Insertar datos
# ==========================
def insertar_resumen(conn, fila):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO Disponibilidad_PRTG
            (Grupo, Dispositivo, Sensor, SensorID, Disponibilidad, Horas_Up, Horas_Down,
             Horas_Omitidas, Total_Horas, Fecha_Inicio, Fecha_Fin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fila.get("Grupo"),
            fila.get("Dispositivo"),
            fila.get("Sensor"),
            fila.get("SensorID"),
            fila.get("Disponibilidad"),
            fila.get("Horas Up", 0),
            fila.get("Horas Down", 0),
            fila.get("Horas Omitidas (Warning/Paused/Unknown)", 0),
            fila.get("Total Horas", 0),
            fila.get("Fecha Inicio"),
            fila.get("Fecha Fin")
        ))
        conn.commit()
        return "insertado"
    except pyodbc.IntegrityError:
        return "duplicado"
    except Exception as e:
        print(f"Error SQL al insertar: {e}")
        return "error"


# ==========================
# MAIN
# ==========================
def main():

    print("\n=== DISPONIBILIDAD PRTG — Basado SOLO en latencia (value_raw) ===\n")

    grupos_input = input("Ingrese los ID de los grupos separados por coma: ")
    grupos = [g.strip() for g in grupos_input.split(",") if g.strip().isdigit()]
    if not grupos:
        print("No ingresó grupos válidos.")
        return

    start_date = input("Fecha inicio: ").strip()
    end_date = input("Fecha fin: ").strip()

    test_url = f"{PRTG_URL}table.json"
    params = {"content": "sensors", "columns": "objid", "id": grupos[0], "username": USERNAME, "passhash": PASSHASH}
    if not get_data_with_retry(test_url, params=params, max_retries=2, timeout=10):
        print("Error conectando a PRTG.")
        return
    print("✔ Conexión a PRTG verificada")

    conn = conectar_sql()
    if not conn:
        print("No se pudo conectar SQL.")
        return

    crear_tabla_si_no_existe(conn)

    print("\nConsultando sensores Ping...\n")
    todos_sensores = []
    for gid in grupos:
        todos_sensores.extend(get_sensors_by_group(gid))

    print(f"✔ Total sensores Ping: {len(todos_sensores)}\n")

    resultados = []
    total_insertados = 0
    total_duplicados = 0

    for idx, s in enumerate(todos_sensores, start=1):
        sid = s.get("objid")
        estado = s.get("status")
        print(f"\n[{idx}/{len(todos_sensores)}] Sensor ID {sid} — {s.get('device')} / {s.get('sensor')} — Estado actual: {estado}")

        # ===============================================================
        # VALIDAR SI EL RANGO YA EXISTE EN LA BD (EVITA DATOS DUPLICADOS)
        # ===============================================================
        if existe_rango_en_bd(conn, sid, start_date, end_date):
            print("⚠ Omitido: Ya existe información de este sensor en un rango de fechas que se cruza")
            total_duplicados += 1
            continue

        disponibilidad, stats = get_historic_data(sid, start_date, end_date)

        print(f"    Disponibilidad calculada: {disponibilidad}%")
        print(f"    Horas UP: {stats.get('muestras_up')} | DOWN: {stats.get('muestras_down')} | Omitidas: {stats.get('muestras_omitidas')} | Total: {stats.get('muestras_totales')}")

        fila = {
            "Grupo": s.get("group"),
            "Dispositivo": s.get("device"),
            "Sensor": s.get("sensor"),
            "SensorID": sid,
            "Disponibilidad": disponibilidad,
            "Horas Up": stats.get("muestras_up", 0),
            "Horas Down": stats.get("muestras_down", 0),
            "Horas Omitidas (Warning/Paused/Unknown)": stats.get("muestras_omitidas", 0),
            "Total Horas": stats.get("muestras_totales", 0),
            "Fecha Inicio": start_date,
            "Fecha Fin": end_date
        }

        resultado = insertar_resumen(conn, fila)

        if resultado == "insertado":
            print("Insertado en SQL")
            total_insertados += 1
        elif resultado == "duplicado":
            print("Omitido (registro duplicado exacto)")
            total_duplicados += 1
        else:
            print("Error insertando en SQL")

        resultados.append(fila)

        time.sleep(REQUEST_DELAY)

    try:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            fieldnames = list(resultados[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(resultados)

        print(f"\n✔ CSV generado: {OUTPUT_FILE}")
    except:
        print(f"\nNo se pudo generar el CSV")

    print(f"\n=== PROCESO COMPLETADO ===")
    print(f"Insertados: {total_insertados}")
    print(f"Duplicados: {total_duplicados}")

    conn.close()


if __name__ == "__main__":
    main()
