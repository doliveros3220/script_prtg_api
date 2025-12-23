import requests
import csv
import time
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===== CONFIGURACIÓN =====
PRTG_URL = "https://TU.URL.com/api/"
USERNAME = "tu_user"
PASSHASH = "tupasshash"  # Usa passhash, no contraseña directa
BATCH_SIZE = 40
MAX_RETRIES = 3
RETRY_DELAY = 8
OUTPUT_FILE = "canales_sensores.csv"

# ===== FUNCIONES =====
def get_data_with_retry(url, params):
    """Obtiene datos de la API con reintentos en caso de fallo."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, verify=False, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[ERROR] Intento {attempt} fallido: {e}")
            if attempt < MAX_RETRIES:
                print(f"Reintentando en {RETRY_DELAY} segundos...")
                time.sleep(RETRY_DELAY)
            else:
                print("Error crítico: No se pudo obtener datos tras varios intentos.")
                return None

def get_all_sensors():
    """Obtiene todos los sensores en lotes evitando duplicados."""
    print("Obteniendo sensores...")
    sensors = []
    seen_ids = set()
    start = 0

    while True:
        params = {
            "content": "sensors",
            "output": "json",
            "columns": "objid,device,sensor,group,host",
            "count": BATCH_SIZE,
            "start": start,
            "username": USERNAME,
            "passhash": PASSHASH
        }

        data = get_data_with_retry(PRTG_URL + "table.json", params)
        if not data or "sensors" not in data:
            print("No se obtuvieron más sensores o error en respuesta.")
            break

        batch = [s for s in data["sensors"] if s["objid"] not in seen_ids]
        if not batch:
            print("No hay sensores nuevos. Fin de la consulta.")
            break

        for s in batch:
            seen_ids.add(s["objid"])

        sensors.extend(batch)
        print(f"Lote obtenido: {len(batch)} sensores (Total acumulado: {len(sensors)})")

        if len(batch) < BATCH_SIZE:
            break

        start += BATCH_SIZE

    print(f"Total de sensores obtenidos: {len(sensors)}")
    return sensors

def get_channels_for_sensor(sensor_id):
    """Obtiene los canales de un sensor desde la API de PRTG."""
    url = f"{PRTG_URL}table.json"
    params = {
        "content": "channels",
        "id": sensor_id,
        "columns": "objid,name,lastvalue,unit",
        "username": USERNAME,
        "passhash": PASSHASH
    }

    data = get_data_with_retry(url, params)
    if not data or "channels" not in data:
        print(f"[WARN] Sensor {sensor_id} no devolvió canales válidos.")
        return []

    parsed_channels = []
    for ch in data["channels"]:
        parsed_channels.append({
            "Channel": ch.get("name"),
            "LastValue": ch.get("lastvalue"),
            "Unit": ch.get("unit", "")
        })

    return parsed_channels

def main():
    all_data = []
    sensors = get_all_sensors()

    print("\nObteniendo canales de cada sensor...")
    for i, sensor in enumerate(sensors, 1):
        sensor_id = sensor["objid"]
        channels = get_channels_for_sensor(sensor_id)

        for ch in channels:
            all_data.append({
                "Group": sensor["group"],
                "Device": sensor["device"],
                "Sensor": sensor["sensor"],
                "Host": sensor["host"],
                "SensorID": sensor_id,
                "Channel": ch.get("Channel"),
                "LastValue": ch.get("LastValue"),
                "Unit": ch.get("Unit")
            })

        if i % 20 == 0:
            print(f"Procesados {i} sensores...")

    print("\n=== Primeros 10 resultados ===")
    for row in all_data[:10]:
        print(row)

    print(f"\nExportando datos a {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["Group", "Device", "Sensor", "Host", "SensorID", "Channel", "LastValue", "Unit"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_data)

    print(f"Exportación completada. Total de registros: {len(all_data)}")

if __name__ == "__main__":
    main()
