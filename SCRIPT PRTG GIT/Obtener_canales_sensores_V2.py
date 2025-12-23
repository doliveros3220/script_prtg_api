import requests
import csv
import time
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============== CONFIGURACIÓN =================
PRTG_URL = "https://TU.URL.com/api/"
USERNAME = "tu_user"
PASSHASH = "tupasshash"
OUTPUT_FILE = "canales_por_dispositivo.csv"
MAX_RETRIES = 3
RETRY_DELAY = 5

# =============== FUNCIONES =================
def get_data_with_retry(params):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(
                PRTG_URL + "table.json",
                params=params,
                verify=False,
                timeout=30
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"[ERROR] Intento {attempt}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                return None

def get_sensors_by_device(device_id):
    print(f"\nObteniendo sensores del Device ID {device_id} ...")

    sensors = []
    seen_ids = set()
    start = 0
    PAGE_SIZE = 500  # Límite real de PRTG

    while True:
        params = {
            "content": "sensors",
            "output": "json",
            "columns": "objid,group,device,sensor,host",
            "filter_parentid": device_id,
            "count": PAGE_SIZE,
            "start": start,
            "username": USERNAME,
            "passhash": PASSHASH
        }

        data = get_data_with_retry(params)
        if not data or "sensors" not in data:
            break

        batch = data["sensors"]

        new_items = 0
        for s in batch:
            if s["objid"] not in seen_ids:
                seen_ids.add(s["objid"])
                sensors.append(s)
                new_items += 1

        print(f"   ↳ Lote desde {start}: {new_items} sensores nuevos")

        # 🔑 condición de salida REAL
        if new_items == 0:
            break

        start += PAGE_SIZE

    print(f"Total sensores obtenidos: {len(sensors)}")
    return sensors


def get_channels(sensor_id):
    params = {
        "content": "channels",
        "output": "json",
        "id": sensor_id,
        "columns": "name,lastvalue,unit",
        "username": USERNAME,
        "passhash": PASSHASH
    }

    data = get_data_with_retry(params)
    if not data or "channels" not in data:
        return []

    return [
        {
            "Channel": ch.get("name"),
            "LastValue": ch.get("lastvalue"),
            "Unit": ch.get("unit", "")
        }
        for ch in data["channels"]
    ]

# =============== MAIN =================
def main():
    device_id = input("Ingresa el Device ID de PRTG: ").strip()

    if not device_id.isdigit():
        print("Device ID inválido")
        return

    sensors = get_sensors_by_device(device_id)
    if not sensors:
        print("No se encontraron sensores")
        return

    all_rows = []

    print("\nObteniendo canales...")
    for i, sensor in enumerate(sensors, 1):
        channels = get_channels(sensor["objid"])

        for ch in channels:
            all_rows.append({
                "Group": sensor["group"],
                "Device": sensor["device"],
                "Sensor": sensor["sensor"],
                "Host": sensor["host"],
                "SensorID": sensor["objid"],
                "Channel": ch["Channel"],
                "LastValue": ch["LastValue"],
                "Unit": ch["Unit"]
            })

        print(f"{i}/{len(sensors)} sensores procesados")

    print(f"\nExportando a {OUTPUT_FILE} ...")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Group", "Device", "Sensor", "Host", "SensorID", "Channel", "LastValue", "Unit"]
        )
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Exportación completada | Registros: {len(all_rows)}")

if __name__ == "__main__":
    main()

