import requests
import csv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURACIÓN ===
PRTG_SERVER = "https://TU.URL.com"   # Ejemplo: https://monitoreo.midominio.com
USERNAME = "tu_user"                  # Usuario PRTG
PASSHASH = "tupasshash"                   # Passhash del usuario
CSV_FILE = "dispositivos_prtg.csv"         # Archivo CSV de salida

# === API base ===
API_URL = f"{PRTG_SERVER}/api/table.json"

# === Configuración de columnas ===
columns = "objid,probe,group,device,host,status,message,active,sensorcount,downsens"

def obtener_todos_dispositivos():
    todos = []
    start = 0
    step = 500
    ids_vistos = set()  # Evita duplicados

    while True:
        params = {
            "content": "devices",
            "columns": columns,
            "username": USERNAME,
            "passhash": PASSHASH,
            "count": step,
            "start": start
        }

        print(f"Consultando dispositivos desde {start} hasta {start + step}...")
        response = requests.get(API_URL, params=params, verify=False)
        response.raise_for_status()
        data = response.json()

        devices = data.get("devices", [])
        if not devices:
            print("No hay más dispositivos para obtener.")
            break

        # Filtramos los ya vistos (por si el API devuelve repetidos)
        nuevos = [d for d in devices if d.get("objid") not in ids_vistos]

        if not nuevos:
            print("No se encontraron dispositivos nuevos, finalizando.")
            break

        for d in nuevos:
            ids_vistos.add(d.get("objid"))
            todos.append(d)

        print(f"  → {len(nuevos)} nuevos agregados. Total acumulado: {len(todos)}")

        # Si devuelve menos de 500, asumimos que fue la última página
        if len(devices) < step:
            print("Última página recibida.")
            break

        start += step

    return todos


def exportar_csv(devices):
    with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Probe", "Grupo", "Dispositivo", "IP / Host", "Estado", "Sensores Totales", "Sensores en Down"])

        for d in devices:
            writer.writerow([
                d.get("objid", ""),
                d.get("probe", ""),
                d.get("group", ""),
                d.get("device", ""),
                d.get("host", ""),
                d.get("status", ""),
                d.get("sensorcount", ""),
                d.get("downsens", ""),
                d.get("message", "")
            ])

    print(f"\nExportación completada: {CSV_FILE}")


if __name__ == "__main__":
    try:
        print("Conectando con PRTG...")
        dispositivos = obtener_todos_dispositivos()
        print(f"\nTotal de dispositivos obtenidos: {len(dispositivos)}")
        exportar_csv(dispositivos)
    except Exception as e:
        print(f"Error: {e}")
