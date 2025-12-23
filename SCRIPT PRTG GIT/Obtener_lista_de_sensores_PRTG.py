import requests
import csv
import time
import sys

# ==============================
# CONFIGURACIÓN DEL USUARIO
# ==============================
PRTG_URL = "https://TU.URL.com/api/table.json"  # Cambia por tu URL
USERNAME = "tu_user"
PASSHASH = "tupasshash"
BLOCK_SIZE = 400
OUTPUT_FILE = "sensores_prtg.csv"
# ==============================


def obtener_sensores(offset):
    """
    Obtiene un bloque de sensores desde la API de PRTG.
    Retorna una lista de sensores o None si hay error.
    """
    params = {
        "content": "sensors",
        "columns": "objid,group,device,sensor,status,message,lastvalue,priority,uptime",
        "count": BLOCK_SIZE,
        "start": offset,
        "username": USERNAME,
        "passhash": PASSHASH
    }

    for intento in range(3):  # Hasta 3 intentos
        try:
            print(f"   → Intento {intento + 1} consultando offset {offset}...")
            response = requests.get(PRTG_URL, params=params, timeout=30, verify=False)
            response.raise_for_status()

            data = response.json()
            if "sensors" in data:
                return data["sensors"]
            else:
                print("  Respuesta sin campo 'sensors'.")
                return []
        except Exception as e:
            print(f"Error en intento {intento + 1}: {e}")
            time.sleep(8)
    return None


def exportar_csv(sensores):
    """Exporta la lista de sensores a un archivo CSV."""
    if not sensores:
        print("No hay sensores para exportar.")
        return

    keys = sensores[0].keys()
    with open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(sensores)

    print(f"\nExportación completada: {OUTPUT_FILE}")
    print(f"Total de sensores exportados: {len(sensores)}")


def main():
    print("Iniciando extracción de sensores desde PRTG...\n")
    todos_los_sensores = []
    offset = 0
    ultimo_bloque_ids = set()

    while True:
        print(f"Obteniendo sensores desde offset {offset}...")
        sensores = obtener_sensores(offset)
        if sensores is None:
            print("Error persistente tras 3 intentos. Abortando.")
            break

        cantidad = len(sensores)
        print(f"{cantidad} sensores obtenidos en este bloque.\n")

        # --- Condiciones de parada ---
        if cantidad == 0:
            print("No se encontraron más sensores. Finalizando extracción.")
            break

        # Si el bloque actual tiene los mismos IDs que el anterior (repetido)
        ids_actuales = {s['objid'] for s in sensores}
        if ids_actuales == ultimo_bloque_ids:
            print("Se detectó repetición de datos. Finalizando extracción.")
            break

        todos_los_sensores.extend(sensores)
        ultimo_bloque_ids = ids_actuales

        # Si el bloque tiene menos de BLOCK_SIZE, probablemente es el último
        if cantidad < BLOCK_SIZE:
            print("Último bloque recibido (menos de 400 sensores).")
            break

        offset += BLOCK_SIZE
        time.sleep(1)

    exportar_csv(todos_los_sensores)
    print("\nProceso completado.")


if __name__ == "__main__":
    requests.packages.urllib3.disable_warnings()
    try:
        main()
    except KeyboardInterrupt:
        print("\nEjecución interrumpida por el usuario.")
        sys.exit(0)
    except Exception as e:
        print(f"\nError crítico: {e}")
