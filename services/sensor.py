import json
import paho.mqtt.client as mqtt
import sys, time, struct, pandas as pd
from datetime import datetime, timezone
from bluepy import btle

# ---- Konfiguration ---------------------------------------------------
DEFAULT_MAC = "98:07:2D:27:F1:86"
RETRY_ON_DISCONNECT = 1           

UUID_HUM_CONF  = btle.UUID("f000aa22-0451-4000-b000-000000000000")
UUID_HUM_DATA  = btle.UUID("f000aa21-0451-4000-b000-000000000000")

UUID_BAR_CONF  = btle.UUID("f000aa42-0451-4000-b000-000000000000")
UUID_BAR_DATA  = btle.UUID("f000aa41-0451-4000-b000-000000000000")

UUID_LUX_CONF  = btle.UUID("f000aa72-0451-4000-b000-000000000000")
UUID_LUX_DATA  = btle.UUID("f000aa71-0451-4000-b000-000000000000")

UUID_BAT_LEVEL = btle.UUID("00002a19-0000-1000-8000-00805f9b34fb")

def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z"

def read_all(mac: str) -> pd.DataFrame:
    for attempt in range(RETRY_ON_DISCONNECT + 1):
        try:
            return _read_once(mac)
        except btle.BTLEDisconnectError as e:
            if attempt < RETRY_ON_DISCONNECT:
                print("🔄  Disconnect – versuche erneut …")
                time.sleep(1.0)
            else:
                raise e  # nach letztem Versuch durchreichen

def _read_once(mac: str) -> pd.DataFrame:
    dev = btle.Peripheral(mac)
    rows = []
    asset_id = f"TI-SensorTag-{mac.replace(':','')[-6:]}"
    loc_id   = "Labor_ColorSorter_Umgebung"

    try:
        # ---- HDC1000 --------------------------------------------------
        dev.getCharacteristics(uuid=UUID_HUM_CONF)[0].write(b"\x01", withResponse=True)
        time.sleep(1.8)
        raw_h = dev.getCharacteristics(uuid=UUID_HUM_DATA)[0].read()
        t_raw, rh_raw = struct.unpack("<HH", raw_h)
        temp_c = round((t_raw / 65536.0) * 165.0 - 40.0, 2)
        rh     = round(((rh_raw & ~0x0003) / 65536.0) * 100.0, 2)
        ts = now_iso()
        rows += [
            {"Sensor": "Temperature", "Value": temp_c, "Unit": "°C",  "DateTime": ts},
            {"Sensor": "Humidity",    "Value": rh,     "Unit": "%",   "DateTime": ts}
        ]

        # ---- BMP280 ---------------------------------------------------
        dev.getCharacteristics(uuid=UUID_BAR_CONF)[0].write(b"\x01", withResponse=True)
        time.sleep(2.0)
        raw_b = dev.getCharacteristics(uuid=UUID_BAR_DATA)[0].read()
        p_raw = raw_b[3] | (raw_b[4] << 8) | (raw_b[5] << 16)
        press = round(p_raw / 100.0, 2)
        rows.append({"Sensor": "Pressure", "Value": press, "Unit": "hPa", "DateTime": now_iso()})

        # ---- OPT3001 (Luxometer) --------------------------------------
        dev.getCharacteristics(uuid=UUID_LUX_CONF)[0].write(b"\x01", withResponse=True)
        time.sleep(0.8)
        raw_l = dev.getCharacteristics(uuid=UUID_LUX_DATA)[0].read()
        raw_lux = struct.unpack(">H", raw_l)[0]   # big-endian 16-bit
        exp = (raw_lux & 0xF000) >> 12
        mant = (raw_lux & 0x0FFF)
        lux = round((mant * (0.01 * (2 ** exp))), 2)
        rows.append({"Sensor": "Illuminance", "Value": lux, "Unit": "lx", "DateTime": now_iso()})

        # ---- Batterie -------------------------------------------------
        bat = int(dev.getCharacteristics(uuid=UUID_BAT_LEVEL)[0].read()[0])
        rows.append({"Sensor": "Battery", "Value": bat, "Unit": "%", "DateTime": now_iso()})

    finally:
        dev.disconnect()

    df = pd.DataFrame(rows)
    df.insert(0, "AssetID", asset_id)
    df.insert(1, "LocationID", loc_id)
    return df

# ---------------------------------------------------------------------------
# MQTT – Broker-Parameter anpassen
# ---------------------------------------------------------------------------
MQTT_HOST  = "iwilr2-5.campus.fh-ludwigshafen.de"
MQTT_PORT  = 1883
MQTT_ROOT  = "Factory/Team2"         
MQTT_RETAIN= True                     
MQTT_QOS   = 0

import json, paho.mqtt.client as mqtt

def publish_row(row, client: mqtt.Client):
    subtopic = row["Sensor"].replace(" ", "")
    topic = f"{MQTT_ROOT}/{subtopic}"

    payload = {
        "@context": f"http://iwilr4-9.campus.fh-ludwigshafen.de/iotsemantic/tinkerforge/{subtopic.lower()}.jsonld",
        "_UTC_timestamp": row["DateTime"],
        subtopic.lower(): row["Value"],
        "unit": row["Unit"],
        "asset": row["AssetID"],
        "location": row["LocationID"]
    }
    client.publish(topic,
                   json.dumps(payload),
                   qos=MQTT_QOS,
                   retain=MQTT_RETAIN)
    print(f"↗︎  {topic}  {payload}")

# ---------------------------------------------------------------------------
# Hauptaufruf
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mac = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MAC
    df  = read_all(mac)

    # 1) DataFrame anzeigen
    print(df.to_string(index=False))

    # 2) MQTT-Publizieren
    client = mqtt.Client(client_id="colorsorter-sensor-01")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

    for _, r in df.iterrows():
        publish_row(r, client)

    client.disconnect()
