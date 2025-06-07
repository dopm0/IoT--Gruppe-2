#!/usr/bin/env python3
import sys, time, struct, json, pandas as pd
from datetime import datetime, timezone
from bluepy import btle
import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# BLE-UUIDs  (TI SensorTag CC2650)
# ---------------------------------------------------------------------------
DEFAULT_MAC = "98:07:2D:27:F1:86"
RETRY_ON_DISCONNECT = 1

# HDC1000  – Temperature + Humidity
UUID_HUM_CONF = btle.UUID("f000aa22-0451-4000-b000-000000000000")
UUID_HUM_DATA = btle.UUID("f000aa21-0451-4000-b000-000000000000")

# OPT3001 – Illuminance (Luxometer)
UUID_LUX_CONF = btle.UUID("f000aa72-0451-4000-b000-000000000000")
UUID_LUX_DATA = btle.UUID("f000aa71-0451-4000-b000-000000000000")

# MPU9250 – Movement (hier nur Accelerometer)
UUID_MOV_CONF = btle.UUID("f000aa82-0451-4000-b000-000000000000")
UUID_MOV_DATA = btle.UUID("f000aa81-0451-4000-b000-000000000000")

# ---------------------------------------------------------------------------
# MQTT – Broker-Parameter
# ---------------------------------------------------------------------------
MQTT_HOST   = "iwilr2-5.campus.fh-ludwigshafen.de"
MQTT_PORT   = 1883
MQTT_ROOT   = "Factory/ColorSorter/ConditionMonitoring"
MQTT_RETAIN = True
MQTT_QOS    = 0

# ---------------------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z"


def read_all(mac: str) -> pd.DataFrame:
    """Einmalige Messung – reconnect bei kurzem Disconnect."""
    for attempt in range(RETRY_ON_DISCONNECT + 1):
        try:
            return _read_once(mac)
        except btle.BTLEDisconnectError:
            if attempt < RETRY_ON_DISCONNECT:
                print("Disconnect – versuche erneut …")
                time.sleep(1.0)
            else:
                raise


def _read_once(mac: str) -> pd.DataFrame:
    dev = btle.Peripheral(mac)
    rows, ts = [], now_iso()
    asset_id = f"TI-SensorTag-{mac.replace(':','')[-6:]}"
    loc_id   = "Labor_ColorSorter_Umgebung"

    try:
        # ---- Temperature & Humidity (HDC1000) -------------------------
        dev.getCharacteristics(uuid=UUID_HUM_CONF)[0].write(b"\x01", withResponse=True)
        time.sleep(1.8)
        raw_h = dev.getCharacteristics(uuid=UUID_HUM_DATA)[0].read()
        t_raw, rh_raw = struct.unpack("<HH", raw_h)
        temp_c = round((t_raw / 65536.0) * 165.0 - 40.0, 2)
        rh     = round(((rh_raw & ~0x0003) / 65536.0) * 100.0, 2)
        rows += [
            {"Sensor": "Temperature", "Value": temp_c, "Unit": "CEL", "DateTime": ts},
            {"Sensor": "Humidity",    "Value": rh,     "Unit": "P1",  "DateTime": ts},
        ]

        # ---- Illuminance (OPT3001) -----------------------------------
        dev.getCharacteristics(uuid=UUID_LUX_CONF)[0].write(b"\x01", withResponse=True)
        time.sleep(0.8)
        raw_l = dev.getCharacteristics(uuid=UUID_LUX_DATA)[0].read()
        raw16 = struct.unpack(">H", raw_l)[0]
        exp, mant = (raw16 >> 12) & 0xF, raw16 & 0x0FFF
        lux = round(mant * 0.01 * (2 ** exp), 2)
        rows.append({"Sensor": "Illuminance", "Value": lux, "Unit": "LUX", "DateTime": now_iso()})

    finally:
        dev.disconnect()

    df = pd.DataFrame(rows)
    df.insert(0, "AssetID", asset_id)
    df.insert(1, "LocationID", loc_id)
    return df


# ---------------------------------------------------------------------------
def publish_row(row: pd.Series, client: mqtt.Client):
    sensor = row["Sensor"]
    topic = f"{MQTT_ROOT}/{sensor}"
    payload = {
        "Observation": {
            "AssetID":         row["AssetID"],
            "SensorTypeCode":  sensor,
            "LocationID":      row["LocationID"],
            "MeasureContent":  row["Value"],
            "MeasureUnitCode": row["Unit"],
            "DateTime":        row["DateTime"],
        }
    }
    client.publish(topic, json.dumps(payload), qos=MQTT_QOS, retain=MQTT_RETAIN)


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mac = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MAC
    client = mqtt.Client(client_id="colorsorter-sensor-01")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    try:
        while True:
            df = read_all(mac)
            print("Daten erfolgreich Abgerufen")
            for _, row in df.iterrows():
                publish_row(row, client)
            print("Daten erfogreich in MQTT published")
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nBeende Messung")
    finally:
        client.loop_stop()
        client.disconnect()