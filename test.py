#!/usr/bin/env python3
import sys, time, struct, json
from datetime import datetime, timezone
import pandas as pd
from bluepy import btle
import paho.mqtt.client as mqtt

# -----------------------------------------------------------------------------
# Konfiguration
DEFAULT_MAC            = "98:07:2D:27:F1:86"
LOCATION_ID            = "Labor_ColorSorter_Umgebung"
MEASUREMENT_INTERVAL   = 60  # Sekunden

MQTT_BROKER, MQTT_PORT = "iwilr2-5.campus.fh-ludwigshafen.de", 1883
MQTT_ROOT              = "Factory/ColorSorter/ConditionMonitoring"
MQTT_QOS, MQTT_RETAIN  = 0, True

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z"

# -----------------------------------------------------------------------------
class Sensor:
    registry = []

    def __init__(self, sensor_type, uuid_cfg, uuid_dat, delay, unit, decode_fn):
        self.sensor_type = sensor_type
        self.uuid_cfg     = btle.UUID(uuid_cfg)
        self.uuid_dat     = btle.UUID(uuid_dat)
        self.delay        = delay
        self.unit         = unit
        self.decode_fn    = decode_fn
        Sensor.registry.append(self)

    def read(self, dev, ts):
        dev.getCharacteristics(uuid=self.uuid_cfg)[0].write(b"\x01", withResponse=True)
        time.sleep(self.delay)
        raw = dev.getCharacteristics(uuid=self.uuid_dat)[0].read()
        value = self.decode_fn(raw)
        return {
            "Sensor":   self.sensor_type,
            "Value":    value,
            "Unit":     self.unit,
            "DateTime": ts
        }

# -----------------------------------------------------------------------------
# Sensor-Definitionen
Sensor(
    "Temperature",
    "f000aa22-0451-4000-b000-000000000000",
    "f000aa21-0451-4000-b000-000000000000",
    1.8, "CEL",
    lambda raw: round((struct.unpack("<HH", raw)[0] / 65536.0) * 165.0 - 40.0, 2)
)
Sensor(
    "Humidity",
    "f000aa22-0451-4000-b000-000000000000",
    "f000aa21-0451-4000-b000-000000000000",
    1.8, "P1",
    lambda raw: round(((struct.unpack("<HH", raw)[1] & ~0x03) / 65536.0) * 100.0, 2)
)
Sensor(
    "Illuminance",
    "f000aa72-0451-4000-b000-000000000000",
    "f000aa71-0451-4000-b000-000000000000",
    0.8, "LUX",
    lambda raw: round(
        ((r := struct.unpack(">H", raw)[0]) & 0x0FFF) * 0.01 * (2 ** ((r >> 12) & 0xF)), 2
    )
)

# -----------------------------------------------------------------------------
def read_all_sensors(mac: str) -> pd.DataFrame:
    dev = btle.Peripheral(mac)
    ts = now_iso()
    assetid = f"TI-SensorTag-{mac.replace(':','')[-6:]}"
    rows = []
    try:
        for sensor in Sensor.registry:
            meas = sensor.read(dev, ts)
            meas["AssetID"]    = assetid
            meas["LocationID"] = LOCATION_ID
            rows.append(meas)
    finally:
        dev.disconnect()
    return pd.DataFrame(rows)

# -----------------------------------------------------------------------------
def publish_row(row: dict, client: mqtt.Client):
    payload = {
        "Observation": {
            "AssetID":         row["AssetID"],
            "SensorTypeCode":  row["Sensor"],
            "LocationID":      row["LocationID"],
            "MeasureContent":  row["Value"],
            "MeasureUnitCode": row["Unit"],
            "DateTime":        row["DateTime"],
        }
    }
    topic = f"{MQTT_ROOT}/{row['Sensor']}"
    client.publish(topic, json.dumps(payload), qos=MQTT_QOS, retain=MQTT_RETAIN)

# -----------------------------------------------------------------------------
def main():
    client = mqtt.Client()
    client.connect(MQTT_BROKER, MQTT_PORT)
    client.loop_start()
    try:
        while True:
            try:
                df = read_all_sensors(DEFAULT_MAC)
            except btle.BTLEDisconnectError:
                print("Verbindung unterbrochen ...")
                sys.exit(1)

            print(df.to_string(index=False))
            for record in df.to_dict(orient="records"):
                publish_row(record, client)

            print("Messwerte erfolgreich veröffentlicht.\n")
            time.sleep(MEASUREMENT_INTERVAL)
    except KeyboardInterrupt:
        print("\nMessung beendet.")
    finally:
        client.loop_stop()
        client.disconnect()