import paho.mqtt.client as mqtt
import os
import threading
import time
import pymysql
from datetime import datetime
import socket
import random

# 🔹 ข้อมูล MySQL Server
db_config = {
    'host': 'sql12.freesqldatabase.com',
    'user': 'sql12774523',
    'password': 'pQQXJPx74e',
    'database': 'sql12774523'
}

# 🔹 ตัวแปรข้อมูลเซ็นเซอร์
sensor_data = {
    "Temp": None,
    "Hum": None,
    "PM2_5": None,
    "PM10": None,
    "Ozone": None,
    "Carbon": None,
    "Nitro": None,
    "Sulfur": None,
    "people_no": None
}
csv_lock = threading.Lock()

# 🔹 บันทึกลง MySQL Database
def save_to_data(data):
    connection = None
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        sql = """
        INSERT INTO sensor_data (
            timestamp, temp, hum, pm2_5, pm10, ozone, carbon, nitro, sulfur, people_no
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        timestamp = datetime.now()

        # 🔥 แปลง None เป็น SQL NULL ถูกต้อง
        values = (
            timestamp,
            data.get("Temp"),
            data.get("Hum"),
            data.get("PM2_5"),
            data.get("PM10"),
            data.get("Ozone"),
            data.get("Carbon"),
            data.get("Nitro"),
            data.get("Sulfur"),
            data.get("people_no")
        )

        cursor.execute(sql, values)
        connection.commit()
        print(f"✅  บันทึกสำเร็จ (people = {data.get('people_no')})")

    except Exception as e:
        print("❌  บันทกล้มเหลว:", e)

    finally:
        if connection:
            cursor.close()
            connection.close()

# 🔹 Thread บันทึกข้อมูลทุก 10 วินาที
def periodic_save():
    supabase_timer = 0
    while True:
        time.sleep(10)

        people_no = random.randint(20, 60)
        sensor_data["people_no"] = people_no

        with csv_lock:
            supabase_timer += 1
            if supabase_timer >= 6:  # ทุก 60 วินาที
                save_to_data(sensor_data)
                supabase_timer = 0

            # ♻️ Reset ค่าทุกอย่าง
            for key in sensor_data:
                sensor_data[key] = None

# 🔹 รับ MQTT Message
def on_message(client, userdata, msg):
    topic = msg.topic
    try:
        value = float(msg.payload.decode("utf-8"))
    except:
        return

    with csv_lock:
        for key in sensor_data:
            if key in topic:
                sensor_data[key] = value
                break

# 🔹 เชื่อมต่อ MQTT Server
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ เชื่อมต่อ MQTT สำเร็จ")
        client.subscribe("IQA_Test/#")
    else:
        print("❌ MQTT เชื่อมไม่สำเร็จ:", rc)

# 🚀 เริ่มโปรแกรม
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect("broker.emqx.io")

threading.Thread(target=periodic_save, daemon=True).start()
mqtt_client.loop_forever()
