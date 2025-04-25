import paho.mqtt.client as mqtt
# import csv
import os
import threading
import time
# import psycopg2
from datetime import datetime
import pymysql
import socket
import random 

# # ✅ บังคับให้ใช้ IPv4 แก้ปัญหา IPv6 DNS
# if not hasattr(socket, "_original_getaddrinfo"):
#     socket._original_getaddrinfo = socket.getaddrinfo
# socket.getaddrinfo = lambda *args, **kwargs: [
#     ai for ai in socket._original_getaddrinfo(*args, **kwargs)
#     if ai[0] == socket.AF_INET
# ]

# 🔹 ข้อมูล Supabase

db_config = {
    'host': 'sql12.freesqldatabase.com',
    'user': 'sql12774523',
    'password': 'pQQXJPx74e',
    'database': 'sql12774523'
}

# 🔹 ตั้งค่า CSV
# csv_filename = "sensor_data.csv"
# if not os.path.exists(csv_filename):
#     with open(csv_filename, mode="w", newline="") as file:
#         writer = csv.writer(file)
#         writer.writerow(["Timestamp", "Temp", "Hum", "PM2_5", "PM10", "Ozone", "Carbon", "Nitro", "Sulfur", "people_no"])

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

# 🔹 บันทึกลง Supabase
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
        people_no = data["people_no"]
        values = (
            timestamp,
            data["Temp"],
            data["Hum"],
            data["PM2_5"],
            data["PM10"],
            data["Ozone"],
            data["Carbon"],
            data["Nitro"],
            data["Sulfur"],
            people_no
        )
        cursor.execute(sql, values)
        connection.commit()
        print(f"✅  บันทึกสำเร็จ (people = {people_no})")

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
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ✅ สุ่ม people_no ก่อนบันทึก
        people_no = random.randint(20, 60)
        sensor_data["people_no"] = people_no

        with csv_lock:
            # with open(csv_filename, mode="a", newline="") as file:
            #     writer = csv.writer(file)
            #     writer.writerow([timestamp] + [sensor_data[key] for key in sensor_data])
            #     print("📁 บันทึก CSV:", sensor_data)

            supabase_timer += 1
            if supabase_timer >= 6:  # 2 รอบ = 20 วินาที
                save_to_data(sensor_data)
                supabase_timer = 0

            # ♻️ Reset ค่าทุกอย่าง
            for key in sensor_data:
                sensor_data[key] = None

# 🔹 รับ MQTT
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

# 🔹 เชื่อมต่อ MQTT
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
