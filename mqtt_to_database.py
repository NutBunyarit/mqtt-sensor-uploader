import paho.mqtt.client as mqtt
import threading
import time
import pymysql
import socket
from datetime import datetime
import random
import requests
# 🔥 บังคับใช้ IPv4 กัน DNS พัง
if not hasattr(socket, "_original_getaddrinfo"):
    socket._original_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda *args, **kwargs: [
    ai for ai in socket._original_getaddrinfo(*args, **kwargs)
    if ai[0] == socket.AF_INET
]

# 🔹 ข้อมูล Database
# db_config = {
#     'host': 'sql12.freesqldatabase.com',
#     'user': 'sql12774523',
#     'password': 'pQQXJPx74e',
#     'database': 'sql12774523',
#     'port': 3306
# }

# 🔹 ตัวแปรเก็บข้อมูลเซ็นเซอร์
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

# 🔹 ฟังก์ชันบันทึกลง Database
def save_to_data(data):
    connection = None
    try:
        #connection = pymysql.connect(**db_config)
        #cursor = connection.cursor()
        url = 'https://aqi-prediction.azurewebsites.net/insert_data'
        payload = {
            'temp': data.get("Temp"),
            'hum' : data.get("Hum"),
            'pm2_5' : data.get("PM2_5"),
            'pm10' : data.get("PM10"),
            'ozone' : data.get("Ozone"),
            'carbon' : data.get("Carbon"),
            'nitro' : data.get("Nitro"),
            'sulfur' : data.get("Sulfur"),
            'people_no' : data.get("people_no"),
        }
        
        response = requests.post(url, json=payload)

        print(response.status_code)  # ดู status code (เช่น 200, 404, 500)
        print(response.text)         # ดูข้อความที่ได้กลับมา
        # sql = """
        # INSERT INTO sensor_data (
        #     timestamp, temp, hum, pm2_5, pm10, ozone, carbon, nitro, sulfur, people_no
        # ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        # """
        #timestamp = datetime.now()

        # ✅ แปลง None เป็น NULL อัตโนมัติ
        # values = (
        #     timestamp,
        #     data.get("Temp"),
        #     data.get("Hum"),
        #     data.get("PM2_5"),
        #     data.get("PM10"),
        #     data.get("Ozone"),
        #     data.get("Carbon"),
        #     data.get("Nitro"),
        #     data.get("Sulfur"),
        #     data.get("people_no")
        # )
       
       # cursor.execute(sql, values)
      #  connection.commit()
        print(f"✅  บันทึกสำเร็จ (people_no = {data.get('people_no')})")

    except Exception as e:
        print("❌  บันทกล้มเหลว:", e)

    # finally:
        # if connection:
        #     cursor.close()
        #     connection.close()

# 🔹 Thread สำหรับบันทึกข้อมูลเป็นระยะ
def periodic_save():
    supabase_timer = 0
    while True:
        time.sleep(10)

        people_no = random.randint(20, 60)
        sensor_data["people_no"] = people_no

        with csv_lock:
            supabase_timer += 1
            if supabase_timer >= 1:  # ทุก 60 วินาที
                save_to_data(sensor_data)
                supabase_timer = 0

            # ♻️ ล้างค่าเก่า
            for key in sensor_data:
                sensor_data[key] = None

# 🔹 รับข้อมูลจาก MQTT
def on_message(client, userdata, msg):
    topic = msg.topic
    try:
        value = float(msg.payload.decode("utf-8"))
    except:
        return

    with csv_lock:
        for key in sensor_data:
            if key.lower() in topic.lower():  # เช็ค topic case-insensitive
                sensor_data[key] = value
                break

# 🔹 เชื่อมต่อ MQTT
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ เชื่อมต่อ MQTT สำเร็จ")
        client.subscribe("IQA_Test/#")
    else:
        print(f"❌ เชื่อมต่อ MQTT ล้มเหลว: {rc}")

# 🚀 เริ่มโปรแกรม

mqtt_client = mqtt.Client()
mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect("broker.emqx.io", port=8883)

threading.Thread(target=periodic_save, daemon=True).start()
mqtt_client.loop_forever()
