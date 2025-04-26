import requests

url = 'https://aqi-prediction.azurewebsites.net/data'
payload = {
    'temp': '2025-04-21'
}

response = requests.post(url, json=payload)

print(response.status_code)  # ดู status code (เช่น 200, 404, 500)
print(response.text)         # ดูข้อความที่ได้กลับมา