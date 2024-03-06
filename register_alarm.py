import requests
import time
import json

url = "https://9e7zcye2he.execute-api.ap-northeast-2.amazonaws.com/dev/register_alarm"
headers = {
    "x-api-key": "P1fr2EBx5z6s0AAbdd8aQ52hD82ZFnmD3fExsaUM",
    "Content-Type": "application/json"
}

try:
    with open("alarm_time.txt", "r") as file:
        alarm_times = file.readlines()
        for alarm_line in alarm_times:
            alarm_info = alarm_line.strip().split(',')
            if len(alarm_info) == 4 and "thing" not in alarm_line:
                thing, property, desc, alarm_timestamp = alarm_info

                data = {
                    "thing": thing,
                    "property": property,
                    "ship_number": "HDGRC7F_W",
                    "priority": "WARNING",
                    "m_code": "MNALAB",
                    "desc": desc,
                    "desc_detail": "description detail",
                    "res": "res",
                    "alarm_level": "point",
                    "eas_group": "model version",
                    "device": "model name",
                    "area": "HiPOM",
                    "state": "A",
                    "alarm_time": alarm_timestamp
                }

                response = requests.post(url, json=data, headers=headers)
                print("Json:", json.dumps(data, indent=4))  
                print("Response Code:", response.status_code)
                print("Response Content:", response.json())
                time.sleep(1)
            else:
                print("Invalid or header line in alarm_time.txt")
except FileNotFoundError:
    print("Alarm time file not found.")
