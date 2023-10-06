import csv
import sqlite3
conn = sqlite3.connect('smoke_data.db')
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS new_smoke_table (
                    id INTEGER PRIMARY KEY,
                    CO2_concentration INTEGER)''')
data = []

with open('smoke_detection_iot.csv', newline='') as csvfile:
    fieldnames = ["id","UTC", "Temperature[C]", "Humidity[%]", "TVOC[ppb]", "eCO2[ppm]", "Raw H2", "Raw Ethanol",
                  "Pressure[hPa]", "PM1.0", "PM2.5", "NC0.5", "NC1.0", "NC2.5", "CNT", "Fire Alarm"]
    lines = csv.DictReader(csvfile, fieldnames= fieldnames)
    next(lines)
    line_limit = 0


    for row in lines:
        data.append( {
            "id": row["id"],
            "CO2_concentration": row["eCO2[ppm]"]
        })
        line_limit += 1
        if line_limit == 300:
            break
for dic_rows in data:
    cursor.execute('''INSERT OR REPLACE INTO new_smoke_table (id, CO2_concentration) VALUES (?, ?)''', (dic_rows["id"], dic_rows["CO2_concentration"]))
res = cursor.execute('''SELECT * FROM new_smoke_table''')
k = res.fetchall()
for line in k:
    print(line)

conn.commit()
conn.close()
