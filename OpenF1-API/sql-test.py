from urllib.request import urlopen
import json
import mysql.connector

# List die Daten über URL aus der API ein
response = urlopen('https://api.openf1.org/v1/position?meeting_key=1217&driver_number=40&position<=3')
position = json.loads(response.read().decode('utf-8'))

response = urlopen('https://api.openf1.org/v1/intervals?session_key=9165&interval<0.005')
intervals = json.loads(response.read().decode('utf-8'))

# Verbindung zur DB
conn = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "",
    database = "f1"
)

cursor = conn.cursor()

# Tabellen erstellen
cursor.execute("""
CREATE TABLE IF NOT EXISTS positions (
   date DATETIME(3),
   driver_number VARCHAR(2) PRIMARY KEY,
   meeting_key VARCHAR(45),
   position INT,
   session_key VARCHAR(45)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS intervals (
    date DATETIME(3),
    driver_number VARCHAR(2) PRIMARY KEY,
    gap_to_leader DOUBLE,
    `interval` DOUBLE, 
    meeting_key VARCHAR(45),
    session_key VARCHAR(45)
)
""")

# Daten in Tabelle einfügen
for item in position:
    cursor.execute("""
    INSERT INTO positions (date, driver_number, meeting_key, position, session_key)
    VALUES(%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        date = VALUES(date),
        driver_number = VALUES(driver_number),
        meeting_key = VALUES(meeting_key),
        position = VALUES(position),
        session_key = VALUES(session_key)
    """, (item["date"], item["driver_number"], item["meeting_key"], item["position"], item["session_key"]))

for item in intervals:
    cursor.execute("""
    INSERT INTO intervals (date, driver_number, gap_to_leader, `interval`, meeting_key, session_key)
    VALUES(%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        date = VALUES(date),
        driver_number = VALUES(driver_number),
        gap_to_leader = VALUES(gap_to_leader),
        `interval` = VALUES(`interval`),
        meeting_key = VALUES(meeting_key),
        session_key = VALUES(session_key)
    """, (item["date"], item["driver_number"], item["gap_to_leader"], item["interval"], item["meeting_key"], item["session_key"]))

# Änderungen speichern und Verbindung schließen
conn.commit()
cursor.close()
conn.close()