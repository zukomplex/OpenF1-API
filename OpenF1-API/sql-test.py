from urllib.request import urlopen
import json
import mysql.connector
import time
from datetime import datetime

# Verbindung zur DB
conn = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "",
    database = "f1"
)

cursor = conn.cursor()

# Tabellen erstellen --> ´interval´ muss mit Backticks geschrieben werden weil Befehl in SQL sonst schon vergeben
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

while True:
    try:
        
        # List die Daten über URL aus der API ein
        response = urlopen('https://api.openf1.org/v1/position')
        position = json.loads(response.read().decode('utf-8'))

        response = urlopen('https://api.openf1.org/v1/intervals')
        intervals = json.loads(response.read().decode('utf-8'))

        # Daten in Tabellen einfügen
        for item in position:
            cursor.execute("""
            INSERT IGNORE INTO positions (date, driver_number, meeting_key, position, session_key)
            VALUES (%s, %s, %s, %s, %s)
            """, (
                item["date"],
                item["driver_number"],
                item["meeting_key"],
                item["position"],
                item["session_key"]
            ))

        for item in intervals:
            cursor.execute("""
            INSERT IGNORE INTO intervals (date, driver_number, gap_to_leader, `interval`, meeting_key, session_key)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                item["date"],
                item["driver_number"],
                item["gap_to_leader"],
                item["interval"],
                item["meeting_key"],
                item["session_key"]
            ))

        # Änderungen speichern und Verbindung schließen
        conn.commit()
        print(f"{datetime.now()} - neue Daten gespeichert.")

    except Exception as e:
        print("Fehler beim Abruf oder Einfügen!", e)

    # Warten bis zur nächsten Abfrage
    time.sleep(4)