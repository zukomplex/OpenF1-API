import requests
from pymongo import MongoClient


# Connection zur MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['OpenF1']
collection = db['drivers']


# URL mit Session mit allen Fahrern abrufen
    # Stand 04.06.2025
driver_url = "https://api.openf1.org/v1/drivers?session_key=9967"
response = requests.get(driver_url)
full_driver_url_data = response.json()


# gewollte Daten extrahieren aus json-Datei
driver_entries = []
for driver in full_driver_url_data:
    driver_entries.append({
        "full_name": driver.get("full_name"),
        "driver_number": driver.get("driver_number"),
        "team_name": driver.get("team_name"),
    })

# bestehende Collection leeren
collection.delete_many({})

# Daten in MongoDB speichern
if driver_entries:
    collection.insert_many(driver_entries)
    print(f"{len(driver_entries)} Einträge eingefügt!")

else:
    print("Keine Daten eingefügt!")

print("fertig")