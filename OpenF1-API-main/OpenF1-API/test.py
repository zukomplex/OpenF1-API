from urllib.request import urlopen
import json

# List die Daten über URL aus der API ein
response = urlopen('https://api.openf1.org/v1/position?meeting_key=1217&driver_number=40&position<=3')
position = json.loads(response.read().decode('utf-8'))

response = urlopen('https://api.openf1.org/v1/intervals?session_key=9165&interval<0.005')
intervals = json.loads(response.read().decode('utf-8'))

# Daten werden anhand der "driver_number" zu einem Datensatz verbunden    
    # Dictionary aus Intervals für schnelle Zuordnung
intervals_dict = {item["driver_number"]: item for item in intervals}

    # Daten werden gemerged
merged_data = []
for item in position:
    driver_number = item["driver_number"]
    interval_data = intervals_dict.get(driver_number, {})
    merged_item = {**item, **interval_data}
    merged_data.append(merged_item)

print(json.dumps(merged_data, indent=2))
print("\n")
print("\n")
print(json.dumps(position, indent=2))
print("\n")
print("\n")
print(json.dumps(intervals, indent=2))