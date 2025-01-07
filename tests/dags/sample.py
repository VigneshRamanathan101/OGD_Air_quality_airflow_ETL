import json
js = """[{
      "country": "India",
      "state": "Chhattisgarh",
      "city": "Bhilai",
      "station": "32Bungalows, Bhilai - CECB",
      "last_update": "06-01-2025 22:00:00",
      "latitude": "21.194815",
      "longitude": "81.314770",
      "pollutant_id": "OZONE",
      "min_value": "6",
      "max_value": "113",
      "avg_value": "73"
    },
    {
      "country": "India",
      "state": "Chhattisgarh",
      "city": "Bhilai",
      "station": "Hathkhoj, Bhilai - CECB",
      "last_update": "06-01-2025 22:00:00",
      "latitude": "21.224231",
      "longitude": "81.408350",
      "pollutant_id": "OZONE",
      "min_value": "4",
      "max_value": "45",
      "avg_value": "33"
    },
    {
      "country": "India",
      "state": "Chhattisgarh",
      "city": "Chhal",
      "station": "Nawapara SECL Colony, Chhal - CECB",
      "last_update": "06-01-2025 22:00:00",
      "latitude": "22.118125",
      "longitude": "83.140608",
      "pollutant_id": "OZONE",
      "min_value": "18",
      "max_value": "68",
      "avg_value": "46"
    }]"""

transform_date = json.loads(js)
print(transform_date)

for i, data in enumerate(transform_date):
    for key, value in data.items():
        print(
            """INSERT INTO air_quality (
                country,
                state,
                city,
                station,
                last_update,
                latitude,
                longitude,
                pollutant_id,
                min_value,
                max_value,
                avg_value
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (data["country"], data["state"], data["city"], data["station"],
              data["last_update"], data["latitude"], data["longitude"],
              data["pollutant_id"], data["min_value"], data["max_value"],
              data["avg_value"]))

        # Commit the changes

        print()