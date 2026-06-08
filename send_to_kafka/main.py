import requests
response = requests.get("https://api.open-meteo.com/v1/forecast", params={
    "latitude": 51.5,
    "longtitude": -0.11,
    "current": "temperature_2m",
})
print(response.json())