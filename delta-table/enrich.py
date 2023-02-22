from __future__ import annotations

import os

import requests

VALIDATION_API_URL = "https://maps.googleapis.com/maps/api/geocode/json"
GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")


class GeoLocation:
    def __init__(self, lat, lon):
        self.latitude = lat
        self.longitude = lon


def get_geo_location_from_postcode(postcode: str) -> GeoLocation:

    params = {"address": postcode, "key": GOOGLE_KEY}
    response = requests.post(
        url=VALIDATION_API_URL,
        params=params
    )

    geocode = response.json().get("results").get("geometry")

    return GeoLocation(
        lat=geocode.get("location").get("lat"),
        lon=geocode.get("location").get("lng"),
    )
