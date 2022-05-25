import os

import requests


class UserDetails:
    def __init__(self, full_name, location, role, seniority, company):
        self.full_name = full_name
        self.location = location
        self.role = role
        self.seniority = seniority
        self.company = company


class EnrichmentError(Exception):
    pass


def enrich_user_email(email: str):
    clearbit_key = os.getenv("CLEARBIT_API_KEY")

    params = {"email": email}
    headers = {"Authorization": f"Bearer {clearbit_key}"}
    response = requests.get(
        url="https://person-stream.clearbit.com/v2/combined/find",
        params=params,
        headers=headers,
    )

    try:
        person = response.json().get("person")
        company = response.json().get("company")

        return UserDetails(
            full_name=person["name"]["fullName"],
            location=person["location"],
            role=person["employment"]["role"],
            seniority=person["employment"]["seniority"],
            company=company["name"],
        )
    except Exception as e:
        raise EnrichmentError(f"Error fetching information: {e}")
