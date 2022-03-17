#!/usr/bin/env python

import asyncio
import hashlib

from turbine import Turbine
from turbine.runtime import Record, Records


def anonymize(records: Records) -> Records:
    updated = []
    for record in records:
        valueToUpdate = record.value
        hashedEmail = hashlib.sha256(
            valueToUpdate['payload']['after']['email'].encode()).hexdigest()
        valueToUpdate['payload']['after']['email'] = hashedEmail
        updated.append(
            Record(
                key=record.key,
                value=valueToUpdate,
                timestamp=record.timestamp
            )
        )
    return Records(records=updated, stream="")


""" 
Entrypoint for a turbine app running against the Meroxa platform
"""


async def main():

    # Instantiate turbine client -> Platform runtime
    tb = Turbine(runtime='platform', path_to_data_app=".")

    # Get remote resource
    source = await tb.resources("store")

    # Read from remote resource
    records = await source.records("diffuser")

    # Deploy function with source as input
    anonymized = await tb.process(records, anonymize, {"test": "var"})

    # Get destination
    destinationDb = await tb.resources("s3")

    # Write results out
    await destinationDb.write(anonymized, "user_activity")

# Schedules and runs the asyncronous coroutine that uses the turbine platform
asyncio.run(main())
