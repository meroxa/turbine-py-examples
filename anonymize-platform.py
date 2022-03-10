#!/usr/bin/env python

import asyncio
import hashlib
import meroxa

from turbine import Turbine
from turbine.runtime import AppConfig
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


cfg = AppConfig(
    name="default",
    environment="staging",
    pipeline="Default",
    resources={
        "pg": "store",
        "s3": ""
    }
)

# Meroxa client
clientOptions = meroxa.ClientOptions(
    auth="",
    url=None
)


""" 
Entrypoint for a turbine app running against the Meroxa platform
"""
async def main(options):

    # Instantiate turbine client -> Platform runtime
    tb = Turbine(
        config=cfg,
        is_local=False,
        clientOptions=clientOptions,
        imageName="imageName"
    )

    # Get remote resource
    resource = await tb.runtime.resources("store")

    # Read from remote resource
    records = await resource.records("diffuser")

    print(records)

    # Get destination
    dest = await tb.runtime.resources("dest")

    # Deploy function with source as input
    # anonymized = await tb.runtime.process(session, records, anonymize,
    # {"test": "envvar"})

    # Write

    # print(anonymized)


# Schedules and runs the asyncronous corutine that uses the turbine platform 
asyncio.run(main(clientOptions))
