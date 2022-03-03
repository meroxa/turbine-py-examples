#!/usr/bin/env python

import asyncio
import hashlib
import meroxa

from turbine import Turbine
from turbine.runtime import AppConfig
from turbine.runtime import Record, Records

import typing as t


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
        "pg": "cheatham-resource-mar2",
        "s3": ""
    }
)

# Meroxa client
clientOptions = meroxa.ClientOptions(
    auth="",
    url=None
)


# Aysnc calls in python are required to be run in either a context
# manager or a managed async loop of some sort
async def main():

    # Instantiate turbine client -> Platform runtime
    tb = Turbine(
        config=cfg,
        is_local=False,
        clientOptions=clientOptions,
        imageName="imageName"
    )

    # Get remote resource
    resource = await tb.runtime.resources("cheatham-resource-mar2")

    # Read from remote resource
    records = await resource.records("diffuser")

    print(records)


asyncio.run(main())
