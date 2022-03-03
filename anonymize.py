#!/usr/bin/env python

import hashlib
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


pathToApp = "/Users/ericcheatham/workspace/turbine-py-examples/fixtures"

cfg = AppConfig("test", "test", "test", {"pg": "pg.json", "s3": ""})

tb = Turbine(
    config=cfg,
    pathToApp=pathToApp,
    is_local=True
)

resource = tb.runtime.resources("pg")

records = resource.records("user_activity")

anonymized = tb.process(records.records, anonymize)

destination = tb.runtime.resources("s3")

destination.write(anonymized, "user_activity")
