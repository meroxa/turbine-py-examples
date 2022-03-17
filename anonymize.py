#!/usr/bin/env python

import os

import hashlib
import typing as t

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

tb = Turbine(runtime='local', path_to_data_app='.')

resource = tb.runtime.resources("pg")

records = resource.records("user_activity")

anonymized = tb.process(records.records, anonymize)

destination = tb.runtime.resources("s3")

destination.write(anonymized, "user_activity")
