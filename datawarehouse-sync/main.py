import hashlib
import logging
import sys
import typing as t
import json

from turbine.runtime import Record, Runtime

logging.basicConfig(level=logging.INFO)

def unwrap_envelope(records: t.List[Record]) -> t.List[Record]:
    '''unwraps the CDC envelope from the record returning only the payload'''
    updated = []
    for record in records:
        payload = record.value["payload"]

        try:
            if check_cdc_envelope(record):
                payload = payload["after"]
                new_record = Record(
                    key=record.key,
                    value=payload,
                    timestamp=record.timestamp,
                )
                updated.append(new_record)
            elif check_schema_envelope(record):
                # no need to unwrap
                updated.append(record)
        except Exception as e:
            print("Error occurred while parsing records: " + str(e))
    return updated

def check_cdc_envelope(record: Record) -> bool:
    '''determines if the record is a CDC formatted record, with an envelope'''
    if "payload" in record.value.keys() and "source" in record.value["payload"].keys():
        return True
    else:
        return False

def check_schema_envelope(record: Record) -> bool:
    '''determines if the record is a JSON with schema formatted record'''
    if "schema" in record.value.keys():
        return True
    else:
        return False

class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:
            source = await turbine.resources("pg")

            records = await source.records("orders", {})

            processed = await turbine.process(records, unwrap_envelope)

            destination_db = await turbine.resources("sfdwh")

            await destination_db.write(processed, "orders", {})
        except Exception as e:
            print(e, file=sys.stderr)
