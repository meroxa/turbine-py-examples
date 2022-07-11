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
                new_record = unwrap_cdc_envelope(record)
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

def unwrap_cdc_envelope(record: Record) -> Record:
    '''unwraps the CDC envelope from the record returning only the payload, with the correct schema'''
    payload = record.value["payload"]
    schema_fields = record.value["schema"]["fields"]
    for i in schema_fields:
        if i["field"] == "after":
            print("found after schema")
            # reformat schema
            del i['field']
            i['name'] = record.value["schema"]["name"]
            record.value["schema"] = i
            break

    record.value["payload"] = payload["after"]
    return record

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
