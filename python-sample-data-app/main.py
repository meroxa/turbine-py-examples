import hashlib
import typing as t
from turbine.runtime import Record, Runtime


def anonymize(records: t.List[Record]) -> t.List[Record]:
    updated = []
    for record in records:
        try:
            value_to_update = record.value
            hashed_email = hashlib.sha256(
                value_to_update["payload"]["after"]["email"].encode()
            ).hexdigest()
            value_to_update["payload"]["after"]["email"] = hashed_email
            updated.append(
                Record(
                    key=record.key, 
                    value=value_to_update, 
                    timestamp=record.timestamp
                )
            )
        except Exception as e:
            print("Error occurred while parsing records: " + str(e))

    return updated


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:
            # Get remote resource
            source = await turbine.resources("source_name")

            # Read from remote resource
            records = await source.records("collection_name")

            # Deploy function with source as input
            anonymized = await turbine.process(records, anonymize)

            # Get destination
            destination_db = await turbine.resources("destination_name")

            # Write results out
            await destination_db.write(anonymized, "collection_name")

        except ChildProcessError as cpe:
            print(cpe)
        except Exception as e:
            print(e)

