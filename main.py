import asyncio
import hashlib
import os
import typing as t
from pathlib import Path

from turbine import Turbine
from turbine.runtime import Record


def anonymize(records: t.List[Record]) -> t.List[Record]:
    updated = []
    for record in records:
        try: 
            value_to_update = record.value
            hashed_email = hashlib.sha256(
                value_to_update['payload']['after']['email'].encode()).hexdigest()
            value_to_update['payload']['after']['email'] = hashed_email
            updated.append(
                Record(
                    key=record.key,
                    value=value_to_update,
                    timestamp=record.timestamp
                )
            )
        except Exception as e: 
            print("Error occured while parsing records: " + str(e)) 

    return updated


class App:

    @staticmethod
    async def run(turbine: Turbine):
        # Get remote resource
        source = await turbine.resources("source_name")

        # Read from remote resource
        records = await source.records("user_activity")

        # Deploy function with source as input
        anonymized = await turbine.process(records, anonymize)

        # Get destination
        destination_db = await turbine.resources("destination_resource")

        # Write results out
        await destination_db.write(anonymized, "user_activity")

    
def main():
    curr = os.path.abspath(os.path.dirname(__file__))

    asyncio.run(
        App.run(
            Turbine(
                runtime="local",
                path_to_data_app=curr
            )
        )
    )


if __name__ == "__main__":
    main()
