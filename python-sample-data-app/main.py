import asyncio
import hashlib
import os
import typing as t
import pdb
from turbine import Turbine
from turbine.runtime import Record
import pdb

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
<<<<<<< Updated upstream
        # Get remote resource
        try:
            # Find resource 
            source = await turbine.resources("source_name")
            if isinstance(source, ChildProcessError):
                raise ChildProcessError(source)

            # Read from remote resource
            records = await source.records("collection_name")
            if isinstance(records, ChildProcessError):
                raise ChildProcessError(records)

            # Deploy function with source as input
            anonymized = await turbine.process(records, anonymize)
            if isinstance(anonymized, ChildProcessError):
                raise ChildProcessError(anonymized)

            # Get destination
            destination_db = await turbine.resources("destination_name")
            if isinstance(destination_db, ChildProcessError):
                raise ChildProcessError(destination_db)

            # Write results out
            results = await destination_db.write(anonymized, "collection_name")
            if isinstance(results, ChildProcessError):
                raise ChildProcessError(results)

        except ChildProcessError as cpe: 
            print(cpe)
        except Exception as e: 
            print(e)
        
=======
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
>>>>>>> Stashed changes


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
