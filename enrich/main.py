import logging
import typing as t

from turbine.runtime import Record, Runtime

from enrich import enrich_user_email


def enrich_data(records: t.List[Record]) -> t.List[Record]:
    for record in records:
        logging.info(f"Got email: {record.value['payload']['email']}")

        enrichment = enrich_user_email(record.value["payload"]["email"])

        record.value["payload"]["full_name"] = enrichment.full_name
        record.value["payload"]["company"] = enrichment.company
        record.value["payload"]["location"] = enrichment.location
        record.value["payload"]["role"] = enrichment.role
        record.value["payload"]["seniority"] = enrichment.seniority

    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):

        logging.basicConfig(level=logging.INFO)

        try:
            # Get remote resource
            resource = await turbine.resources("source_db")

            # Read from remote resource
            records = await resource.records("user_activity")

            # Deploy function with source as input
            enriched = await turbine.process(records, enrich_data, {})

            # Write results out
            await resource.write(enriched, "user_activity_enriched")

        except ChildProcessError as cpe:
            print(cpe)
        except Exception as e:
            print(e)
