import json
import logging
import sys
import typing as t

from turbine.runtime import Record, Runtime

from alert import send_slack_alert

logging.basicConfig(level=logging.INFO)

WEBHOOK_URL = (
    ""  # Obtain a Slack webhook URL from your Slack Apps dashboard
)


def send_alert(records: t.List[Record]) -> t.List[Record]:
    for record in records:
        try:
            if isinstance(record.value, str):
                payload = json.loads(record.value)["payload"]
            else:
                payload = record.value["payload"]

            send_slack_alert(
                webhook_url=WEBHOOK_URL,
                payload=payload
            )
        except Exception as e:
            logging.warning(f"Error occurred while parsing records: {e}")

    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):

        try:
            source = await turbine.resources("pg")

            records = await source.records("customer_order")

            data = await turbine.process(records, send_alert)

            destination = await turbine.resources("webhook-dest")

            await destination.write(data, "customerOrders")
        except Exception as e:
            print(e, file=sys.stderr)
