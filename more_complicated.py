import sys

from Turbine import turbine


class MyFunction(Turbine):
    def task(lfkjsdflkjsdfklj):
        return


# Take a list of records and return a list of mutated records
def anonymize(records):

    return hash(records)


def main() -> int:

    # Fetch existing resource
    pg = turbine.Resource("mypg")

    # Pull records from table
    records = pg.records("user_activity")

    # mutate records
    updatedRecords = turbine.process(records, anonymize)

    # Destination resource
    destination = turbine.Resource("myS3bucket")

    # Write to that destination
    destination.write(updatedRecords, "user_activity_masked")

    return


if __name__ == '__main__':
    sys.exit(main())
