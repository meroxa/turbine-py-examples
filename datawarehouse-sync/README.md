
# Data Warehouse Sync - Turbine Data App

This Turbine App fetches data in real-time from an operational datastore (Postgres) and pushes it into a Data Warehouse (Snowflake).

## Pre-requisites

* Source Postgres
* Destination Snowflake

## Run Locally

I have included both non-CDC and CDC formatted fixtures (`pg.json` and `pg_cdc.json` respectively). You will need to update the `app.json` to map the desired ficture to the `pg` resource referenced in the Turbine app.

To run (as with all Turbine Apps):

```
meroxa apps run
```

## Deploy

To deploy (as with all Turbine Apps):

```
meroxa apps deploy
```

## Functionality

This Turbine App does not actually do much beyond optionally __unwrapping__ CDC formatted records for writing to a Snowflake destination resource.

In the case where a non-CDC source is used (say a __polling based__ Postgres instance), it will simply pass through the records unmodified.

### Testing

Testing should follow standard Python development practices.

## Documentation && Reference

The most comprehensive documentation for Turbine and how to work with Turbine apps is on the Meroxa site: [https://docs.meroxa.com/](https://docs.meroxa.com)