import dlt

@dlt.table(
    name="bronze_telematics",
    comment="Raw telematics ingestion from files"
)
def bronze_telematics():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/workspace/default/telematics_raw/schemas/bronze_telematics"
        )
        .load("/Volumes/workspace/default/telematics_raw/raw_telematics")
    )