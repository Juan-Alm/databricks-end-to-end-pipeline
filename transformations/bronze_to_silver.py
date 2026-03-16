from pyspark.sql.functions import (
    col, to_date, date_format, trim, initcap, split, size, when, concat, lit, abs, to_timestamp, regexp_extract 
)

catalog = "workspace"
bronze_schema = "bronze"
silver_schema = "silver"

# clean claims

@dlt.table(
    name = f"{catalog}.{silver_schema}.claims",
    comment = "Cleaned claims",
    table_properties = {"quality": "silver"}
)

@dlt.expect_all_or_drop({
        "valid_claim_number": "claim_no IS NOT NULL",
        "valid_incident_hour": "incident_hour BETWEEN 0 AND 23", # col is not integer in bronze, parsed in claims()
    })

def claims():
    df = dlt.read(f"{catalog}.{bronze_schema}.claims")
    return(
        df.withColumn("incident_hour", col("hour").cast("int"))
        .withColumn("license_issue_date", to_date(col("license_issue_date"), "dd-MM-yyyy"))
        .withColumn("incident_date", to_date(col("date"), "yyyy-MM-dd"))
        .drop("hour")
        .drop("date")
        .drop("_rescued_data")
    )

    # clean policies

@dlt.table(
    name = f"{catalog}.{silver_schema}.policies",
    comment = "Cleaned policies",
    table_properties = {"quality": "silver"}
)

@dlt.expect_all_or_drop({
        "valid_policy_number": "policy_no IS NOT NULL"
    })

def policies():
    df = dlt.read(f"{catalog}.{bronze_schema}.policies")
    return(   
        df.withColumnRenamed("POLICY_NO", "policy_no")
        .withColumnRenamed("PREMIUM", "premium")
        .withColumn("premium", abs(col("premium")))
        .drop("_rescued_data")
    )

# clean customers

@dlt.table(
    name = f"{catalog}.{silver_schema}.customers",
    comment = "Cleaned customers",
    table_properties = {"quality": "silver"}
)

@dlt.expect_all_or_drop({
        "valid_customer_id": "customer_id IS NOT NULL"
    })



def customers():
    df = dlt.read(f"{catalog}.{bronze_schema}.customers")


    name_normalized = when(
        size(split(trim(col("name")), ",")) == 2,
        concat(
            initcap(trim(split(col("name"), ",").getItem(1))),
            lit(" "),
            initcap(trim(split(col("name"), ",").getItem(0)))
        )
    ).otherwise(initcap(trim(col("name"))))


    return(
        df.withColumn("date_of_birth", to_date(col("date_of_birth"), "dd-MM-yyyy"))
        .withColumn("firstname", split(name_normalized, " ").getItem(0))
        .withColumn("lastname", split(name_normalized, " ").getItem(1))
        .withColumn("address", concat(col("borough"), lit(", "), col("zip_code")))
        .drop("name", "_rescued_data")
    )

# clean telematics (use dlt.readStream)

@dlt.table(
    name = f"{catalog}.{silver_schema}.telematics",
    comment = "Cleaned telematics",
    table_properties = {"quality": "silver"}
)

@dlt.expect_all_or_drop({
        "valid_coordinates": "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180"
    })

def telematics():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.bronze_telematics")
    
    return(
        df.withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .drop("_rescued_data")
    )


# clean training images
@dlt.table(
    name = f"{catalog}.{silver_schema}.training_images",
    comment = "Cleaned training images",
    table_properties = {"quality": "silver"}
)

def training_images():
    df = dlt.read(f"{catalog}.{bronze_schema}.training_images")

    return(
        df.withColumn("label",
                      regexp_extract(col("path"), r"/(\d+)-([a-zA-Z]+)(?: \(\d+\))?\.png$", 2)
                      )
    )


# clean images

@dlt.table(
    name = f"{catalog}.{silver_schema}.claim_images",
    comment = "Cleaned claim images",
    table_properties = {"quality": "silver"}
)

def claim_images():
    df = dlt.read(f"{catalog}.{bronze_schema}.claim_images")

    return(
        df.withColumn("image_name", regexp_extract(col("path"), r".*/(.*?.jpg)", 1))
    )
                        

