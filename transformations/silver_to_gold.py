import geopy
import pandas as pd
from pyspark.sql.functions import col, lit, concat, pandas_udf, avg
from typing import Iterator
import random

catalog = "workspace"
silver_schema = "silver"
gold_schema = "gold"

def geocode(geolocator, address):
    try:
        # demo mode (skip real API calls)
        return pd.Series({
            "latitude": random.uniform(-90, 90),
            "longitude": random.uniform(-180, 180)
        })

        # real version
        location = geolocator.geocode(address)
        if location:
            return pd.Series({
                "latitude": location.latitude,
                "longitude": location.longitude
            })

    except Exception:
        return pd.Series({"latitude": None, "longitude": None})
    
@pandas_udf("struct<latitude:double, longitude:double>")
def get_lat_long(addresses: pd.Series) -> pd.Series:

    geolocator = geopy.Nominatim(user_agent="databricks_geocoder")

    return addresses.apply(lambda addr: geocode(geolocator, addr))
    


# telematics

@dlt.table(
    name = f"{catalog}.{gold_schema}.aggregated_telematics",
    comment = "Average telematics",
    table_properties = {"quality": "gold"}
    )

def telematics():
    df = dlt.read(f"{catalog}.{silver_schema}.telematics")
    return(
        df.groupBy("chassis_no")
        .agg(
            avg("speed").alias("telematics_speed"),
            avg("latitude").alias("telematics_latitude"),
            avg("longitude").alias("telematics_longitude"),
            )
        )
    
# claim policy
@dlt.table(
    name = f"{catalog}.{gold_schema}.customer_claim_policy",
    comment = "Curated claim joined with policy records",
    table_properties = {"quality": "gold"}
    )

def customer_claim_policy():
    #read the cleaned policy records
    policy = (
        dlt.read(f"{catalog}.{silver_schema}.policies")
        .drop("created_at", "updated_at")
        .withColumnRenamed("CUST_ID", "customer_id")
        .withColumnRenamed("CHASSIS_NO", "chassis_no")
    )

    #read the cleaned claims records
    claim = dlt.read(f"{catalog}.{silver_schema}.claims")

    #read the cleaned customer records
    customer = (
        dlt.read(f"{catalog}.{silver_schema}.customers")
        .drop("created_at", "updated_at")
    )

    claim_policy = claim.join(policy, "policy_no")

    return claim_policy.join(customer, "customer_id")




# claim-policy-telematics
@dlt.table(
    name = f"{catalog}.{gold_schema}.customer_claim_policy_telematics",
    comment = "Claims with geolocation latitude/longitud",
    table_properties = {"quality": "gold"}
    )

def customer_claim_policy_telematics():

    telematics = dlt.read(f"{catalog}.{gold_schema}.aggregated_telematics")

    customer_claim_policy = (
        dlt.read(f"{catalog}.{gold_schema}.customer_claim_policy")
        .where("borough is not null")
        .withColumnRenamed("CHASSIS_NO", "chassis_no")
    )

    return (
        customer_claim_policy
        .withColumn("lat_long", get_lat_long(concat(col("address"), lit(""))))
        .join(telematics, "chassis_no", "left")
        .withColumn("latitude", col("lat_long.latitude"))
        .withColumn("longitude", col("lat_long.longitude"))
        .drop("lat_long")
    )

