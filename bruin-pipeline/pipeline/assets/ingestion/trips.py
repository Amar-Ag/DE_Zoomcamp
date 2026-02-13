"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: gcp-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: integer
    description: Provider that generated the record (1=Creative Mobile, 2=VeriFone)
  - name: pickup_datetime
    type: timestamp
    description: Date and time the meter was engaged
  - name: dropoff_datetime
    type: timestamp
    description: Date and time the meter was disengaged
  - name: passenger_count
    type: integer
    description: Number of passengers in the vehicle
  - name: trip_distance
    type: float64
    description: Elapsed trip distance in miles
  - name: ratecode_id
    type: integer
    description: Final rate code in effect (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group)
  - name: store_and_fwd_flag
    type: string
    description: Whether the trip record was held before sending (Y=store and forward, N=not)
  - name: pu_location_id
    type: integer
    description: TLC taxi zone where the meter was engaged
  - name: do_location_id
    type: integer
    description: TLC taxi zone where the meter was disengaged
  - name: payment_type
    type: integer
    description: Numeric code of how the passenger paid
  - name: fare_amount
    type: float64
    description: Time-and-distance fare calculated by the meter
  - name: extra
    type: float64
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float64
    description: MTA tax triggered based on the metered rate in use
  - name: tip_amount
    type: float64
    description: Tip amount (automatically populated for credit card tips)
  - name: tolls_amount
    type: float64
    description: Total amount of all tolls paid in trip
  - name: improvement_surcharge
    type: float64
    description: Improvement surcharge assessed at the flag drop
  - name: total_amount
    type: float64
    description: Total amount charged to passengers
  - name: congestion_surcharge
    type: float64
    description: Total amount collected for NYS congestion surcharge
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
  - name: extracted_at
    type: timestamp
    description: Timestamp when the record was extracted

@bruin"""

import os
import json
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta


def materialize():
    start_date = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d").date()
    end_date = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d").date()
    taxi_types = json.loads(os.environ.get("BRUIN_VARS", "{}")).get("taxi_types", ["yellow", "green"])

    # Build list of (taxi_type, year, month) combos within the run window
    months = []
    current = date(start_date.year, start_date.month, 1)
    last = date(end_date.year, end_date.month, 1)
    while current <= last:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    extracted_at = datetime.utcnow()
    frames = []

    for taxi_type in taxi_types:
        for year, month in months:
            url = f"{base_url}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            try:
                df = pd.read_parquet(url)

                # Normalise column names across yellow/green schemas
                df = df.rename(columns={
                    # yellow
                    "tpep_pickup_datetime": "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime",
                    # green
                    "lpep_pickup_datetime": "pickup_datetime",
                    "lpep_dropoff_datetime": "dropoff_datetime",
                    "VendorID": "vendor_id",
                    "RatecodeID": "ratecode_id",
                    "PULocationID": "pu_location_id",
                    "DOLocationID": "do_location_id",
                    "payment_type": "payment_type",
                })

                keep = [
                    "vendor_id", "pickup_datetime", "dropoff_datetime",
                    "passenger_count", "trip_distance", "ratecode_id",
                    "store_and_fwd_flag", "pu_location_id", "do_location_id",
                    "payment_type", "fare_amount", "extra", "mta_tax",
                    "tip_amount", "tolls_amount", "improvement_surcharge",
                    "total_amount", "congestion_surcharge",
                ]
                df = df[[c for c in keep if c in df.columns]]
                df["taxi_type"] = taxi_type
                df["extracted_at"] = extracted_at
                frames.append(df)
            except Exception as e:
                print(f"Skipping {taxi_type} {year}-{month:02d}: {e}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)
