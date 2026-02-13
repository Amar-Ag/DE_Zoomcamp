/* @bruin

name: staging.trips
type: bq.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: vendor_id
    type: integer
    description: Provider that generated the record
  - name: pickup_datetime
    type: timestamp
    description: Date and time the meter was engaged
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Date and time the meter was disengaged
  - name: passenger_count
    type: integer
    description: Number of passengers in the vehicle
  - name: trip_distance
    type: float64
    description: Elapsed trip distance in miles
    checks:
      - name: non_negative
  - name: pu_location_id
    type: integer
    description: TLC taxi zone where the meter was engaged
  - name: do_location_id
    type: integer
    description: TLC taxi zone where the meter was disengaged
  - name: payment_type
    type: integer
    description: Numeric code of how the passenger paid
  - name: payment_type_name
    type: string
    description: Human-readable payment method name
  - name: fare_amount
    type: float64
    description: Time-and-distance fare calculated by the meter
    checks:
      - name: non_negative
  - name: tip_amount
    type: float64
    description: Tip amount
    checks:
      - name: non_negative
  - name: total_amount
    type: float64
    description: Total amount charged to passengers
    checks:
      - name: non_negative
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)

custom_checks:
  - name: no_negative_trip_distance
    description: All trip distances should be zero or positive
    query: |
      SELECT COUNT(*)
      FROM staging.trips
      WHERE trip_distance < 0
    value: 0

@bruin */

WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY taxi_type, pickup_datetime, pu_location_id, do_location_id, total_amount
            ORDER BY extracted_at DESC
        ) AS rn
    FROM ingestion.trips
    WHERE pickup_datetime >= '{{ start_datetime }}'
      AND pickup_datetime < '{{ end_datetime }}'
      AND pickup_datetime IS NOT NULL
      AND trip_distance >= 0
      AND total_amount >= 0
)

SELECT
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.pu_location_id,
    t.do_location_id,
    t.payment_type,
    p.payment_type_name,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,
    t.taxi_type
FROM deduped t
LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id
WHERE t.rn = 1
