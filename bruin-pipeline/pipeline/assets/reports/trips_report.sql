/* @bruin

name: reports.trips_report
type: bq.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: Date of trip pickup
    primary_key: true
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    primary_key: true
  - name: payment_type_name
    type: string
    description: Payment method used
    primary_key: true
  - name: total_trips
    type: integer
    description: Number of trips
    checks:
      - name: non_negative
  - name: total_passengers
    type: integer
    description: Total number of passengers
    checks:
      - name: non_negative
  - name: total_distance_miles
    type: float64
    description: Total trip distance in miles
    checks:
      - name: non_negative
  - name: total_revenue
    type: float64
    description: Total fare revenue
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: float64
    description: Average fare per trip
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float64
    description: Average trip distance in miles
    checks:
      - name: non_negative

@bruin */

SELECT
    DATE(pickup_datetime)       AS pickup_date,
    taxi_type,
    COALESCE(payment_type_name, 'unknown') AS payment_type_name,
    COUNT(*)                    AS total_trips,
    SUM(passenger_count)        AS total_passengers,
    ROUND(SUM(trip_distance), 2) AS total_distance_miles,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(fare_amount), 2)  AS avg_fare_amount,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3
