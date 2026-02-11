with source as (
    select * from {{ source ('raw_data', 'fhv_tripdata_2019_partitioned_clustered') }}
),
renamed as (
    select *
    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where dispatching_base_num  is not null
)

select * from renamed