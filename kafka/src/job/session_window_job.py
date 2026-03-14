from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_green_trips_source(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def create_sessions_sink(t_env):
    table_name = 'sessions_per_location'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            num_trips BIGINT,
            PRIMARY KEY (PULocationID, window_start, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source = create_green_trips_source(t_env)
        sink = create_sessions_sink(t_env)

        t_env.execute_sql(f"""
        INSERT INTO {sink}
        SELECT
            PULocationID,
            window_start,
            window_end,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(TABLE {source}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY PULocationID, window_start, window_end
        """).wait()

    except Exception as e:
        print("Job failed:", str(e))


if __name__ == '__main__':
    run()