"""dlt pipeline to ingest NYC taxi data from the DE Zoomcamp REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_source():
    """Define dlt resources from the NYC taxi REST API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net",
            # Public API — no authentication required
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "rides",
                "endpoint": {
                    "path": "/data_engineering_zoomcamp_api",
                    # Response is a direct JSON array (not wrapped in an object)
                    "data_selector": "$",
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        # Must set total_path=None to override the default of "total".
                        # This API returns a bare array with no total count field,
                        # so we rely on stop_after_empty_page instead.
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_source())
    print(load_info)  # noqa: T201
