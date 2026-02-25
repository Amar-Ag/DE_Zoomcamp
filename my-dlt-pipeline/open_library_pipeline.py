"""dlt pipeline to ingest books data from the Open Library REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_source(
    s3_access_key: str = dlt.secrets.value,
    s3_secret_key: str = dlt.secrets.value,
):
    """Define dlt resources from Open Library REST API endpoints."""
    # Archive.org S3 keys use the format: Authorization: LOW <access>:<secret>
    auth_header = f"LOW {s3_access_key}:{s3_secret_key}"

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org",
            "headers": {"Authorization": auth_header},
        },
        "resource_defaults": {
            "primary_key": "key",
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "books",
                # Open Library's search endpoint is the list API for books.
                # The /api/books endpoint requires specific bibkeys (ISBNs/IDs)
                # and is not suitable for bulk extraction.
                "endpoint": {
                    "path": "/search.json",
                    "params": {
                        # Adjust `q` to narrow down the result set.
                        # "subject:science" returns ~570k results; use a more
                        # specific query (e.g. "python programming") for faster runs.
                        "q": "subject:science",
                        "fields": (
                            "key,title,author_name,author_key,"
                            "isbn,first_publish_year,number_of_pages_median,"
                            "subject,language,edition_count"
                        ),
                        "limit": 100,
                    },
                    "data_selector": "docs",
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        "total_path": "numFound",
                        # Remove or increase `maximum_page` once you're ready
                        # to load the full dataset.
                        "maximum_page": 5,
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="open_library_pipeline",
    destination="duckdb",
    # `refresh="drop_sources"` cleans data and state on each run.
    # Remove once you have a working incremental pipeline.
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    # Keys are loaded automatically from .dlt/secrets.toml
    load_info = pipeline.run(open_library_source())
    print(load_info)  # noqa: T201
