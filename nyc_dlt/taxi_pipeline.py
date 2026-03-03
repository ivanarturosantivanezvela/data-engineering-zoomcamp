"""REST API source for NYC taxi data (Data Engineering Zoomcamp API)."""

import dlt
import requests
from typing import Iterator, List, Dict

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
PAGE_SIZE = 1_000

@dlt.resource(
    name="trips",
    write_disposition="replace",
)
def trips() -> Iterator[List[Dict]]:
    """
    NYC Taxi trips REST API source.
    Pagination stops when an empty page is returned.
    """
    page = 1

    while True:
        response = requests.get(
            BASE_URL,
            params={
                "page": page,
                "page_size": PAGE_SIZE,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        # STOP CONDITION (this is what you want)
        if not data:
            break

        yield data
        page += 1


def run():
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi",
    )

    load_info = pipeline.run(trips())
    print(load_info)


if __name__ == "__main__":
    run()
