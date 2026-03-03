import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    import duckdb

    DATABASE_URL = "/Users/ivan/Desktop/TODO/Python/dlt-nyc/nyc/taxi_pipeline.duckdb"
    engine = duckdb.connect(DATABASE_URL, read_only=False)
    return (engine,)


@app.cell
def _(engine):
    result = engine.execute("""
        SELECT
        MIN(trip_pickup_date_time) AS start_date,
        MAX(trip_pickup_date_time) AS end_date
        FROM nyc_taxi.trips;
    """).fetchall()

    print(result)
    return


@app.cell
def _(engine):
    result2 = engine.execute("""
        SELECT
            COUNT(*) FILTER (WHERE payment_type = 'Credit') * 1.0
            / COUNT(*) AS credit_card_proportion
        FROM nyc_taxi.trips;
    """).fetchone()

    print(result2)
    return


@app.cell
def _(engine):
    result3 = engine.execute("""
        SELECT
            SUM(tip_amt) AS total_tips
        FROM nyc_taxi.trips;
    """).fetchone()

    print(result3)
    return


if __name__ == "__main__":
    app.run()
