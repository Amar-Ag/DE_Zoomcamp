import marimo

__generated_with = "0.20.2"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # SQL Explorer
    Query your dlt pipeline data with custom SQL.
    """)
    return


@app.cell
def _(mo):
    pipeline_picker = mo.ui.dropdown(
        options=["taxi_pipeline", "open_library_pipeline"],
        value="taxi_pipeline",
        label="Pipeline",
    )
    pipeline_picker
    return (pipeline_picker,)


@app.cell
def _(pipeline_picker):
    import dlt
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_picker.value,
        destination="duckdb",
    )
    backend = pipeline.dataset().ibis(read_only=True)
    return (backend,)


@app.cell
def _(backend, mo):
    tables = backend.list_tables()
    mo.callout(
        mo.md("**Available tables:** " + ", ".join(f"`{t}`" for t in tables)),
        kind="info",
    )
    return


@app.cell
def _(mo):
    # ── Add or edit queries here — no extra cells or buttons needed ──
    default_queries = [
        "SELECT * FROM rides LIMIT 10",
        "SELECT MIN(trip_pickup_date_time), MAX(trip_pickup_date_time) FROM rides",
        "SELECT payment_type, COUNT(*) AS trips,  COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rides) AS percentage FROM rides GROUP BY 1 ORDER BY 2 DESC",
        "SELECT SUM(tip_amt) FROM rides"
    ]
    # mo.ui.array gives marimo proper reactive tracking of each element.
    # A plain Python list won't trigger re-execution when a button inside it is clicked.
    editors = mo.ui.array([mo.ui.code_editor(value=q, language="sql") for q in default_queries])
    buttons = mo.ui.array([mo.ui.run_button(label="▶ Run") for _ in default_queries])
    return buttons, editors


@app.cell
def _(backend, buttons, editors, mo):
    panels = []
    for i in range(len(editors)):
        editor, button = editors[i], buttons[i]
        if button.value:
            try:
                df = backend.sql(editor.value).execute()
                result = mo.vstack([
                    mo.md(f"**{len(df):,} rows returned**"),
                    mo.ui.table(df, pagination=True, page_size=10),
                ])
            except Exception as e:
                result = mo.callout(mo.md(f"**Error:** `{e}`"), kind="danger")
        else:
            result = mo.callout(mo.md("Click **▶ Run** to execute."), kind="neutral")

        panels.append(mo.vstack([
            mo.md(f"### Query {i + 1}"),
            editor,
            button,
            result,
        ]))

    all_panels = mo.vstack(panels)
    return (all_panels,)


@app.cell
def _(all_panels):
    all_panels
    return


if __name__ == "__main__":
    app.run()
