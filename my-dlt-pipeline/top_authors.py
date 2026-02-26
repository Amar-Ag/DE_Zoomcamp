import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # Top 10 Authors by Book Count
    Data sourced from the **Open Library** pipeline via dlt + DuckDB.
    """)
    return


@app.cell
def _():
    import dlt
    import ibis
    import plotly.express as px

    return dlt, ibis, px


@app.cell
def _(dlt):
    pipeline = dlt.pipeline(
        pipeline_name="open_library_pipeline",
        destination="duckdb",
    )
    # .ibis() returns an ibis-backed view of the dataset
    dataset = pipeline.dataset().ibis(read_only=True)
    return (dataset,)


@app.cell
def _(dataset, ibis, mo, px):
    try:
        # dlt stores list-type fields in child tables named {parent}__{field}
        # books.author_name (a list) → child table books__author_name
        # each row has: _dlt_parent_id (FK to books), _dlt_list_idx, value
        authors = dataset.table("books__author_name")

        top_authors = (
            authors
            .group_by("value")
            .aggregate(book_count=authors._dlt_parent_id.nunique())
            .order_by(ibis.desc("book_count"))
            .limit(10)
            .execute()
            .rename(columns={"value": "author"})
        )

        fig = px.bar(
            top_authors,
            x="book_count",
            y="author",
            orientation="h",
            title="Top 10 Authors by Book Count",
            labels={"book_count": "Number of Books", "author": "Author"},
            color="book_count",
            color_continuous_scale="Blues",
        )
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            coloraxis_showscale=False,
            plot_bgcolor="white",
            height=450,
        )
        output = mo.ui.plotly(fig)

    except Exception as e:
        output = mo.callout(
            mo.md(
                f"**No data found.**\n\n"
                f"Run the pipeline first, then refresh this notebook:\n"
                f"```\npython open_library_pipeline.py\n```\n\n"
                f"_Error: `{e}`_"
            ),
            kind="warn",
        )
    return (output,)


@app.cell
def _(output):
    output
    return


if __name__ == "__main__":
    app.run()
