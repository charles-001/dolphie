from dolphie import Dolphie
from dolphie.Functions import format_number
from textual.widgets import Sparkline


def update_sparklines(dolphie: Dolphie):
    statuses = dolphie.statuses
    saved_status = dolphie.saved_status
    loop_duration_seconds = dolphie.loop_duration_seconds

    if not saved_status:
        queries_per_second = 0
        selects_per_second = 0
        inserts_per_second = 0
        updates_per_second = 0
        deletes_per_second = 0
    else:
        queries_per_second = round((statuses["Queries"] - saved_status["Queries"]) / loop_duration_seconds)
        selects_per_second = round((statuses["Com_select"] - saved_status["Com_select"]) / loop_duration_seconds)
        inserts_per_second = round((statuses["Com_insert"] - saved_status["Com_insert"]) / loop_duration_seconds)
        updates_per_second = round((statuses["Com_update"] - saved_status["Com_update"]) / loop_duration_seconds)
        deletes_per_second = round((statuses["Com_delete"] - saved_status["Com_delete"]) / loop_duration_seconds)

    sparklines = dolphie.app.query("Sparkline")

    # Dictionary to map sparkline names to query types
    query_types = {
        "dashboard_panel_qps": queries_per_second,
        "dml_panel_data_queries": queries_per_second,
        "dml_panel_data_select": selects_per_second,
        "dml_panel_data_insert": inserts_per_second,
        "dml_panel_data_update": updates_per_second,
        "dml_panel_data_delete": deletes_per_second,
    }

    for sparkline in sparklines:
        sparkline: Sparkline

        dml_per_second = query_types.get(sparkline.id)

        if dml_per_second is None:
            # If the sparkline doesn't have a valid name, continue to the next one
            continue

        if dml_per_second > 0:
            sparkline_data = dolphie.dml_panel_qps.setdefault(sparkline.id, [])

            sparkline_data.append(dml_per_second)

            # Retain only the last 300 data points
            sparkline_data = sparkline_data[-300:]

            dolphie.dml_panel_qps[sparkline.id] = sparkline_data

            sparkline.data = sparkline_data
            sparkline.refresh()

            if sparkline.id.startswith("dml_panel"):
                dml = sparkline.id.split("_")[3].upper()
                dolphie.app.query_one(f"#{sparkline.id}_label").update(
                    f"[b #c5c7d2]{dml}[/b #c5c7d2] ({format_number(dml_per_second)})"
                )
            else:
                if dolphie.display_dml_panel:
                    dolphie.app.query_one("#dashboard_panel_qps").display = False
