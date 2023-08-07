from datetime import datetime

import plotext as plt
from dolphie import Dolphie
from dolphie.Functions import format_number
from rich.ansi import AnsiDecoder
from rich.console import Group
from rich.jupyter import JupyterMixin
from textual.widgets import Sparkline

DML_TYPES = {
    "queries": "Queries",
    "select": "Com_select",
    "insert": "Com_insert",
    "update": "Com_update",
    "delete": "Com_delete",
}


def update_data(dolphie: Dolphie):
    statuses = dolphie.statuses
    saved_status = dolphie.saved_status
    loop_duration_seconds = dolphie.loop_duration_seconds

    if saved_status:
        for component, data in dolphie.qps_data.items():
            if component == "datetimes":
                continue

            dml_type = component.split("_")[-1]
            status_key = DML_TYPES.get(dml_type, None)

            if status_key is not None:
                qps_value = round((statuses[status_key] - saved_status[status_key]) / loop_duration_seconds)
                data["qps"].append(qps_value)

            if component == "dashboard_panel_queries":
                sparkline = dolphie.app.query_one("#dashboard_panel_queries", Sparkline)
                if not sparkline.display:
                    sparkline.display = True

                sparkline.data = data["qps"]
                sparkline.refresh()

        formatted_datetime = datetime.now().strftime("%H:%M:%S")
        dolphie.qps_data["datetimes"].append(formatted_datetime)


class create_plot(JupyterMixin):
    def __init__(self, qps_data):
        self.qps_data = qps_data

    def __rich_console__(self, console, options):
        width = options.max_width or console.width
        height = 15
        max_y_value = 0

        plt.clf()

        plt.date_form("H:M:S")
        plt.canvas_color((3, 9, 24))
        plt.axes_color((3, 9, 24))
        plt.ticks_color((144, 169, 223))

        plt.plotsize(width, height)

        for component, component_data in self.qps_data.items():
            if component == "datetimes" or not component_data["visible"]:
                continue

            component_name = component.split("_")[2]
            x = self.qps_data["datetimes"]
            y = component_data["qps"]

            if y:
                plt.plot(x, y, marker="braille", label=component_name.upper(), color=component_data["color"])
                max_y_value = max(max_y_value, max(y))

        # I create my own y ticks to format the numbers how I like them
        max_y_ticks = 5
        y_tick_interval = max_y_value / max_y_ticks
        y_ticks = [i * y_tick_interval for i in range(max_y_ticks + 1)]
        y_labels = [format_number(val, for_plot=True, decimal=1) for val in y_ticks]
        plt.yticks(y_ticks, y_labels)

        yield Group(*AnsiDecoder().decode(plt.build()))
