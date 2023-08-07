from datetime import datetime

import plotext as plt
from dolphie import Dolphie
from rich.ansi import AnsiDecoder
from rich.console import Group
from rich.jupyter import JupyterMixin
from textual.widgets import Sparkline


def update_data(dolphie: Dolphie):
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

    for component in dolphie.qps_data:
        if component == "datetimes":
            continue

        dml_type = component.split("_")[2]

        if dml_type == "queries":
            dml_qps = queries_per_second
        elif dml_type == "select":
            dml_qps = selects_per_second
        elif dml_type == "insert":
            dml_qps = inserts_per_second
        elif dml_type == "update":
            dml_qps = updates_per_second
        elif dml_type == "delete":
            dml_qps = deletes_per_second

        dolphie.qps_data[component]["qps"].append(dml_qps)

        if component == "dashboard_panel_queries":
            sparkline = dolphie.app.query_one("#dashboard_panel_queries", Sparkline)
            sparkline.data = dolphie.qps_data[component]["qps"]
            sparkline.refresh()

    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%d/%m/%Y %H:%M:%S")
    dolphie.qps_data["datetimes"].append(formatted_datetime)


class create_plot(JupyterMixin):
    def __init__(self, qps_data):
        self.decoder = AnsiDecoder()
        self.qps_data = qps_data

    def __rich_console__(self, console, options):
        self.width = options.max_width or console.width
        self.height = 15

        plt.clf()
        plt.date_form("d/m/Y H:M:S")
        plt.canvas_color((3, 9, 24))
        plt.axes_color((3, 9, 24))
        plt.ticks_color((144, 169, 223))

        plt.plotsize(self.width, self.height)

        max_y_value = 0

        for dml, dml_data in self.qps_data.items():
            if dml == "datetimes" or not dml_data["visible"]:
                continue

            dml_name = dml.split("_")[2]
            x = self.qps_data["datetimes"]
            y = dml_data["qps"]
            plt.plot(x, y, marker="braille", label=dml_name.upper(), color=dml_data["color"])

            max_y_value = max(max_y_value, max(y))

        # I create my own y ticks so it doesn't output a decimal for numbers - kinda hacky but there's no other way
        max_y_ticks = 6
        y_tick_interval = max_y_value / max_y_ticks
        y_ticks = [i * y_tick_interval for i in range(max_y_ticks + 1)]
        y_labels = [str(int(val)) for val in y_ticks]
        plt.yticks(y_ticks, y_labels)

        self.rich_canvas = Group(*self.decoder.decode(plt.build()))

        yield self.rich_canvas
