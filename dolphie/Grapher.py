import plotext as plt
from dolphie.Functions import format_number
from rich.ansi import AnsiDecoder
from rich.console import Group
from rich.jupyter import JupyterMixin


class create_graph(JupyterMixin):
    def __init__(self, graph_source, graph_data):
        self.graph_source = graph_source
        self.graph_data = graph_data

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

        if self.graph_source == "replica_lag":
            x = self.graph_data["datetimes"]
            y = self.graph_data["metrics"]

            if y:
                plt.plot(x, y, marker="braille", label="Lag (secs)", color=(68, 180, 255))
                max_y_value = max(max_y_value, max(y))
        elif self.graph_source == "dml_qps":
            for component, component_data in self.graph_data.items():
                if component == "datetimes" or not component_data["visible"]:
                    continue

                component_name = component.split("_")[2]
                x = self.graph_data["datetimes"]
                y = component_data["qps"]

                if y:
                    plt.plot(x, y, marker="braille", label=component_name.upper(), color=component_data["color"])
                    max_y_value = max(max_y_value, max(y))

        # I create my own y ticks to format the numbers how I like them
        max_y_ticks = 5
        y_tick_interval = max_y_value / max_y_ticks

        if y_tick_interval >= 1:
            y_ticks = [i * y_tick_interval for i in range(max_y_ticks + 1)]
            y_labels = [format_number(val, for_plot=True, decimal=1) for val in y_ticks]
        else:
            y_ticks = [i for i in range(int(max_y_value) + 1)]
            y_labels = [format_number(val, for_plot=True, decimal=1) for val in y_ticks]

        plt.yticks(y_ticks, y_labels)

        yield Group(*AnsiDecoder().decode(plt.build()))
