from datetime import datetime, timedelta

from dolphie.Modules.Functions import format_number
from dolphie.Modules.MetricManager import MetricData
from dolphie.Modules.TabManager import Tab
from rich.style import Style
from rich.table import Table


def create_panel(tab: Tab) -> Table:
    dolphie = tab.dolphie

    global_status = dolphie.global_status

    runtime = str(datetime.now() - dolphie.dolphie_start_time).split(".")[0]

    table_title_style = Style(color="#bbc8e8", bold=True)
    table_information = Table(show_header=False, box=None, title="Host Information", title_style=table_title_style)

    table_information.add_column()
    table_information.add_column(min_width=25, max_width=27)
    table_information.add_row("[label]Version", f"{dolphie.host_distro} {dolphie.host_version}")
    table_information.add_row("[label]Uptime", str(timedelta(seconds=global_status["ProxySQL_Uptime"])))
    table_information.add_row("[label]Runtime", f"{runtime} [label]Latency[/label] {dolphie.refresh_latency}s")
    table_information.add_row("[label]Active TRX", f"{global_status['Active_Transactions']}")
    # table_information.add_row(
    #     "[label]Threads",
    #     "[label]con[/label] %s[highlight]/[/highlight][label]run[/label]"
    #     " %s[highlight]/[/highlight][label]cac[/label] %s"
    #     % (
    #         format_number(global_status["Threads_connected"]),
    #         format_number(global_status["Threads_running"]),
    #         format_number(global_status["Threads_cached"]),
    #     ),
    # )
    # table_information.add_row(
    #     "[label]Tables",
    #     "[label]open[/label] %s[highlight]/[/highlight][label]opened[/label] %s"
    #     % (
    #         format_number(global_status["Open_tables"]),
    #         format_number(global_status["Opened_tables"]),
    #     ),
    # )

    tab.dashboard_host_information.update(table_information)

    ###############
    # Statistics #
    ###############
    table_stats = Table(show_header=False, box=None, title="Statistics/s", title_style=table_title_style)

    table_stats.add_column()
    table_stats.add_column(min_width=6)

    # Add DML statistics
    metrics = dolphie.metric_manager.metrics.dml
    metric_labels = {
        "Queries": "Queries",
        "SELECT": "Com_select",
        "INSERT": "Com_insert",
        "UPDATE": "Com_update",
        "DELETE": "Com_delete",
        "REPLACE": "Com_replace",
        "COMMIT": "Com_commit",
        "ROLLBACK": "Com_rollback",
    }

    for label, metric_name in metric_labels.items():
        metric_data: MetricData = getattr(metrics, metric_name)

        if metric_data.values:
            table_stats.add_row(f"[label]{label}", format_number(metric_data.values[-1]))
        else:
            table_stats.add_row(f"[label]{label}", "0")

    tab.dashboard_statistics.update(table_stats)
