from datetime import datetime, timedelta

from dolphie.Modules.Functions import format_bytes, format_number
from dolphie.Modules.MetricManager import MetricData
from dolphie.Modules.TabManager import Tab
from rich.style import Style
from rich.table import Table


def create_panel(tab: Tab) -> Table:
    dolphie = tab.dolphie

    global_status = dolphie.global_status

    ####################
    # Host Information #
    ####################
    runtime = str(datetime.now() - dolphie.dolphie_start_time).split(".")[0]

    table_title_style = Style(color="#bbc8e8", bold=True)
    table = Table(show_header=False, box=None, title="Host Information", title_style=table_title_style)

    table.add_column()
    table.add_column(min_width=15)
    table.add_row("[label]Version", f"{dolphie.host_distro} {dolphie.host_version}")
    table.add_row("[label]Uptime", str(timedelta(seconds=global_status["ProxySQL_Uptime"])))
    table.add_row(
        "[label]MySQL",
        (
            f"{dolphie.global_variables['mysql-server_version']} "
            f"[label]Workers[/label] {global_status['MySQL_Thread_Workers']}"
        ),
    )
    table.add_row(
        "[label]Latency",
        f"[label]CP Avg[/label] {round(global_status.get('proxysql_backend_host_average_latency', 0) / 1000, 2)}ms",
    )
    if not dolphie.replay_file:
        table.add_row("[label]Runtime", f"{runtime} [dark_gray]({round(dolphie.worker_processing_time, 2)}s)")
    tab.dashboard_section_1.update(table)

    ##########################
    # Connection Information #
    ##########################
    proxysql_connections = dolphie.metric_manager.metrics.proxysql_connections

    table = Table(show_header=False, box=None, title="Connections", title_style=table_title_style)

    table.add_column()
    table.add_column(min_width=6)
    data_dict = {
        "[label]FE Connected": proxysql_connections.Client_Connections_connected.values,
        "[label]FE Non-idle": proxysql_connections.Client_Connections_non_idle.values,
        "[label]BE Connected": proxysql_connections.Server_Connections_connected.values,
        "[label]FE Created": proxysql_connections.Client_Connections_created.values,
        "[label]BE Created": proxysql_connections.Server_Connections_created.values,
    }

    fe_usage = round(
        (dolphie.global_status["Client_Connections_connected"] / dolphie.global_variables["mysql-max_connections"])
        * 100,
        2,
    )

    metric_data = dolphie.metric_manager.metrics.proxysql_multiplex_efficiency.proxysql_multiplex_efficiency_ratio
    if metric_data.values:
        if metric_data.values[-1] >= 85:
            color_code = "green"
        elif metric_data.values[-1] >= 50:
            color_code = "yellow"
        else:
            color_code = "red"

        mp_efficiency = f"[{color_code}]{metric_data.values[-1]}%[/{color_code}]"
    else:
        mp_efficiency = "N/A"

    if fe_usage >= 90:
        color_code = "red"
    elif fe_usage >= 70:
        color_code = "yellow"
    else:
        color_code = "green"

    table.add_row("[label]MP Efficiency", mp_efficiency)
    table.add_row("[label]FE Usage", f"[{color_code}]{fe_usage}%")
    table.add_row("[label]Active TRX", f"{global_status['Active_Transactions']}")
    for label, values in data_dict.items():
        if values:
            value = format_number(values[-1])
        else:
            value = 0

        if "Created" in label or "Aborted" in label or "Wrong Passwd" in label:
            table.add_row(label, f"{value}/s")
        else:
            table.add_row(label, f"{value}")

    # Reuse Innodb table for connection information
    tab.dashboard_section_2.update(table)

    ####################################
    # Query Sent/Recv Rate Information #
    ####################################
    proxysql_queries_network_data = dolphie.metric_manager.metrics.proxysql_queries_data_network

    table = Table(show_header=False, box=None, title="Query Data Rates/s", title_style=table_title_style)

    table.add_column()
    table.add_column(min_width=7)
    data_dict = {
        "[label]FE Sent": proxysql_queries_network_data.Queries_frontends_bytes_sent.values,
        "[label]BE Sent": proxysql_queries_network_data.Queries_backends_bytes_sent.values,
        "[label]FE Recv": proxysql_queries_network_data.Queries_frontends_bytes_recv.values,
        "[label]BE Recv": proxysql_queries_network_data.Queries_backends_bytes_recv.values,
    }

    for label, values in data_dict.items():
        if values:
            value = format_bytes(values[-1])
        else:
            value = 0

        if "Created" in label or "Aborted" in label or "Wrong Passwd" in label:
            table.add_row(label, f"{value}/s")
        else:
            table.add_row(label, f"{value}")

    # Reuse binary log table for connection information
    tab.dashboard_section_3.update(table)

    ###############
    # Statistics #
    ###############
    table = Table(show_header=False, box=None, title="Statistics/s", title_style=table_title_style)

    table.add_column()
    table.add_column(min_width=6)

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
            table.add_row(f"[label]{label}", format_number(metric_data.values[-1]))
        else:
            table.add_row(f"[label]{label}", "0")

    tab.dashboard_section_4.update(table)
