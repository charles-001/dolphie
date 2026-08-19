from datetime import datetime, timedelta

from dolphie.Modules.Functions import coerce_float, format_bytes, format_number
from dolphie.Modules.MetricDefinitions import MetricData
from dolphie.Modules.TabManager import Tab
from dolphie.Modules.Theme import ThemedTable as Table
from dolphie.Panels.Dashboard import create_system_utilization_table


def create_panel(tab: Tab) -> None:
    dolphie = tab.dolphie

    global_status = dolphie.global_status
    global_variables = dolphie.global_variables
    metric_manager = dolphie.metric_manager

    ####################
    # Host Information #
    ####################
    runtime = str(datetime.now().astimezone() - dolphie.dolphie_start_time).split(".")[0]

    table_title_style = "b_light_blue"
    table = Table(
        show_header=False,
        box=None,
        title=f"{dolphie.panels.dashboard.formatted_key}Host Information",
        title_style=table_title_style,
    )

    table.add_column()
    table.add_column(min_width=15)
    table.add_row("[$label]Version", f"{dolphie.host_distro} {dolphie.host_version}")
    table.add_row(
        "[$label]Uptime",
        str(timedelta(seconds=coerce_float(global_status["ProxySQL_Uptime"]))),
    )
    table.add_row(
        "[$label]MySQL",
        (
            f"{global_variables['mysql-server_version']} "
            f"[$label]Workers[/$label] {global_status['MySQL_Thread_Workers']}"
        ),
    )
    if not dolphie.replay_file:
        table.add_row("[$label]Runtime", runtime)

    if dolphie.worker_processing_time:
        table.add_row("[$label]Latency", f"{round(dolphie.worker_processing_time, 2)}s")

    tab.dashboard_section_1.update(table)

    ######################
    # System Utilization #
    ######################
    table = create_system_utilization_table(tab)

    if table:
        tab.dashboard_section_6.update(table)

    ##########################
    # Connection Information #
    ##########################
    proxysql_connections = metric_manager.metrics.proxysql_connections

    table = Table(show_header=False, box=None, title="Connections", title_style=table_title_style)

    table.add_column()
    table.add_column(min_width=6)
    data_dict = {
        "[$label]FE Connected": proxysql_connections.Client_Connections_connected.latest_value(),
        "[$label]FE Non-idle": proxysql_connections.Client_Connections_non_idle.latest_value(),
        "[$label]BE Connected": proxysql_connections.Server_Connections_connected.latest_value(),
        "[$label]FE Created": proxysql_connections.Client_Connections_created.latest_value(),
        "[$label]BE Created": proxysql_connections.Server_Connections_created.latest_value(),
    }

    max_connections = coerce_float(global_variables["mysql-max_connections"])
    fe_usage = (
        round(coerce_float(global_status["Client_Connections_connected"]) / max_connections * 100, 2)
        if max_connections > 0
        else 0
    )

    metric_data = metric_manager.metrics.proxysql_multiplex_efficiency.proxysql_multiplex_efficiency_ratio
    latest_efficiency = metric_data.latest_value()
    if latest_efficiency is not None:
        if latest_efficiency >= 85:
            color_code = "$green"
        elif latest_efficiency >= 50:
            color_code = "$yellow"
        else:
            color_code = "$red"

        mp_efficiency = f"[{color_code}]{latest_efficiency}%[/{color_code}]"
    else:
        mp_efficiency = "N/A"

    if fe_usage >= 90:
        color_code = "$red"
    elif fe_usage >= 70:
        color_code = "$yellow"
    else:
        color_code = "$green"

    table.add_row("[$label]MP Efficiency", mp_efficiency)
    table.add_row("[$label]FE Usage", f"[{color_code}]{fe_usage}%")
    table.add_row("[$label]Active TRX", f"{global_status['Active_Transactions']}")
    for label, latest_value in data_dict.items():
        value = format_number(latest_value) if latest_value is not None else 0

        if "Created" in label or "Aborted" in label or "Wrong Passwd" in label:
            table.add_row(label, f"{value}/s")
        else:
            table.add_row(label, f"{value}")

    # Reuse Innodb table for connection information
    tab.dashboard_section_2.update(table)

    ####################################
    # Query Sent/Recv Rate Information #
    ####################################
    proxysql_queries_network_data = metric_manager.metrics.proxysql_queries_data_network

    table = Table(
        show_header=False,
        box=None,
        title="Query Data Rates/s",
        title_style=table_title_style,
    )

    table.add_column()
    table.add_column(min_width=9)
    data_dict = {
        "[$label]FE Sent": proxysql_queries_network_data.Queries_frontends_bytes_sent.latest_value(),
        "[$label]BE Sent": proxysql_queries_network_data.Queries_backends_bytes_sent.latest_value(),
        "[$label]FE Recv": proxysql_queries_network_data.Queries_frontends_bytes_recv.latest_value(),
        "[$label]BE Recv": proxysql_queries_network_data.Queries_backends_bytes_recv.latest_value(),
    }

    for label, latest_value in data_dict.items():
        value = format_bytes(latest_value) if latest_value is not None else 0

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
    table.add_column(min_width=7)

    # Add DML statistics
    metrics = metric_manager.metrics.dml
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

        latest_value = metric_data.latest_value()
        if latest_value is not None:
            table.add_row(f"[$label]{label}", format_number(latest_value))
        else:
            table.add_row(f"[$label]{label}", "0")

    tab.dashboard_section_4.update(table)
