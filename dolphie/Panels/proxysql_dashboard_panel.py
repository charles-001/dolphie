from datetime import datetime, timedelta

from rich.style import Style
from rich.table import Table

from dolphie.Modules.Functions import format_bytes, format_number
from dolphie.Modules.MetricManager import MetricData
from dolphie.Modules.TabManager import Tab


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

    ##################
    # System Metrics #
    ##################
    if dolphie.system_utilization:
        table_system_utilization = Table(
            show_header=False, box=None, title="System Utilization", title_style=table_title_style
        )
        table_system_utilization.add_column()
        table_system_utilization.add_column(min_width=18, max_width=25)

        table_system_utilization.add_row("Uptime", str(timedelta(seconds=dolphie.system_utilization.get("Uptime"))))

        cpu_percent = dolphie.metric_manager.metrics.system_cpu.CPU_Percent.last_value
        if cpu_percent:
            cpu_percent = round(cpu_percent, 2)
            if cpu_percent > 90:
                formatted_cpu_percent = f"[red]{cpu_percent}%[/red]"
            elif cpu_percent > 80:
                formatted_cpu_percent = f"[yellow]{cpu_percent}%[/yellow]"
            else:
                formatted_cpu_percent = f"[green]{cpu_percent}%[/green]"
        else:
            formatted_cpu_percent = "N/A"
        table_system_utilization.add_row(
            "[label]CPU", f"{formatted_cpu_percent} [label]cores[/label] {dolphie.system_utilization.get('CPU_Count')}"
        )

        load_averages = dolphie.system_utilization.get("CPU_Load_Avg")
        if load_averages:
            load_1, load_5, load_15 = load_averages
            formatted_load = f"{load_1:.2f} {load_5:.2f} {load_15:.2f}"
            table_system_utilization.add_row("[label]Load", formatted_load)
        else:
            table_system_utilization.add_row("[label]Load", "N/A")

        memory_percent_used = dolphie.system_utilization.get("Memory_Percent_Used")
        if dolphie.metric_manager.metrics.system_memory.Memory_Used.last_value:
            memory_percent_used = round(memory_percent_used, 2)
            if memory_percent_used > 90:
                formatted_memory_percent_used = f"[red]{memory_percent_used}%[/red]"
            elif memory_percent_used > 80:
                formatted_memory_percent_used = f"[yellow]{memory_percent_used}%[/yellow]"
            else:
                formatted_memory_percent_used = f"[green]{memory_percent_used}%[/green]"

            table_system_utilization.add_row(
                "[label]Memory",
                (
                    f"{formatted_memory_percent_used}\n"
                    f"{format_bytes(dolphie.metric_manager.metrics.system_memory.Memory_Used.last_value)}"
                    f"[dark_gray]/[/dark_gray]"
                    f"{format_bytes(dolphie.metric_manager.metrics.system_memory.Memory_Total.last_value)}"
                ),
            )
        else:
            table_system_utilization.add_row("[label]Memory", "N/A")

        table_system_utilization.add_row(
            "[label]Swap",
            (
                f"{format_bytes(dolphie.system_utilization.get('Swap_Used'))}"
                f"[dark_gray]/[/dark_gray]"
                f"{format_bytes(dolphie.system_utilization.get('Swap_Total'))}"
            ),
        )

        network_down_values = dolphie.metric_manager.metrics.system_network.Network_Down.values
        network_up_values = dolphie.metric_manager.metrics.system_network.Network_Up.values
        if network_down_values and network_up_values:
            # Check if the lists have elements before accessing the last element
            if network_down_values and network_up_values:
                last_network_down = format_bytes(network_down_values[-1])
                last_network_up = format_bytes(network_up_values[-1])
            else:
                last_network_down = "0"
                last_network_up = "0"

            # Add row to table with the network metrics
            table_system_utilization.add_row(
                "[label]Network",
                (f"[label]Dn[/label] {last_network_down}\n[label]Up[/label] {last_network_up}"),
            )
        else:
            table_system_utilization.add_row("[label]Network", "N/A")

        tab.dashboard_section_6.update(table_system_utilization)

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
