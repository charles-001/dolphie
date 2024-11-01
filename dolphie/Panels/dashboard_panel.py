from datetime import datetime, timedelta

from rich.style import Style
from rich.table import Table

from dolphie.Modules.Functions import format_bytes, format_number
from dolphie.Modules.MetricManager import MetricData
from dolphie.Modules.MySQL import ConnectionSource
from dolphie.Modules.TabManager import Tab
from dolphie.Panels import replication_panel


def create_panel(tab: Tab) -> Table:
    dolphie = tab.dolphie

    global_status = dolphie.global_status
    global_variables = dolphie.global_variables
    binlog_status = dolphie.binlog_status

    table_title_style = Style(color="#bbc8e8", bold=True)

    ####################
    # Host Information #
    ####################
    table_information = Table(show_header=False, box=None, title="Host Information", title_style=table_title_style)

    if dolphie.replicaset:
        host_type = "InnoDB ReplicaSet"
    elif dolphie.innodb_cluster_read_replica:
        host_type = "InnoDB Cluster Read Replica"
    elif dolphie.innodb_cluster:
        host_type = "InnoDB Cluster"
    elif dolphie.group_replication:
        host_type = "Group Replication"
    elif dolphie.galera_cluster:
        host_type = "Galera Cluster"
    else:
        if dolphie.connection_source_alt == ConnectionSource.mariadb:
            host_type = "MariaDB"
        else:
            host_type = "MySQL"

    replicas = 0
    if dolphie.replica_manager.available_replicas:
        replicas = len(dolphie.replica_manager.available_replicas)

    table_information.add_column()
    table_information.add_column(min_width=25, max_width=27)
    table_information.add_row("[label]Version", f"{dolphie.host_distro} {dolphie.host_version}")
    table_information.add_row(
        "[label]", "%s (%s)" % (global_variables["version_compile_os"], global_variables["version_compile_machine"])
    )
    table_information.add_row("[label]Type", host_type)
    table_information.add_row("[label]Uptime", str(timedelta(seconds=global_status["Uptime"])))
    table_information.add_row("[label]Replicas", "%s" % replicas)
    table_information.add_row(
        "[label]Threads",
        "[label]con[/label] %s[highlight]/[/highlight][label]run[/label]"
        " %s[highlight]/[/highlight][label]cac[/label] %s"
        % (
            format_number(global_status["Threads_connected"]),
            format_number(global_status["Threads_running"]),
            format_number(global_status["Threads_cached"]),
        ),
    )
    table_information.add_row(
        "[label]Tables",
        "[label]open[/label] %s[highlight]/[/highlight][label]opened[/label] %s"
        % (
            format_number(global_status["Open_tables"]),
            format_number(global_status["Opened_tables"]),
        ),
    )
    if not dolphie.replay_file:
        runtime = str(datetime.now() - dolphie.dolphie_start_time).split(".")[0]
        table_information.add_row(
            "[label]Runtime", f"{runtime} [dark_gray]({round(dolphie.worker_processing_time, 2)}s)"
        )

    tab.dashboard_section_1.update(table_information)

    ######################
    # System Utilization #
    ######################
    table = create_system_utilization_table(tab)

    if table:
        tab.dashboard_section_6.update(create_system_utilization_table(tab))

    ###########
    # InnoDB  #
    ###########
    table_innodb = Table(show_header=False, box=None, title="InnoDB", title_style=table_title_style)

    table_innodb.add_column()
    table_innodb.add_column(width=9)

    # Calculate InnoDB memory read hit efficiency
    ib_pool_disk_reads = global_status.get("Innodb_buffer_pool_reads", 0)
    ib_pool_mem_reads = global_status.get(
        "Innodb_buffer_pool_read_requests", 1
    )  # Default to 1 to avoid division by zero

    if ib_pool_disk_reads >= ib_pool_mem_reads:
        innodb_efficiency = "[red]0.00%"
    else:
        efficiency = 100 - (ib_pool_disk_reads / ib_pool_mem_reads * 100)

        if efficiency > 90:
            color_code = "green"
        elif efficiency > 80:
            color_code = "yellow"
        else:
            color_code = "red"

        innodb_efficiency = f"[{color_code}]{efficiency:.2f}%"

    # Add data to our table
    table_innodb.add_row("[label]Read Hit", innodb_efficiency)
    table_innodb.add_row("[label]Chkpt Age", dolphie.metric_manager.get_metric_checkpoint_age(format=True))
    table_innodb.add_row("[label]AHI Hit", dolphie.metric_manager.get_metric_adaptive_hash_index())

    bp_instances = global_variables.get("innodb_buffer_pool_instances", 1)
    plural = "s" if bp_instances > 1 else ""
    table_innodb.add_row(f"[label]BP Instance{plural}", format_number(bp_instances))

    table_innodb.add_row("[label]BP Size", format_bytes(global_variables["innodb_buffer_pool_size"]))
    table_innodb.add_row(
        "[label]BP Available",
        format_bytes(
            dolphie.global_variables["innodb_buffer_pool_size"] - dolphie.global_status["Innodb_buffer_pool_bytes_data"]
        ),
    )
    table_innodb.add_row("[label]BP Dirty", format_bytes(global_status["Innodb_buffer_pool_bytes_dirty"]))
    table_innodb.add_row(
        "[label]History List", format_number(dolphie.innodb_metrics.get("trx_rseg_history_len", "N/A"))
    )

    tab.dashboard_section_2.update(table_innodb)

    ##############
    # Binary Log #
    ##############
    table_primary = Table(show_header=False, box=None, title="Binary Log", title_style=table_title_style)

    if global_variables.get("log_bin") == "OFF" or not binlog_status.get("File"):
        table_primary.add_column(justify="center")
        table_primary.add_row("\n\n\n[b][label]Disabled")
    else:
        table_primary.add_column()
        table_primary.add_column(max_width=40)

        binlog_cache = 100
        binlog_cache_disk = global_status["Binlog_cache_disk_use"]
        binlog_cache_mem = global_status["Binlog_cache_use"]
        if binlog_cache_disk and binlog_cache_mem:
            if binlog_cache_disk >= binlog_cache_mem:
                innodb_efficiency = "[red]0.00%"
            elif binlog_cache_mem > binlog_cache_disk:
                binlog_cache = round(100 - (binlog_cache_disk / binlog_cache_mem), 2)

        table_primary.add_row("[label]File name", binlog_status["File"])
        table_primary.add_row(
            "[label]Position",
            "%s" % (str(binlog_status["Position"])),
        )
        table_primary.add_row(
            "[label]Size",
            "%s" % format_bytes(binlog_status["Position"]),
        )
        table_primary.add_row("[label]Diff", format_bytes(binlog_status["Diff_Position"]))
        table_primary.add_row("[label]Cache Hit", f"{binlog_cache}%")

        binlog_format = global_variables.get("binlog_format", "N/A")
        binlog_row_image = None
        if binlog_format == "ROW":
            binlog_row_image = global_variables.get("binlog_row_image", "N/A")
            table_primary.add_row("[label]Format", "{} ({})".format(binlog_format, binlog_row_image))
        else:
            table_primary.add_row("[label]Format", binlog_format, binlog_row_image)

        if dolphie.connection_source_alt == ConnectionSource.mariadb:
            table_primary.add_row("[label]Encrypt", global_variables.get("encrypt_binlog", "N/A"))
        else:
            table_primary.add_row("[label]GTID", global_variables.get("gtid_mode", "N/A"))
            table_primary.add_row("[label]Compression", global_variables.get("binlog_transaction_compression", "N/A"))

    tab.dashboard_section_3.update(table_primary)

    ###############
    # Replication #
    ###############
    if dolphie.replication_status and not dolphie.panels.replication.visible:
        tab.dashboard_section_5.display = True
        tab.dashboard_section_5.update(replication_panel.create_replication_table(tab, dashboard_table=True))
    else:
        tab.dashboard_section_5.display = False
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

    tab.dashboard_section_4.update(table_stats)


def create_system_utilization_table(tab: Tab) -> Table:
    dolphie = tab.dolphie

    table_system_utilization = None
    if dolphie.system_utilization:
        table_system_utilization = Table(
            show_header=False, box=None, title="System Utilization", title_style=Style(color="#bbc8e8", bold=True)
        )
        table_system_utilization.add_column()
        table_system_utilization.add_column(min_width=18, max_width=25)

        table_system_utilization.add_row("Uptime", str(timedelta(seconds=dolphie.system_utilization.get("Uptime"))))

        cpu_percent_values = dolphie.metric_manager.metrics.system_cpu.CPU_Percent.values
        if cpu_percent_values:
            cpu_percent = round(cpu_percent_values[-1], 2)
            if cpu_percent > 90:
                formatted_cpu_percent = f"[red]{cpu_percent}%[/red]"
            elif cpu_percent > 80:
                formatted_cpu_percent = f"[yellow]{cpu_percent}%[/yellow]"
            else:
                formatted_cpu_percent = f"[green]{cpu_percent}%[/green]"
        else:
            formatted_cpu_percent = "[dark_gray]N/A[/dark_gray]"
        table_system_utilization.add_row(
            "[label]CPU", f"{formatted_cpu_percent} [label]cores[/label] {dolphie.system_utilization.get('CPU_Count')}"
        )

        load_averages = dolphie.system_utilization.get("CPU_Load_Avg")
        if load_averages:
            load_1, load_5, load_15 = load_averages
            formatted_load = f"{load_1:.2f} {load_5:.2f} {load_15:.2f}"
            table_system_utilization.add_row("[label]Load", formatted_load)
        else:
            table_system_utilization.add_row("[label]Load", "[dark_gray]N/A[/dark_gray]")

        memory_used = dolphie.metric_manager.metrics.system_memory.Memory_Used.last_value
        memory_total = dolphie.metric_manager.metrics.system_memory.Memory_Total.last_value
        if memory_used and memory_total:
            memory_percent_used = round((memory_used / memory_total) * 100, 2)
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
                    f"{format_bytes(memory_used)}"
                    f"[dark_gray]/[/dark_gray]"
                    f"{format_bytes(memory_total)}"
                ),
            )
        else:
            table_system_utilization.add_row("[label]Memory", "[dark_gray]N/A[/dark_gray]")

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
            table_system_utilization.add_row("[label]Network", "[dark_gray]N/A[/dark_gray]")

    return table_system_utilization
