from __future__ import annotations

from datetime import datetime, timedelta

from dolphie.Modules.Functions import coerce_float, coerce_int, format_bytes, format_number
from dolphie.Modules.MetricDefinitions import MetricData
from dolphie.Modules.MySQL import ConnectionSource
from dolphie.Modules.TabManager import Tab
from dolphie.Modules.Theme import ThemedTable as Table
from dolphie.Panels import Replication as ReplicationPanel


def _format_utilization_percent(
    value: float,
    thresholds: tuple[float, float] = (80, 90),
    colors: tuple[str, str, str] = ("$green", "$yellow", "$red"),
) -> str:
    if value > thresholds[1]:
        color = colors[2]
    elif value > thresholds[0]:
        color = colors[1]
    else:
        color = colors[0]
    return f"[{color}]{value}%[/{color}]"


def create_panel(tab: Tab) -> None:
    dolphie = tab.dolphie

    global_status = dolphie.global_status
    global_variables = dolphie.global_variables
    binlog_status = dolphie.binlog_status

    table_title_style = "b_light_blue"

    ####################
    # Host Information #
    ####################
    table_information = Table(
        show_header=False,
        box=None,
        title=f"{dolphie.panels.dashboard.formatted_key}Host Information",
        title_style=table_title_style,
    )

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
        host_type = "MariaDB" if dolphie.connection_source_alt == ConnectionSource.mariadb else "MySQL"

    replicas = dolphie.replica_manager.discovery_count

    table_information.add_column()
    table_information.add_column(min_width=25, max_width=35)
    table_information.add_row("[$label]Version", f"{dolphie.host_distro} {dolphie.host_version}")
    if global_variables.get("version_compile_os") and global_variables.get("version_compile_machine"):
        table_information.add_row(
            "[$label]",
            f"{global_variables['version_compile_os']} ({global_variables['version_compile_machine']})",
        )
    table_information.add_row("[$label]Type", host_type)

    if dolphie.galera_cluster or dolphie.group_replication or dolphie.innodb_cluster:
        if dolphie.galera_cluster:
            nodes = dolphie.global_status.get("wsrep_cluster_size", 0)
            node_state = dolphie.global_status.get("wsrep_local_state_comment", "N/A")
            if node_state == "Synced":
                state_colored = f"[$green]{node_state}[/$green]"
            elif node_state in ("Donor/Desynced", "Donor"):
                state_colored = f"[$yellow]{node_state}[/$yellow]"
            else:
                state_colored = f"[$red]{node_state}[/$red]"
            node_value = f"{nodes} ({state_colored})"
        else:
            nodes = len(dolphie.group_replication_members)
            node_value = str(nodes)
        if replicas:
            node_value += f" [$label]Replicas[/$label] {replicas}"
        table_information.add_row("[$label]Nodes", node_value)
    else:
        table_information.add_row("[$label]Replicas", str(replicas))

    table_information.add_row(
        "[$label]Threads",
        f"[$label]con[/$label] {format_number(global_status['Threads_connected'])}"
        f"[$highlight]/[/$highlight][$label]run[/$label] {format_number(global_status['Threads_running'])}"
        f"[$highlight]/[/$highlight][$label]cac[/$label] {format_number(global_status['Threads_cached'])}",
    )
    table_information.add_row(
        "[$label]Tables",
        f"[$label]open[/$label] {format_number(global_status['Open_tables'])}"
        f"[$highlight]/[/$highlight][$label]opened[/$label] {format_number(global_status['Opened_tables'])}",
    )
    table_information.add_row("[$label]Uptime", str(timedelta(seconds=coerce_float(global_status["Uptime"]))))

    if not dolphie.replay_file:
        runtime = str(datetime.now().astimezone() - dolphie.dolphie_start_time).split(".")[0]
        table_information.add_row(
            "[$label]Runtime",
            f"{runtime} [$label]Latency[/$label] {round(dolphie.worker_processing_time, 2)}s",
        )
    else:
        if dolphie.worker_processing_time:
            table_information.add_row("[$label]Latency", f"{round(dolphie.worker_processing_time, 2)}s")

    tab.dashboard_section_1.update(table_information)

    ######################
    # System Utilization #
    ######################
    table = create_system_utilization_table(tab)

    if table:
        tab.dashboard_section_6.update(table)

    ###########
    # InnoDB  #
    ###########
    table_innodb = Table(show_header=False, box=None, title="InnoDB", title_style=table_title_style)

    table_innodb.add_column()
    table_innodb.add_column(width=9)

    # Calculate InnoDB memory read hit efficiency
    ib_pool_disk_reads = coerce_float(global_status.get("Innodb_buffer_pool_reads"))
    ib_pool_mem_reads = coerce_float(
        global_status.get("Innodb_buffer_pool_read_requests", 1),
        1,
    )  # Default to 1 to avoid division by zero

    if ib_pool_disk_reads >= ib_pool_mem_reads:
        innodb_efficiency = "[$red]0.00%"
    else:
        efficiency = 100 - (ib_pool_disk_reads / ib_pool_mem_reads * 100)

        if efficiency > 90:
            color_code = "$green"
        elif efficiency > 80:
            color_code = "$yellow"
        else:
            color_code = "$red"

        innodb_efficiency = f"[{color_code}]{efficiency:.2f}%"

    # Add data to our table
    table_innodb.add_row("[$label]Read Hit", innodb_efficiency)
    table_innodb.add_row(
        "[$label]Chkpt Age",
        dolphie.metric_manager.get_formatted_checkpoint_age(),
    )
    table_innodb.add_row("[$label]AHI Hit", dolphie.metric_manager.get_formatted_ahi_status())

    bp_instances = coerce_int(global_variables.get("innodb_buffer_pool_instances"), 1)
    plural = "s" if bp_instances > 1 else ""
    table_innodb.add_row(f"[$label]BP Instance{plural}", format_number(bp_instances))

    table_innodb.add_row("[$label]BP Size", format_bytes(global_variables["innodb_buffer_pool_size"]))
    table_innodb.add_row(
        "[$label]BP Available",
        format_bytes(
            coerce_int(global_variables["innodb_buffer_pool_size"])
            - coerce_int(global_status["Innodb_buffer_pool_bytes_data"])
        ),
    )
    table_innodb.add_row("[$label]BP Dirty", format_bytes(global_status["Innodb_buffer_pool_bytes_dirty"]))
    table_innodb.add_row(
        "[$label]History List",
        format_number(dolphie.innodb_metrics.get("trx_rseg_history_len", "N/A")),
    )

    tab.dashboard_section_2.update(table_innodb)

    ##############
    # Binary Log #
    ##############
    table_primary = Table(show_header=False, box=None, title="Binary Log", title_style=table_title_style)

    if global_variables.get("log_bin") == "OFF" or not binlog_status or not binlog_status.get("File"):
        tab.dashboard_section_3.display = False
    else:
        tab.dashboard_section_3.display = True
        table_primary.add_column()
        table_primary.add_column(max_width=40)

        binlog_cache = 100
        binlog_cache_disk = coerce_float(global_status["Binlog_cache_disk_use"])
        binlog_cache_mem = coerce_float(global_status["Binlog_cache_use"])
        if binlog_cache_disk and binlog_cache_mem:
            if binlog_cache_disk >= binlog_cache_mem:
                binlog_cache = 0
            else:
                binlog_cache = round(100 - (binlog_cache_disk / binlog_cache_mem * 100), 2)

        table_primary.add_row("[$label]File name", binlog_status["File"])
        table_primary.add_row(
            "[$label]Position",
            str(binlog_status["Position"]),
        )
        table_primary.add_row(
            "[$label]Size",
            format_bytes(coerce_int(binlog_status["Position"])),
        )
        diff_position = binlog_status.get("Diff_Position", 0)
        if not isinstance(diff_position, (int, str)):
            diff_position = 0
        table_primary.add_row("[$label]Diff", format_bytes(diff_position))
        table_primary.add_row("[$label]Cache Hit", f"{binlog_cache}%")

        binlog_format = global_variables.get("binlog_format", "N/A")
        if binlog_format == "ROW":
            binlog_row_image = global_variables.get("binlog_row_image", "N/A")
            table_primary.add_row("[$label]Format", f"{binlog_format} ({binlog_row_image})")
        else:
            table_primary.add_row("[$label]Format", binlog_format)

        if dolphie.connection_source_alt == ConnectionSource.mariadb:
            table_primary.add_row("[$label]GTID Strict", global_variables.get("gtid_strict_mode", "N/A"))
            table_primary.add_row("[$label]Encrypt", global_variables.get("encrypt_binlog", "N/A"))
        else:
            table_primary.add_row("[$label]GTID", global_variables.get("gtid_mode", "N/A"))
            table_primary.add_row(
                "[$label]Compression",
                global_variables.get("binlog_transaction_compression", "N/A"),
            )

        tab.dashboard_section_3.update(table_primary)

    ###############
    # Replication #
    ###############
    replication_channels = ReplicationPanel.user_replication_channels(dolphie.replication_status)
    if replication_channels and not dolphie.panels.replication.visible:
        tab.dashboard_section_5.display = True
        # Surface stopped, errored, or unknown channels before ordinary numeric lag.
        priority_channel = max(replication_channels, key=ReplicationPanel.replication_channel_priority)
        is_multi_source = len(replication_channels) > 1
        tab.dashboard_section_5.update(
            ReplicationPanel.create_replication_table(
                tab, dashboard_table=True, channel_data=priority_channel, show_channel_name=is_multi_source
            )
        )
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

        latest_value = metric_data.latest_value()
        if latest_value is not None:
            table_stats.add_row(f"[$label]{label}", format_number(latest_value))
        else:
            table_stats.add_row(f"[$label]{label}", "0")

    tab.dashboard_section_4.update(table_stats)


def create_system_utilization_table(tab: Tab) -> Table | None:
    dolphie = tab.dolphie

    system_utilization = dolphie.system_utilization
    if not system_utilization:
        return None

    table = Table(
        show_header=False,
        box=None,
        title="System Utilization",
        title_style="b_light_blue",
    )
    table.add_column()
    table.add_column(min_width=18, max_width=25)

    # Uptime
    uptime = system_utilization.get("Uptime", "N/A")
    table.add_row(
        "[$label]Uptime",
        str(timedelta(seconds=coerce_float(uptime))) if uptime != "N/A" else "N/A",
    )

    # CPU
    cpu_percent = dolphie.metric_manager.metrics.system_cpu.CPU_Percent.latest_value()
    if cpu_percent is not None:
        cpu_percent = round(cpu_percent, 2)
        formatted_cpu_percent = _format_utilization_percent(cpu_percent)
        cpu_cores = system_utilization.get("CPU_Count", "N/A")
        table.add_row("[$label]CPU", f"{formatted_cpu_percent} [$label]cores[/$label] {cpu_cores}")
    else:
        table.add_row("[$label]CPU", "N/A")

    # CPU Load
    load_averages = system_utilization.get("CPU_Load_Avg")
    if isinstance(load_averages, (list, tuple)):
        formatted_load = " ".join(f"{coerce_float(avg):.2f}" for avg in load_averages)
        table.add_row("[$label]Load", formatted_load)

    # Memory
    memory_used = dolphie.metric_manager.metrics.system_memory.Memory_Used.last_value
    memory_total = dolphie.metric_manager.metrics.system_memory.Memory_Total.last_value
    if memory_used and memory_total:
        memory_percent_used = round((memory_used / memory_total) * 100, 2)
        formatted_memory_percent_used = _format_utilization_percent(memory_percent_used)
        table.add_row(
            "[$label]Memory",
            (
                f"{formatted_memory_percent_used}\n{format_bytes(memory_used)}"
                f"[$dark_gray]/[/$dark_gray]{format_bytes(memory_total)}"
            ),
        )
    else:
        table.add_row("[$label]Memory", "N/A\n")

    # Swap
    swap_used_value = system_utilization.get("Swap_Used")
    swap_total_value = system_utilization.get("Swap_Total")
    swap_used = format_bytes(coerce_float(swap_used_value)) if swap_used_value is not None else "N/A"
    swap_total = format_bytes(coerce_float(swap_total_value)) if swap_total_value is not None else "N/A"
    table.add_row("[$label]Swap", f"{swap_used}[$dark_gray]/[/$dark_gray]{swap_total}")

    # Disk I/O
    disk_read = dolphie.metric_manager.metrics.system_disk_io.Disk_Read.latest_value()
    disk_write = dolphie.metric_manager.metrics.system_disk_io.Disk_Write.latest_value()
    if disk_read is not None and disk_write is not None:
        table.add_row(
            "[$label]Disk",
            f"[$label]IOPS R[/$label] {format_number(disk_read)}\n[$label]IOPS W[/$label] {format_number(disk_write)}",
        )
    else:
        table.add_row("[$label]Disk", "[$label]IOPS R[/$label] N/A\n[$label]IOPS W[/$label] N/A")

    return table
