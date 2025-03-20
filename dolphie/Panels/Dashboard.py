from datetime import datetime, timedelta

from rich.style import Style
from rich.table import Table

from dolphie.Modules.Functions import format_bytes, format_number
from dolphie.Modules.MetricManager import MetricData
from dolphie.Modules.MySQL import ConnectionSource
from dolphie.Modules.TabManager import Tab
from dolphie.Panels import Replication as ReplicationPanel


def create_panel(tab: Tab) -> Table:
    dolphie = tab.dolphie

    global_status = dolphie.global_status
    global_variables = dolphie.global_variables
    binlog_status = dolphie.binlog_status

    table_title_style = Style(color="#bbc8e8", bold=True)

    ####################
    # Host Information #
    ####################
    table_information = Table(
        show_header=False,
        box=None,
        title=f"{dolphie.panels.get_key(dolphie.panels.dashboard.name)}Host Information",
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
        if dolphie.connection_source_alt == ConnectionSource.mariadb:
            host_type = "MariaDB"
        else:
            host_type = "MySQL"

    replicas = 0
    if dolphie.replica_manager.available_replicas:
        replicas = len(dolphie.replica_manager.available_replicas)

    table_information.add_column()
    table_information.add_column(min_width=25, max_width=35)
    table_information.add_row("[label]Version", f"{dolphie.host_distro} {dolphie.host_version}")
    if global_variables.get("version_compile_os") and global_variables.get("version_compile_machine"):
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
            "[label]Runtime", f"{runtime} [label]Latency[/label] {round(dolphie.worker_processing_time, 2)}s"
        )
    else:
        if dolphie.worker_processing_time:
            table_information.add_row("[label]Latency", f"{round(dolphie.worker_processing_time, 2)}s")

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
        tab.dashboard_section_3.display = False
    else:
        tab.dashboard_section_3.display = True
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
        tab.dashboard_section_5.update(ReplicationPanel.create_replication_table(tab, dashboard_table=True))
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

    if not dolphie.system_utilization:
        return None

    table = Table(
        show_header=False, box=None, title="System Utilization", title_style=Style(color="#bbc8e8", bold=True)
    )
    table.add_column()
    table.add_column(min_width=18, max_width=25)

    def format_percent(value, thresholds=(80, 90), colors=("green", "yellow", "red")):
        if value > thresholds[1]:
            return f"[{colors[2]}]{value}%[/{colors[2]}]"
        elif value > thresholds[0]:
            return f"[{colors[1]}]{value}%[/{colors[1]}]"
        return f"[{colors[0]}]{value}%[/{colors[0]}]"

    # Uptime
    uptime = dolphie.system_utilization.get("Uptime", "N/A")
    table.add_row("[label]Uptime", str(timedelta(seconds=uptime)) if uptime != "N/A" else "N/A")

    # CPU
    cpu_percent_values = dolphie.metric_manager.metrics.system_cpu.CPU_Percent.values
    if cpu_percent_values:
        cpu_percent = round(cpu_percent_values[-1], 2)
        formatted_cpu_percent = format_percent(cpu_percent)
        cpu_cores = dolphie.system_utilization.get("CPU_Count", "N/A")
        table.add_row("[label]CPU", f"{formatted_cpu_percent} [label]cores[/label] {cpu_cores}")
    else:
        table.add_row("[label]CPU", "N/A")

    # CPU Load
    load_averages = dolphie.system_utilization.get("CPU_Load_Avg")
    if load_averages:
        formatted_load = " ".join(f"{avg:.2f}" for avg in load_averages)
        table.add_row("[label]Load", formatted_load)

    # Memory
    memory_used = dolphie.metric_manager.metrics.system_memory.Memory_Used.last_value
    memory_total = dolphie.metric_manager.metrics.system_memory.Memory_Total.last_value
    if memory_used and memory_total:
        memory_percent_used = round((memory_used / memory_total) * 100, 2)
        formatted_memory_percent_used = format_percent(memory_percent_used)
        table.add_row(
            "[label]Memory",
            (
                f"{formatted_memory_percent_used}\n{format_bytes(memory_used)}"
                f"[dark_gray]/[/dark_gray]{format_bytes(memory_total)}"
            ),
        )
    else:
        table.add_row("[label]Memory", "N/A\n")

    # Swap
    swap_used = format_bytes(dolphie.system_utilization.get("Swap_Used", "N/A"))
    swap_total = format_bytes(dolphie.system_utilization.get("Swap_Total", "N/A"))
    table.add_row("[label]Swap", f"{swap_used}[dark_gray]/[/dark_gray]{swap_total}")

    # Disk I/O
    disk_read_values = dolphie.metric_manager.metrics.system_disk_io.Disk_Read.values
    disk_write_values = dolphie.metric_manager.metrics.system_disk_io.Disk_Write.values
    if disk_read_values and disk_write_values:
        last_disk_read = format_number(disk_read_values[-1])
        last_disk_write = format_number(disk_write_values[-1])
        table.add_row("[label]Disk", f"[label]IOPS R[/label] {last_disk_read}\n[label]IOPS W[/label] {last_disk_write}")
    else:
        table.add_row("[label]Disk", "[label]IOPS R[/label] N/A\n[label]IOPS W[/label] N/A")

    return table
