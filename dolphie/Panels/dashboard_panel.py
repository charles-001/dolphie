from datetime import datetime, timedelta

from dolphie.Modules.Functions import format_bytes, format_number
from dolphie.Modules.MetricManager import MetricData
from dolphie.Modules.TabManager import Tab
from dolphie.Panels import replication_panel
from rich.style import Style
from rich.table import Table


def create_panel(tab: Tab) -> Table:
    dolphie = tab.dolphie

    global_status = dolphie.global_status
    global_variables = dolphie.global_variables
    binlog_status = dolphie.binlog_status

    table_title_style = Style(color="#bbc8e8", bold=True)

    ################
    # Information #
    ###############
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
        host_type = "MySQL"

    runtime = str(datetime.now() - dolphie.dolphie_start_time).split(".")[0]

    replicas = 0
    if dolphie.replica_manager.available_replicas:
        replicas = len(dolphie.replica_manager.available_replicas)

    table_information.add_column()
    table_information.add_column(min_width=25, max_width=27)
    table_information.add_row("[label]Version", f"{dolphie.host_distro} {dolphie.mysql_version}")
    table_information.add_row(
        "[label]", "%s (%s)" % (global_variables["version_compile_os"], global_variables["version_compile_machine"])
    )
    table_information.add_row("[label]Type", f"{host_type} [label]SSL[/label] {dolphie.main_db_connection.using_ssl}")
    table_information.add_row("[label]Uptime", str(timedelta(seconds=global_status["Uptime"])))
    table_information.add_row("[label]Runtime", f"{runtime} [label]Latency[/label] {dolphie.refresh_latency}s")
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

    tab.dashboard_host_information.update(table_information)

    ###########
    # InnoDB  #
    ###########
    table_innodb = Table(show_header=False, box=None, title="InnoDB", title_style=table_title_style)

    table_innodb.add_column()
    table_innodb.add_column(width=9)

    history_list_length = dolphie.innodb_metrics.get("trx_rseg_history_len", "N/A")

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

    bp_instances = global_variables.get("innodb_buffer_pool_instances", "N/A")
    if bp_instances != "N/A":
        plural = "s" if bp_instances > 1 else ""
        table_innodb.add_row("[label]BP Instance" + plural, format_number(bp_instances))
    else:
        table_innodb.add_row("[label]BP Instance", bp_instances)

    table_innodb.add_row("[label]BP Size", format_bytes(global_variables["innodb_buffer_pool_size"]))
    table_innodb.add_row(
        "[label]BP Available",
        format_bytes(
            dolphie.global_variables["innodb_buffer_pool_size"] - dolphie.global_status["Innodb_buffer_pool_bytes_data"]
        ),
    )
    table_innodb.add_row("[label]BP Dirty", format_bytes(global_status["Innodb_buffer_pool_bytes_dirty"]))
    table_innodb.add_row("[label]History List", format_number(history_list_length))

    tab.dashboard_innodb.update(table_innodb)

    ##############
    # Binary Log #
    ##############
    table_primary = Table(show_header=False, box=None, title="Binary Log", title_style=table_title_style)

    if not binlog_status:
        table_primary.add_column(justify="center")
        table_primary.add_row("\n\n\n[b][label]Disabled")
    else:
        table_primary.add_column()
        table_primary.add_column(max_width=40)

        if dolphie.previous_binlog_position == 0:
            diff_binlog_position = 0
        elif dolphie.previous_binlog_position > binlog_status["Position"]:
            diff_binlog_position = "Binlog Rotated"
        else:
            diff_binlog_position = format_bytes(binlog_status["Position"] - dolphie.previous_binlog_position)

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
        table_primary.add_row("[label]Diff", str(diff_binlog_position))
        table_primary.add_row("[label]Cache Hit", f"{binlog_cache}%")

        binlog_format = global_variables.get("binlog_format", "N/A")
        binlog_row_image = None
        if binlog_format == "ROW":
            binlog_row_image = global_variables.get("binlog_row_image", "N/A")
            table_primary.add_row("[label]Format", "{} ({})".format(binlog_format, binlog_row_image))
        else:
            table_primary.add_row("[label]Format", binlog_format, binlog_row_image)

        gtid_mode = global_variables.get("gtid_mode", "N/A")
        table_primary.add_row("[label]GTID", gtid_mode)

        binlog_compression = global_variables.get("binlog_transaction_compression", "N/A")
        # binlog_compression_percentage = ""
        # if binlog_compression == "ON":
        #     if dolphie.binlog_transaction_compression_percentage:
        #         binlog_compression_percentage = f" ({dolphie.binlog_transaction_compression_percentage}% gain)"
        #     else:
        #         binlog_compression_percentage = " (N/A gain)"

        table_primary.add_row("[label]Compression", binlog_compression)

        # Save some global_variables to be used in next refresh
        if dolphie.binlog_status:
            dolphie.previous_binlog_position = dolphie.binlog_status["Position"]

    tab.dashboard_binary_log.update(table_primary)

    ###############
    # Replication #
    ###############
    if dolphie.replication_status and not dolphie.panels.replication.visible:
        tab.dashboard_replication.display = True
        tab.dashboard_replication.update(replication_panel.create_replication_table(tab, dashboard_table=True))
    else:
        tab.dashboard_replication.display = False
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
