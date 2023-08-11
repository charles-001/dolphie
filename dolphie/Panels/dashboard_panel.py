from datetime import datetime, timedelta

from dolphie import Dolphie
from dolphie.Modules.Functions import format_bytes, format_number
from dolphie.Panels import replication_panel
from rich import box
from rich.style import Style
from rich.table import Table


def create_panel(dolphie: Dolphie) -> Table:
    global_status = dolphie.global_status
    global_variables = dolphie.global_variables
    binlog_status = dolphie.binlog_status

    tables_to_add = []
    uptime = str(timedelta(seconds=global_status["Uptime"]))

    dashboard_grid = Table.grid()
    dashboard_grid.add_column()
    dashboard_grid.add_column()
    dashboard_grid.add_column()

    table_title_style = Style(bold=True)
    table_box = box.ROUNDED
    table_line_color = "#52608d"

    ################
    # Information #
    ###############
    table_information = Table(
        show_header=False,
        box=table_box,
        title="Host Information",
        title_style=table_title_style,
        style=table_line_color,
    )

    if dolphie.worker_job_time < 1:
        refresh_latency = 0
    else:
        refresh_latency = str(round(dolphie.worker_job_time - dolphie.refresh_interval, 2))

    use_performance_schema_status = "NO"
    if dolphie.use_performance_schema:
        use_performance_schema_status = "YES"

    if global_variables["read_only"] == "ON":
        if not dolphie.replication_status:
            global_variables["read_only"] = "YES ([indian_red]SHOULD BE NO?[/indian_red])"
        else:
            global_variables["read_only"] = "YES"
    elif global_variables["read_only"] == "OFF":
        global_variables["read_only"] = "NO"

    runtime = str(datetime.now() - dolphie.dolphie_start_time).split(".")[0]

    table_information.add_column()
    table_information.add_column(width=25)
    table_information.add_row("[#c5c7d2]Version", "%s %s" % (dolphie.host_distro, dolphie.mysql_version))
    table_information.add_row("[#c5c7d2]Uptime", "%s" % uptime)
    table_information.add_row("[#c5c7d2]Runtime", "%s [#c5c7d2]latency:[/#c5c7d2] %ss" % (runtime, refresh_latency))
    table_information.add_row("[#c5c7d2]Read Only", "%s" % global_variables["read_only"])
    table_information.add_row("[#c5c7d2]Use PS", "%s" % (use_performance_schema_status))
    table_information.add_row(
        "[#c5c7d2]Threads",
        "[#c5c7d2]con[/#c5c7d2] %s[#91abec]/[/#91abec][#c5c7d2]run[/#c5c7d2]"
        " %s[#91abec]/[/#91abec][#c5c7d2]cac[/#c5c7d2] %s"
        % (
            format_number(global_status["Threads_connected"]),
            format_number(global_status["Threads_running"]),
            format_number(global_status["Threads_cached"]),
        ),
    )
    table_information.add_row(
        "[#c5c7d2]Tables",
        "[#c5c7d2]open[/#c5c7d2] %s[#91abec]/[/#91abec][#c5c7d2]opened[/#c5c7d2] %s"
        % (
            format_number(global_status["Open_tables"]),
            format_number(global_status["Opened_tables"]),
        ),
    )

    tables_to_add.append(table_information)

    ###########
    # InnoDB  #
    ###########
    table_innodb = Table(
        show_header=False,
        box=table_box,
        title="InnoDB",
        title_style=table_title_style,
        style=table_line_color,
    )

    table_innodb.add_column()
    table_innodb.add_column(width=9)

    history_list_length = "N/A"
    if "trx_rseg_history_len" in dolphie.innodb_metrics:
        history_list_length = dolphie.innodb_metrics["trx_rseg_history_len"]

    # Calculate InnoDB memory read hit efficiency
    innodb_efficiency = "N/A"

    ib_pool_disk_reads = global_status.get("Innodb_buffer_pool_reads", 0)
    ib_pool_mem_reads = global_status.get(
        "Innodb_buffer_pool_read_requests", 1
    )  # Default to 1 to avoid division by zero

    if ib_pool_disk_reads >= ib_pool_mem_reads:
        innodb_efficiency = "[#fc7979]0.00%"
    else:
        efficiency = 100 - (ib_pool_disk_reads / ib_pool_mem_reads * 100)

        if efficiency > 90:
            color_code = "#54efae"
        elif efficiency > 80:
            color_code = "#f1fb82"
        else:
            color_code = "#fc7979"

        innodb_efficiency = f"[{color_code}]{efficiency:.2f}%"

    hash_search_efficiency = dolphie.metric_manager.get_metric_adaptive_hash_index()

    # Add data to our table
    table_innodb.add_row("[#c5c7d2]Read Hit", "%s" % innodb_efficiency)
    table_innodb.add_row("[#c5c7d2]Chkpt Age", "%s" % dolphie.metric_manager.get_metric_checkpoint_age(format=True))
    table_innodb.add_row("[#c5c7d2]AHI Hit", "%s" % (hash_search_efficiency))
    table_innodb.add_row("[#c5c7d2]BP Size", "%s" % (format_bytes(float(global_variables["innodb_buffer_pool_size"]))))
    table_innodb.add_row(
        "[#c5c7d2]BP Dirty", "%s" % (format_bytes(float(global_status["Innodb_buffer_pool_bytes_dirty"])))
    )

    table_innodb.add_row("[#c5c7d2]History List", "%s" % format_number(history_list_length))
    table_innodb.add_row("[#c5c7d2]", "")

    tables_to_add.append(table_innodb)

    ##############
    # Binary Log #
    ##############
    table_primary = Table()

    if binlog_status:
        table_primary = Table(
            show_header=False,
            box=table_box,
            title="Binary Log",
            title_style=table_title_style,
            style=table_line_color,
        )

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
                innodb_efficiency = "[#fc7979]0.00%"
            elif binlog_cache_mem > binlog_cache_disk:
                binlog_cache = round(100 - (binlog_cache_disk / binlog_cache_mem), 2)

        table_primary.add_column()
        table_primary.add_column(max_width=40)
        table_primary.add_row("[#c5c7d2]File name", "%s" % str(binlog_status["File"]))
        table_primary.add_row(
            "[#c5c7d2]Position",
            "%s" % (str(binlog_status["Position"])),
        )
        table_primary.add_row(
            "[#c5c7d2]Size",
            "%s" % format_bytes(binlog_status["Position"]),
        )
        table_primary.add_row("[#c5c7d2]Diff", "%s" % diff_binlog_position)
        table_primary.add_row("[#c5c7d2]Cache Hit", "%s%%" % str(binlog_cache))
        # MariaDB Support
        if "gtid_mode" in global_variables:
            table_primary.add_row("[#c5c7d2]GTID", "%s" % str(global_variables["gtid_mode"]))
        else:
            table_primary.add_row()
        table_primary.add_row()

        tables_to_add.append(table_primary)

        # Save some global_variables to be used in next refresh
        if dolphie.binlog_status:
            dolphie.previous_binlog_position = dolphie.binlog_status["Position"]

    ###############
    # Replication #
    ###############
    if dolphie.replication_status and not dolphie.display_replication_panel:
        tables_to_add.append(replication_panel.create_table(dolphie, dolphie.replication_status, dashboard_table=True))

    ###############
    # Statisitics #
    ###############
    table_stats = Table(
        show_header=False,
        box=table_box,
        title="Statisitics/s",
        title_style=table_title_style,
        style=table_line_color,
    )

    table_stats.add_column()
    table_stats.add_column(min_width=7)

    table_stats.add_row("[#c5c7d2]Queries", dolphie.metric_manager.get_metric_calculate_per_sec("Queries"))
    table_stats.add_row("[#c5c7d2]SELECT", dolphie.metric_manager.get_metric_calculate_per_sec("Com_select"))
    table_stats.add_row("[#c5c7d2]INSERT", dolphie.metric_manager.get_metric_calculate_per_sec("Com_insert"))
    table_stats.add_row("[#c5c7d2]UPDATE", dolphie.metric_manager.get_metric_calculate_per_sec("Com_update"))
    table_stats.add_row("[#c5c7d2]DELETE", dolphie.metric_manager.get_metric_calculate_per_sec("Com_delete"))
    table_stats.add_row("[#c5c7d2]REPLACE", dolphie.metric_manager.get_metric_calculate_per_sec("Com_replace"))
    table_stats.add_row("[#c5c7d2]ROLLBACK", dolphie.metric_manager.get_metric_calculate_per_sec("Com_rollback"))

    tables_to_add.append(table_stats)

    dashboard_grid.add_row(*tables_to_add)

    return dashboard_grid
