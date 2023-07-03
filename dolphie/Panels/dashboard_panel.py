import re
from datetime import datetime, timedelta

from dolphie import Dolphie
from dolphie.Functions import format_bytes, format_number
from dolphie.Panels import replica_panel
from rich import box
from rich.align import Align
from rich.panel import Panel
from rich.style import Style
from rich.table import Table


def create_panel(dolphie: Dolphie):
    statuses = dolphie.statuses
    variables = dolphie.variables
    innodb_status = dolphie.innodb_status
    saved_status = dolphie.saved_status
    loop_duration_seconds = dolphie.loop_duration_seconds
    primary_status = dolphie.primary_status

    tables_to_add = []
    uptime = str(timedelta(seconds=statuses["Uptime"]))

    dashboard_grid = Table.grid()
    dashboard_grid.add_column()
    dashboard_grid.add_column()
    dashboard_grid.add_column()

    row_style = Style(color="gray78")
    table_title_style = Style(color="grey93", bold=True)
    table_box = box.ROUNDED
    table_line_color = "grey78"

    ################
    # Information #
    ###############
    table_information = Table(
        show_header=False,
        box=box.ROUNDED,
        title="Host Information",
        title_style=table_title_style,
        style=table_line_color,
    )

    if loop_duration_seconds < 1:
        refresh_latency = 0
    else:
        refresh_latency = str(round(loop_duration_seconds - dolphie.refresh_interval, 2))

    use_performance_schema_status = "NO"
    if dolphie.use_performance_schema:
        use_performance_schema_status = "YES"

    if variables["read_only"] == "ON":
        if not dolphie.replica_status:
            variables["read_only"] = "YES ([red]SHOULD BE NO?[/red])"
        else:
            variables["read_only"] = "YES"
    elif variables["read_only"] == "OFF":
        variables["read_only"] = "NO"

    runtime = str(datetime.now() - dolphie.dolphie_start_time).split(".")[0]

    table_information.add_column()
    table_information.add_column(width=27)
    table_information.add_row("Name", "[grey93]%s" % dolphie.host, style=row_style)
    table_information.add_row("Version", "[grey93]%s %s" % (dolphie.host_distro, dolphie.full_version), style=row_style)
    table_information.add_row("Uptime", "[grey93]%s" % uptime, style=row_style)
    table_information.add_row(
        "Runtime", "[grey93]%s [grey78]latency: [grey93]%ss" % (runtime, refresh_latency), style=row_style
    )
    table_information.add_row("Read Only", "[grey93]%s" % variables["read_only"], style=row_style)
    table_information.add_row("Use PS", "[grey93]%s" % (use_performance_schema_status), style=row_style)
    table_information.add_row(
        "Threads",
        "[grey78]con[grey93] %s[steel_blue1]/[grey78]run[grey93] %s[steel_blue1]/[grey78]cac[grey93] %s"
        % (
            format_number(statuses["Threads_connected"]),
            format_number(statuses["Threads_running"]),
            format_number(statuses["Threads_cached"]),
        ),
        style=row_style,
    )
    table_information.add_row(
        "Tables",
        "[grey78]open[grey93] %s[steel_blue1]/[grey78]opened[grey93] %s"
        % (
            format_number(statuses["Open_tables"]),
            format_number(statuses["Opened_tables"]),
        ),
        style=row_style,
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

    # Calculate Checkpoint Efficiency
    innodb_log_file_size = variables["innodb_log_file_size"]

    # MariaDB Support
    if "innodb_log_files_in_group" in variables:
        innodb_log_files_in_group = variables["innodb_log_files_in_group"]
    else:
        innodb_log_files_in_group = 1

    # Save what percentage of log files InnoDB will start to aggressively flush
    # to disk due to checkpointing based on version
    if dolphie.full_version.startswith("8"):
        version_threshold = 0.875
    else:
        version_threshold = 0.81

    checkpoint_efficiency = "N/A"
    log_sequence_number_match = re.search(r"Log sequence number\s*(\d+)", innodb_status["status"])
    last_checkpoint_match = re.search(r"Last checkpoint at\s*(\d+)", innodb_status["status"])
    if log_sequence_number_match and last_checkpoint_match:
        checkpoint_age = int(log_sequence_number_match.group(1)) - int(last_checkpoint_match.group(1))

        max_checkpoint_age = innodb_log_file_size * innodb_log_files_in_group * version_threshold
        if checkpoint_age >= max_checkpoint_age:
            checkpoint_efficiency = "[bright_red]0.00%"
        elif max_checkpoint_age > checkpoint_age:
            checkpoint_efficiency = round(100 - (checkpoint_age / max_checkpoint_age * 100), 2)

            if checkpoint_efficiency > 40:
                checkpoint_efficiency = "[bright_green]%s%%" % checkpoint_efficiency
            elif checkpoint_efficiency > 20:
                checkpoint_efficiency = "[bright_yellow]%s%%" % checkpoint_efficiency
            else:
                checkpoint_efficiency = "[bright_red]%s%%" % checkpoint_efficiency

    # Get history list length
    output = re.search(r"History list length (\d+)", innodb_status["status"])
    if output:
        history_list_length = output.group(1)
    else:
        history_list_length = "N/A"

    # Calculate AIO reads
    total_pending_aio_reads = 0
    total_pending_aio_writes = 0
    output = re.search(
        r"Pending normal aio reads: (?:\d+\s)?\[(.*?)\] , aio writes: (?:\d+\s)?\[(.*?)\]",
        innodb_status["status"],
    )
    if output:
        match = output.group(1).split(",")

        for aio_read in match:
            total_pending_aio_reads += int(aio_read)

        match = output.group(2).split(",")
        for aio_write in match:
            total_pending_aio_writes += int(aio_write)
    else:
        total_pending_aio_reads = "N/A"
        total_pending_aio_writes = "N/A"

    # Calculate InnoDB memory read hit efficiency
    innodb_efficiency = "N/A"

    ib_pool_disk_reads = statuses["Innodb_buffer_pool_reads"]
    ib_pool_mem_reads = statuses["Innodb_buffer_pool_read_requests"]
    if ib_pool_disk_reads >= ib_pool_mem_reads:
        innodb_efficiency = "[bright_red]0.00%"
    elif ib_pool_mem_reads > ib_pool_disk_reads:
        innodb_efficiency = round(100 - (ib_pool_disk_reads / ib_pool_mem_reads * 100), 2)

        if innodb_efficiency > 90:
            innodb_efficiency = "[bright_green]%s%%" % innodb_efficiency
        elif innodb_efficiency > 80:
            innodb_efficiency = "[bright_yellow]%s%%" % innodb_efficiency
        else:
            innodb_efficiency = "[bright_red]%s%%" % innodb_efficiency

    # Calculate AHI Hit efficiency
    hash_searches = 0
    non_hash_searches = 0
    if variables["innodb_adaptive_hash_index"] == "ON":
        output = re.search(r"(\d+\.?\d+) hash searches\/s, (\d+\.?\d+) non-hash searches\/s", innodb_status["status"])
        if output:
            hash_searches = float(output.group(1))
            non_hash_searches = float(output.group(2))

            if non_hash_searches == 0 and hash_searches == 0:
                hash_search_efficiency = "Inactive"
            elif non_hash_searches >= hash_searches:
                hash_search_efficiency = "[bright_red]0.00%"
            elif hash_searches > non_hash_searches:
                hash_search_efficiency = round(100 - (non_hash_searches / hash_searches * 100), 2)

                if hash_search_efficiency > 70:
                    hash_search_efficiency = "[bright_green]%s%%" % hash_search_efficiency
                elif hash_search_efficiency > 50:
                    hash_search_efficiency = "[bright_yellow]%s%%" % hash_search_efficiency
                else:
                    hash_search_efficiency = "[bright_red]%s%%" % hash_search_efficiency
        else:
            hash_search_efficiency = "N/A"
    else:
        hash_search_efficiency = "OFF"

    # Get queries inside InnoDB
    output = re.search(r"(\d+) queries inside InnoDB, (\d+) queries in queue", innodb_status["status"])
    if output:
        queries_active = int(output.group(1))
        queries_queued = int(output.group(2))
    else:
        queries_active = "N/A"
        queries_queued = "N/A"

    # Calculate unpurged transactions
    output = re.search(r"Trx id counter (\d+)", innodb_status["status"])
    if output:
        trx_id_counter = int(output.group(1))
    else:
        trx_id_counter = None

    output = re.search(r"Purge done for trx's n:o < (\d+)", innodb_status["status"])
    if output:
        purge_done_for_trx = int(output.group(1))
    else:
        purge_done_for_trx = None

    if trx_id_counter is not None and purge_done_for_trx is not None:
        unpurged_trx = str(trx_id_counter - purge_done_for_trx)
    else:
        trx_id_counter = "N/A"

    # Add data to our table
    table_innodb.add_row("Read Hit", "[grey93]%s" % innodb_efficiency, style=row_style)
    table_innodb.add_row("Chkpt Age", "[grey93]%s" % checkpoint_efficiency, style=row_style)
    table_innodb.add_row("AHI Hit", "[grey93]%s" % (hash_search_efficiency), style=row_style)

    # Don't show thread concurrency information if it isn't set to on, instead show buffer pool stats
    if "innodb_thread_concurrency" in variables and variables["innodb_thread_concurrency"]:
        concurrency_ratio = round((queries_active / variables["innodb_thread_concurrency"]) * 100)

        if concurrency_ratio >= 80:
            queries_active_formatted = "[bright_red]%s" % format_number(queries_active)
        elif concurrency_ratio >= 60:
            queries_active_formatted = "[bright_yellow]%s" % format_number(queries_active)
        else:
            queries_active_formatted = "[grey93]%s" % format_number(queries_active)

        table_innodb.add_row(
            "Query Active",
            "[grey93]%s [steel_blue1]/ [grey93]%s" % (queries_active_formatted, variables["innodb_thread_concurrency"]),
            style=row_style,
        )
        table_innodb.add_row("Query Queued", "[grey93]%s" % format_number(queries_queued), style=row_style)
    else:
        table_innodb.add_row(
            "BP Size",
            "[grey93]%s" % (format_bytes(float(variables["innodb_buffer_pool_size"]))),
            style=row_style,
        )
        table_innodb.add_row(
            "BP Dirty",
            "[grey93]%s" % (format_bytes(float(statuses["Innodb_buffer_pool_bytes_dirty"]))),
            style=row_style,
        )

    if str(total_pending_aio_reads) == "N/A":
        table_innodb.add_row(
            "Pending AIO",
            "[gray93]N/A",
            style=row_style,
        )
    else:
        table_innodb.add_row(
            "Pending AIO",
            "[gray78]W [grey93]%s [gray78]R [grey93]%s" % (str(total_pending_aio_writes), str(total_pending_aio_reads)),
            style=row_style,
        )

    table_innodb.add_row(
        "History List",
        "[grey93]%s" % format_number(history_list_length),
        style=row_style,
    )
    table_innodb.add_row("Unpurged TRX", "[grey93]%s" % format_number(unpurged_trx), style=row_style)

    tables_to_add.append(table_innodb)

    ##############
    # Binary Log #
    ##############
    table_primary = Table()

    if primary_status:
        table_primary = Table(
            show_header=False,
            box=table_box,
            title="Binary Log",
            title_style=table_title_style,
            style=table_line_color,
        )

        if dolphie.previous_binlog_position == 0:
            diff_binlog_position = 0
        elif dolphie.previous_binlog_position > primary_status["Position"]:
            diff_binlog_position = "Binlog Rotated"
        else:
            diff_binlog_position = format_bytes(primary_status["Position"] - dolphie.previous_binlog_position)

        binlog_cache = 100
        binlog_cache_disk = statuses["Binlog_cache_disk_use"]
        binlog_cache_mem = statuses["Binlog_cache_use"]
        if binlog_cache_disk and binlog_cache_mem:
            if binlog_cache_disk >= binlog_cache_mem:
                innodb_efficiency = "[bright_red]0.00%"
            elif binlog_cache_mem > binlog_cache_disk:
                binlog_cache = round(100 - (binlog_cache_disk / binlog_cache_mem), 2)

        table_primary.add_column()
        table_primary.add_column(max_width=40)
        table_primary.add_row("File name", "[grey93]%s" % str(primary_status["File"]), style=row_style)
        table_primary.add_row(
            "Position",
            "[grey93]%s" % (str(primary_status["Position"])),
            style=row_style,
        )
        table_primary.add_row(
            "Size",
            "[grey93]%s" % format_bytes(primary_status["Position"]),
            style=row_style,
        )
        table_primary.add_row("Diff", "[grey93]%s" % diff_binlog_position, style=row_style)
        table_primary.add_row("Cache Hit", "[grey93]%s%%" % str(binlog_cache), style=row_style)
        # MariaDB Support
        if "gtid_mode" in variables:
            table_primary.add_row("GTID", "[grey93]%s" % str(variables["gtid_mode"]), style=row_style)
        else:
            table_primary.add_row()
        table_primary.add_row()
        table_primary.add_row()

        tables_to_add.append(table_primary)

    ###############
    # Replication #
    ###############
    if dolphie.replica_status and dolphie.layout["replicas"].visible is False:
        tables_to_add.append(replica_panel.create_table(dolphie, dolphie.replica_status, dashboard_table=True))

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

    if loop_duration_seconds == 0:
        queries_per_second = 0
        connects_per_second = 0
        selects_per_second = 0
        inserts_per_second = 0
        updates_per_second = 0
        deletes_per_second = 0
        replaces_per_second = 0
        rollbacks_per_second = 0
    else:
        queries_per_second = round((statuses["Queries"] - saved_status["Queries"]) / loop_duration_seconds)

        connects_per_second = round((statuses["Connections"] - saved_status["Connections"]) / loop_duration_seconds)
        selects_per_second = round((statuses["Com_select"] - saved_status["Com_select"]) / loop_duration_seconds)
        inserts_per_second = round((statuses["Com_insert"] - saved_status["Com_insert"]) / loop_duration_seconds)
        updates_per_second = round((statuses["Com_update"] - saved_status["Com_update"]) / loop_duration_seconds)
        deletes_per_second = round((statuses["Com_delete"] - saved_status["Com_delete"]) / loop_duration_seconds)
        replaces_per_second = round((statuses["Com_replace"] - saved_status["Com_replace"]) / loop_duration_seconds)
        rollbacks_per_second = round((statuses["Com_rollback"] - saved_status["Com_rollback"]) / loop_duration_seconds)

    table_stats.add_row(
        "Queries",
        "[grey93]%s" % format_number(queries_per_second),
        style=row_style,
    )
    table_stats.add_row("SELECT", "[grey93]%s" % format_number(selects_per_second), style=row_style)
    table_stats.add_row("INSERT", "[grey93]%s" % format_number(inserts_per_second), style=row_style)
    table_stats.add_row("UPDATE", "[grey93]%s" % format_number(updates_per_second), style=row_style)
    table_stats.add_row("DELETE", "[grey93]%s" % format_number(deletes_per_second), style=row_style)
    table_stats.add_row(
        "REPLACE",
        "[grey93]%s" % format_number(replaces_per_second),
        style=row_style,
    )
    table_stats.add_row(
        "ROLLBACK",
        "[grey93]%s" % format_number(rollbacks_per_second),
        style=row_style,
    )
    table_stats.add_row(
        "CONNECT",
        "[grey93]%s" % format_number(connects_per_second),
        style=row_style,
    )

    tables_to_add.append(table_stats)

    dashboard_grid.add_row(*tables_to_add)

    dashboard_panel = Panel(
        Align.center(dashboard_grid),
        box=box.SIMPLE,
        border_style="steel_blue1",
    )

    return dashboard_panel
