import re

from dolphie import Dolphie
from dolphie.Functions import format_bytes, format_number
from rich import box
from rich.style import Style
from rich.table import Table


def create_panel(dolphie: Dolphie) -> Table:
    variables = dolphie.variables
    statuses = dolphie.statuses
    loop_duration_seconds = dolphie.loop_duration_seconds
    saved_status = dolphie.saved_status
    innodb_status = dolphie.innodb_status

    table_title_style = Style(bold=True)
    table_box = box.ROUNDED
    table_line_color = "#52608d"

    table_innodb_information = Table(
        box=table_box,
        style=table_line_color,
        title="General",
        title_style=table_title_style,
        show_header=False,
    )
    table_innodb_information.add_column("")
    table_innodb_information.add_column("")

    table_innodb_information.add_row(
        "[#c5c7d2]BP Size",
        format_bytes(float(variables["innodb_buffer_pool_size"])),
    )
    table_innodb_information.add_row(
        "[#c5c7d2]BP Available",
        format_bytes(float(variables["innodb_buffer_pool_size"]) - float(statuses["Innodb_buffer_pool_bytes_data"])),
    )
    table_innodb_information.add_row(
        "[#c5c7d2]BP Dirty",
        format_bytes(float(statuses["Innodb_buffer_pool_bytes_dirty"])),
    )
    # MariaDB Support
    if "innodb_buffer_pool_instances" in variables:
        bp_instances = str(variables["innodb_buffer_pool_instances"])
    else:
        bp_instances = 1
    table_innodb_information.add_row("[#c5c7d2]BP Instances", str(bp_instances))
    table_innodb_information.add_row(
        "[#c5c7d2]BP Pages Free",
        format_number(float(statuses["Innodb_buffer_pool_pages_free"])),
    )

    # MariaDB Support
    if "innodb_log_files_in_group" in variables:
        log_files_in_group = variables["innodb_log_files_in_group"]
    else:
        log_files_in_group = 1

    table_innodb_information.add_row(
        "[#c5c7d2]Total Log Size",
        format_bytes(variables["innodb_log_file_size"] * log_files_in_group),
    )

    if "innodb_adaptive_hash_index_parts" in variables.keys():
        table_innodb_information.add_row(
            "[#c5c7d2]Adapt Hash Idx",
            "%s (%s)" % (variables["innodb_adaptive_hash_index"], variables["innodb_adaptive_hash_index_parts"]),
        )
    else:
        table_innodb_information.add_row(
            "[#c5c7d2]Adapt Hash Idx",
            "%s [#c5c7d2]" % (variables["innodb_adaptive_hash_index"]),
        )

    # Get reads/avg bytes/writes/fsyncs per second
    reads_s = 0
    writes_s = 0
    fsyncs_s = 0
    bytes_s = 0

    search_pattern = re.search(
        r"(\d+\.?\d*) reads/s, (\d+\.?\d*) avg bytes/read, (\d+\.?\d*) writes/s, (\d+\.?\d*) fsyncs/s",
        innodb_status["status"],
    )
    if search_pattern:
        reads_s = search_pattern.group(1)
        writes_s = search_pattern.group(3)
        fsyncs_s = search_pattern.group(4)
        bytes_s = search_pattern.group(2)

    # Get OS file reads, OS file writes, and OS fsyncs
    os_file_reads = 0
    os_file_writes = 0
    os_fsyncs = 0

    search_pattern = re.search(
        r"(\d+) OS file reads, (\d+) OS file writes, (\d+) OS fsyncs",
        innodb_status["status"],
    )
    if search_pattern:
        os_file_reads = search_pattern.group(1)
        os_file_writes = search_pattern.group(2)
        os_fsyncs = search_pattern.group(3)

    table_file_io = Table(
        box=table_box,
        style=table_line_color,
        title="File",
        title_style=table_title_style,
        show_header=False,
    )
    table_file_io.add_column("")
    table_file_io.add_column("", min_width=8)

    table_file_io.add_row("[#c5c7d2]OS Reads", format_number(os_file_reads))
    table_file_io.add_row("[#c5c7d2]OS Writes", format_number(os_file_writes))
    table_file_io.add_row("[#c5c7d2]OS fsyncs", format_number(os_fsyncs))
    table_file_io.add_row("[#c5c7d2]Read/s", format_number(reads_s))
    table_file_io.add_row("[#c5c7d2]Write/s", format_number(writes_s))
    table_file_io.add_row("[#c5c7d2]FSync/s", format_number(fsyncs_s))
    table_file_io.add_row("[#c5c7d2]Bytes/s", format_number(bytes_s))

    table_innodb_activity = Table(
        box=table_box,
        style=table_line_color,
        title="Activity",
        title_style=table_title_style,
        show_header=False,
    )
    table_innodb_activity.add_column("")
    table_innodb_activity.add_column("", min_width=8)

    if not saved_status:
        reads_mem_per_second = 0
        reads_disk_per_second = 0
        writes_per_second = 0
        log_waits = 0
        row_lock_waits = 0
        bp_clean_page_wait = 0
    else:
        reads_mem_per_second = round(
            (statuses["Innodb_buffer_pool_read_requests"] - saved_status["Innodb_buffer_pool_read_requests"])
            / loop_duration_seconds
        )
        reads_disk_per_second = round(
            (statuses["Innodb_buffer_pool_reads"] - saved_status["Innodb_buffer_pool_reads"]) / loop_duration_seconds
        )
        writes_per_second = round(
            (statuses["Innodb_buffer_pool_write_requests"] - saved_status["Innodb_buffer_pool_write_requests"])
            / loop_duration_seconds
        )

        log_waits = round((statuses["Innodb_log_waits"] - saved_status["Innodb_log_waits"]) / loop_duration_seconds)

        row_lock_waits = round(
            (statuses["Innodb_row_lock_waits"] - saved_status["Innodb_row_lock_waits"]) / loop_duration_seconds
        )

        bp_clean_page_wait = (
            statuses["Innodb_buffer_pool_wait_free"] - saved_status["Innodb_buffer_pool_wait_free"]
        ) / loop_duration_seconds

    table_innodb_activity.add_row("[#c5c7d2]BP reads/s (mem)", format_number(reads_mem_per_second))
    table_innodb_activity.add_row("[#c5c7d2]BP reads/s (disk)", format_number(reads_disk_per_second))
    table_innodb_activity.add_row("[#c5c7d2]BP writes/s", format_number(writes_per_second))

    bp_clean_page_wait_color = ""
    if bp_clean_page_wait:
        bp_clean_page_wait_color = "[#fc7979]"

    table_innodb_activity.add_row(
        "[#c5c7d2]BP clean page wait/s",
        bp_clean_page_wait_color + format_number(bp_clean_page_wait),
    )
    table_innodb_activity.add_row("[#c5c7d2]Log waits/s", format_number(log_waits))
    table_innodb_activity.add_row("[#c5c7d2]Row lock waits/s", format_number(row_lock_waits))
    table_innodb_activity.add_row("[#c5c7d2]Row lock time avg", "%sms" % str(statuses["Innodb_row_lock_time_avg"]))

    table_row_operations = Table(
        box=table_box,
        style=table_line_color,
        title="Row Operations/s",
        title_style=table_title_style,
        show_header=False,
    )
    table_row_operations.add_column("")
    table_row_operations.add_column("", min_width=8)

    if not saved_status:
        reads_per_second = 0
        inserts_per_second = 0
        updates_per_second = 0
        deletes_per_second = 0
    else:
        reads_per_second = round(
            (statuses["Innodb_rows_read"] - saved_status["Innodb_rows_read"]) / loop_duration_seconds
        )
        inserts_per_second = round(
            (statuses["Innodb_rows_inserted"] - saved_status["Innodb_rows_inserted"]) / loop_duration_seconds
        )
        updates_per_second = round(
            (statuses["Innodb_rows_updated"] - saved_status["Innodb_rows_updated"]) / loop_duration_seconds
        )
        deletes_per_second = round(
            (statuses["Innodb_rows_deleted"] - saved_status["Innodb_rows_deleted"]) / loop_duration_seconds
        )

    table_row_operations.add_row("[#c5c7d2]Reads", format_number(reads_per_second))
    table_row_operations.add_row("[#c5c7d2]Inserts", format_number(inserts_per_second))
    table_row_operations.add_row("[#c5c7d2]Updates", format_number(updates_per_second))
    table_row_operations.add_row("[#c5c7d2]Deletes", format_number(deletes_per_second))
    table_row_operations.add_row()
    table_row_operations.add_row()
    table_row_operations.add_row()

    # Put these two tables side-by-side
    table_grid = Table.grid()
    table_grid.add_row(table_innodb_information, table_innodb_activity, table_row_operations, table_file_io)

    return table_grid
