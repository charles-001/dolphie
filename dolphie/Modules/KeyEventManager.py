import csv
import re
import threading
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from rich import box
from rich.align import Align
from rich.console import Group
from rich.style import Style
from rich.table import Table
from sqlparse import format as sqlformat
from textual.widgets import Button

from dolphie.DataTypes import (
    ConnectionSource,
    HotkeyCommands,
    ProcesslistThread,
    ProxySQLProcesslistThread,
)
from dolphie.Modules.Functions import (
    escape_markup,
    format_bytes,
    format_number,
    format_query,
    format_sys_table_memory,
)
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.Queries import MySQLQueries, ProxySQLQueries
from dolphie.Widgets.CommandModal import CommandModal
from dolphie.Widgets.CommandScreen import CommandScreen
from dolphie.Widgets.EventLogScreen import EventLog
from dolphie.Widgets.ProxySQLThreadScreen import ProxySQLThreadScreen
from dolphie.Widgets.ThreadScreen import ThreadScreen

if TYPE_CHECKING:
    from dolphie.App import DolphieApp


class KeyEventManager:
    """This module manages all keyboard event processing.

    This includes both immediate key event handling and background command execution
    in threads.
    """

    def __init__(self, app: "DolphieApp"):
        """Initialize the KeyEventManager.

        Args:
            app: Reference to the main DolphieApp instance
        """
        self.app = app

        # Debouncing to prevent rapid key presses from overwhelming the system
        self.last_key_time = {}
        self.default_debounce_interval = timedelta(milliseconds=50)

        # Custom debounce intervals for specific keys that trigger expensive operations
        self.key_debounce_intervals = {
            "left_square_bracket": timedelta(milliseconds=100),  # Replay backward
            "right_square_bracket": timedelta(milliseconds=100),  # Replay forward
            "space": timedelta(milliseconds=300),  # Start worker
            "minus": timedelta(milliseconds=300),  # Remove tab (destructive)
        }

    async def process_key_event(self, key: str) -> None:
        """Process a keyboard event and execute the corresponding action.

        This method handles all keyboard shortcuts and commands in the application,
        from panel switching to data filtering to command execution.

        Args:
            key: The key that was pressed
        """
        tab = self.app.tab_manager.active_tab
        if not tab:
            return

        # Apply debouncing to prevent rapid key presses
        now = datetime.now().astimezone()
        debounce_interval = self.key_debounce_intervals.get(key, self.default_debounce_interval)
        last_time = self.last_key_time.get(key, datetime.min.replace(tzinfo=timezone.utc))

        if now - last_time < debounce_interval:
            return  # Key press is too soon, ignore it

        self.last_key_time[key] = now

        screen_data = None
        dolphie = tab.dolphie

        # Validate key is a valid command (excluding special keys)
        if key not in self.app.command_manager.exclude_keys:
            if not self.app.command_manager.get_commands(dolphie.replay_file, dolphie.connection_source).get(key):
                self.app.notify(
                    f"Key [$highlight]{key}[/$highlight] is not a valid command",
                    severity="warning",
                )
                return

            # Prevent commands from being run if the secondary connection is processing a query already
            if dolphie.secondary_db_connection and dolphie.secondary_db_connection.is_running_query:
                self.app.notify("There's already a command running - please wait for it to finish")
                return

            if not dolphie.main_db_connection.is_connected() and not dolphie.replay_file:
                self.app.notify("You must be connected to a host to use commands")
                return

        if self.app.tab_manager.loading_hostgroups:
            self.app.notify("You can't run commands while hosts are connecting as a hostgroup")
            return

        # Panel switching commands (1-8)
        if key == "1":
            self.app.toggle_panel(dolphie.panels.dashboard.name)

        elif key == "2":
            self.app.tab_manager.active_tab.processlist_datatable.clear()
            self.app.toggle_panel(dolphie.panels.processlist.name)

        elif key == "3":
            self.app.toggle_panel(dolphie.panels.graphs.name)
            self.app.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)

        elif key == "4":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.app.toggle_panel(dolphie.panels.proxysql_hostgroup_summary.name)
                dolphie.proxysql_per_second_data.clear()
                self.app.tab_manager.active_tab.proxysql_hostgroup_summary_datatable.clear()
                return

            if dolphie.replay_file and (not dolphie.replication_status and not dolphie.group_replication_members):
                self.app.notify("This replay file has no replication data")
                return

            if not any(
                [
                    dolphie.replica_manager.available_replicas,
                    dolphie.replication_status,
                    dolphie.galera_cluster,
                    dolphie.group_replication,
                    dolphie.innodb_cluster,
                    dolphie.innodb_cluster_read_replica,
                ]
            ):
                self.app.notify("Replication panel has no data to display")
                return

            self.app.toggle_panel(dolphie.panels.replication.name)
            tab.toggle_entities_displays()

            if dolphie.panels.replication.visible:
                if dolphie.replica_manager.available_replicas:
                    # No loading animation necessary for replay mode
                    if not dolphie.replay_file:
                        tab.replicas_loading_indicator.display = True
                        tab.replicas_title.update(
                            f"[$white][b]Loading [$highlight]{len(dolphie.replica_manager.available_replicas)}"
                            "[/$highlight] replicas...\n"
                        )

                tab.toggle_replication_panel_components()
            else:
                tab.remove_replication_panel_components()

        elif key == "5":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.app.toggle_panel(dolphie.panels.proxysql_mysql_query_rules.name)
                return

            if not dolphie.metadata_locks_enabled and not dolphie.replay_file:
                self.app.notify(
                    "Metadata Locks panel requires MySQL 5.7+ with Performance Schema enabled along with "
                    "[$highlight]wait/lock/metadata/sql/mdl[/$highlight] enabled in setup_instruments table"
                )
                return

            self.app.toggle_panel(dolphie.panels.metadata_locks.name)
            self.app.tab_manager.active_tab.metadata_locks_datatable.clear()

        elif key == "6":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.app.toggle_panel(dolphie.panels.proxysql_command_stats.name)
            else:
                if not dolphie.is_mysql_version_at_least("5.7") or not dolphie.performance_schema_enabled:
                    self.app.notify("DDL panel requires MySQL 5.7+ with Performance Schema enabled")
                    return

                query = (
                    "SELECT enabled FROM performance_schema.setup_instruments WHERE name LIKE 'stage/innodb/alter%';"
                )
                dolphie.secondary_db_connection.execute(query)
                data = dolphie.secondary_db_connection.fetchall()
                for row in data:
                    if row.get("enabled") == "NO":
                        self.app.notify("DDL panel requires Performance Schema to have 'stage/innodb/alter%' enabled")
                        return

                self.app.toggle_panel(dolphie.panels.ddl.name)
                self.app.tab_manager.active_tab.ddl_datatable.clear()

        elif key == "7":
            if dolphie.is_mysql_version_at_least("5.7") and dolphie.performance_schema_enabled:
                if not dolphie.pfs_metrics_last_reset_time:
                    dolphie.pfs_metrics_last_reset_time = datetime.now().astimezone()
                self.app.toggle_panel(dolphie.panels.pfs_metrics.name)
            else:
                self.app.notify("Performance Schema Metrics panel requires MySQL 5.7+ with Performance Schema enabled")

        elif key == "8":
            if dolphie.is_mysql_version_at_least("5.7"):
                if dolphie.statements_summary_data:
                    dolphie.statements_summary_data.internal_data = {}
                    dolphie.statements_summary_data.filtered_data = {}
                self.app.toggle_panel(dolphie.panels.statements_summary.name)
            else:
                self.app.notify("Statements Summary panel requires MySQL 5.7+ with Performance Schema enabled")

        # Tab management commands
        elif key == "grave_accent":
            self.app.tab_manager.setup_host_tab(tab)

        elif key == "space":
            if not tab.worker or not tab.worker.is_running:
                if tab.worker_timer:
                    tab.worker_timer.stop()
                self.app.run_worker_main(tab.id)

        elif key == "plus":
            new_tab = await self.app.tab_manager.create_tab(tab_name="New Tab")
            self.app.tab_manager.switch_tab(new_tab.id)
            self.app.tab_manager.setup_host_tab(new_tab)

        elif key == "equals_sign":

            def command_get_input(tab_name):
                tab.manual_tab_name = tab_name
                self.app.tab_manager.rename_tab(tab, tab_name)

            self.app.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.rename_tab,
                    message="What would you like to rename the tab to?",
                ),
                command_get_input,
            )

        elif key == "minus":
            if len(self.app.tab_manager.tabs) == 1:
                self.app.notify("Removing all tabs is not permitted", severity="error")
            else:
                if not self.app.tab_manager.active_tab:
                    self.app.notify("No active tab to remove", severity="error")
                    return

                await self.app.tab_manager.remove_tab(tab)
                await self.app.tab_manager.disconnect_tab(tab=tab, update_topbar=False)

                self.app.notify(
                    f"Tab [$highlight]{tab.name}[/$highlight] [$white]has been removed",
                    severity="success",
                )
                self.app.tab_manager.tabs.pop(tab.id, None)

        # Replay control commands
        elif key == "left_square_bracket":
            if dolphie.replay_file:
                self.app.query_one("#back_button", Button).press()

        elif key == "right_square_bracket":
            if dolphie.replay_file:
                self.app.query_one("#forward_button", Button).press()

        # Tab navigation
        elif key == "ctrl+a" or key == "ctrl+d":
            if key == "ctrl+a":
                self.app.tab_manager.host_tabs.action_previous_tab()
            elif key == "ctrl+d":
                self.app.tab_manager.host_tabs.action_next_tab()

        # Display toggle commands
        elif key == "a":
            if dolphie.show_additional_query_columns:
                dolphie.show_additional_query_columns = False
                self.app.notify("Processlist will now hide additional columns")
            else:
                dolphie.show_additional_query_columns = True
                self.app.notify("Processlist will now show additional columns")

            self.app.force_refresh_for_replay(need_current_data=True)

        elif key == "A":
            if dolphie.show_statements_summary_query_digest_text_sample:
                dolphie.show_statements_summary_query_digest_text_sample = False
                self.app.notify("Statements Summary will now show query digest")
            else:
                dolphie.show_statements_summary_query_digest_text_sample = True
                self.app.notify("Statements Summary will now show query digest sample")

        # Filter commands
        elif key == "c":
            dolphie.user_filter = None
            dolphie.db_filter = None
            dolphie.host_filter = None
            dolphie.hostgroup_filter = None
            dolphie.query_time_filter = None
            dolphie.query_filter = None

            self.app.force_refresh_for_replay(need_current_data=True)

            self.app.notify("Cleared all filters", severity="success")

        elif key == "C":
            if not dolphie.global_variables.get("innodb_thread_concurrency"):
                self.app.notify("InnoDB thread concurrency is not setup", severity="warning")
                return

            if dolphie.show_threads_with_concurrency_tickets:
                dolphie.show_threads_with_concurrency_tickets = False
                dolphie.show_idle_threads = False
                self.app.notify("Processlist will no longer only show threads with concurrency tickets")
            else:
                dolphie.show_threads_with_concurrency_tickets = True
                dolphie.show_idle_threads = True
                self.app.notify("Processlist will only show threads with concurrency tickets")

            self.app.force_refresh_for_replay(need_current_data=True)

        # Database operation commands
        elif key == "d":
            self.execute_command_in_thread(key=key)

        elif key == "D":
            await self.app.tab_manager.disconnect_tab(tab)

        elif key == "e":
            if dolphie.connection_source_alt == ConnectionSource.mariadb:
                self.app.notify(f"Command [$highlight]{key}[/$highlight] is only available for MySQL connections")
            elif dolphie.connection_source == ConnectionSource.proxysql:
                self.execute_command_in_thread(key=key)
            else:
                if dolphie.is_mysql_version_at_least("8.0") and dolphie.performance_schema_enabled:
                    self.app.app.push_screen(
                        EventLog(
                            dolphie.connection_status,
                            dolphie.app_version,
                            dolphie.host_with_port,
                            dolphie.secondary_db_connection,
                        )
                    )
                else:
                    self.app.notify("Error log command requires MySQL 8+ with Performance Schema enabled")

        elif key == "E":
            processlist = dolphie.processlist_threads_snapshot or dolphie.processlist_threads
            if processlist:
                # Extract headers from the first entry's thread_data
                first_entry = next(iter(processlist.values()))
                headers = first_entry.thread_data.keys()

                # Generate the filename with a timestamp prefix
                timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
                filename = f"processlist-{timestamp}.csv"

                # Write the CSV to a file
                with open(filename, "w", newline="") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=headers)

                    # Write the headers and rows
                    writer.writeheader()
                    for process_thread in processlist.values():
                        writer.writerow(process_thread.thread_data)

                self.app.notify(
                    f"Processlist has been exported to CSV file [$highlight]{filename}",
                    severity="success",
                    timeout=10,
                )
            else:
                self.app.notify("There's no processlist data to export", severity="warning")

        elif key == "f":

            def command_get_input(filter_data):
                # Unpack the data from the modal
                filters_mapping = {
                    "User": "user_filter",
                    "Host": "host_filter",
                    "Database": "db_filter",
                    "Hostgroup": "hostgroup_filter",
                    "Minimum Query Time": "query_time_filter",
                    "Partial Query Text": "query_filter",
                }

                filters = dict(zip(filters_mapping.keys(), filter_data))

                # Apply filters and notify the user for each valid input
                for filter_name, filter_value in filters.items():
                    if filter_value:
                        if filter_name in ["Minimum Query Time", "Hostgroup"]:
                            filter_value = int(filter_value)

                        setattr(dolphie, filters_mapping[filter_name], filter_value)
                        self.app.notify(
                            f"[b]{filter_name}[/b]: [$b_highlight]{filter_value}[/$b_highlight]",
                            title="Filter applied",
                            severity="success",
                        )

                # Refresh data after applying filters
                self.app.force_refresh_for_replay(need_current_data=True)

            self.app.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.thread_filter,
                    message="Filter threads by field(s)",
                    processlist_data=dolphie.processlist_threads_snapshot,
                    host_cache_data=dolphie.host_cache,
                    connection_source=dolphie.connection_source,
                ),
                command_get_input,
            )

        elif key == "i":
            if dolphie.show_idle_threads:
                dolphie.show_idle_threads = False
                dolphie.sort_by_time_descending = True

                self.app.notify("Processlist will now hide idle threads")
            else:
                dolphie.show_idle_threads = True
                dolphie.sort_by_time_descending = False

                self.app.notify("Processlist will now show idle threads")

        elif key == "k":

            def command_get_input(data):
                self.execute_command_in_thread(key=key, additional_data=data)

            self.app.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.thread_kill_by_parameter,
                    message="Kill thread(s)",
                    processlist_data=dolphie.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "l" or key == "o":
            self.execute_command_in_thread(key=key)

        elif key == "m":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.execute_command_in_thread(key=key)
                return

            if not dolphie.is_mysql_version_at_least("5.7") or not dolphie.performance_schema_enabled:
                self.app.notify("Memory usage command requires MySQL 5.7+ with Performance Schema enabled")
            else:
                self.execute_command_in_thread(key=key)

        elif key == "M":

            def command_get_input(filter_data):
                panel = filter_data

                widget = None
                if panel == "processlist":
                    widget = tab.processlist_datatable
                elif panel == "graphs":
                    widget = tab.metric_graph_tabs
                elif panel == "metadata_locks":
                    widget = tab.metadata_locks_datatable
                elif panel == "ddl":
                    widget = tab.ddl_datatable
                elif panel == "pfs_metrics":
                    widget = tab.pfs_metrics_tabs
                elif panel == "statements_summary":
                    widget = tab.statements_summary_datatable
                elif panel == "proxysql_hostgroup_summary":
                    widget = tab.proxysql_hostgroup_summary_datatable
                elif panel == "proxysql_mysql_query_rules":
                    widget = tab.proxysql_mysql_query_rules_datatable
                elif panel == "proxysql_command_stats":
                    widget = tab.proxysql_command_stats_datatable

                if widget:
                    self.app.screen.maximize(widget)

            panel_options = [
                (panel.display_name, panel.name)
                for panel in tab.dolphie.panels.get_all_panels()
                if panel.visible and panel.name not in ["dashboard"]
            ]

            self.app.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.maximize_panel,
                    maximize_panel_options=panel_options,
                    message="Maximize a Panel",
                ),
                command_get_input,
            )

        elif key == "p":
            if dolphie.replay_file:
                self.app.query_one("#pause_button", Button).press()
            else:
                if not dolphie.pause_refresh:
                    dolphie.pause_refresh = True
                    self.app.notify(f"Refresh is paused! Press [$b_highlight]{key}[/$b_highlight] again to resume")
                else:
                    dolphie.pause_refresh = False
                    self.app.notify("Refreshing has resumed", severity="success")

        if key == "P":
            if dolphie.use_performance_schema_for_processlist:
                dolphie.use_performance_schema_for_processlist = False
                self.app.notify(
                    "Switched to using [$b_highlight]Information Schema[/$b_highlight] for Processlist panel"
                )
            else:
                if dolphie.performance_schema_enabled:
                    dolphie.use_performance_schema_for_processlist = True
                    self.app.notify(
                        "Switched to using [$b_highlight]Performance Schema[/$b_highlight] for Processlist panel"
                    )
                else:
                    self.app.notify(
                        "You can't switch to [$b_highlight]Performance Schema[/$b_highlight] for "
                        "Processlist panel because it isn't enabled",
                        severity="warning",
                    )

        elif key == "q":
            self.app.app.exit()

        elif key == "r":

            def command_get_input(refresh_interval):
                dolphie.refresh_interval = refresh_interval

                self.app.notify(
                    f"Refresh interval set to [$b_highlight]{refresh_interval}[/$b_highlight] second(s)",
                    severity="success",
                )

            self.app.app.push_screen(
                CommandModal(HotkeyCommands.refresh_interval, message="Refresh Interval"),
                command_get_input,
            )

        elif key == "R":
            dolphie.metric_manager.reset()
            dolphie.reset_pfs_metrics_deltas()

            self.app.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)
            dolphie.update_switches_after_reset()
            self.app.notify("Metrics have been reset", severity="success")

        elif key == "s":
            if dolphie.sort_by_time_descending:
                dolphie.sort_by_time_descending = False
                self.app.notify("Processlist will now sort threads by time in ascending order")
            else:
                dolphie.sort_by_time_descending = True
                self.app.notify("Processlist will now sort threads by time in descending order")

            self.app.force_refresh_for_replay(need_current_data=True)

        elif key == "S":
            if dolphie.replay_file:
                self.app.query_one("#seek_button", Button).press()

        elif key == "t":
            if dolphie.connection_source == ConnectionSource.proxysql:

                def command_get_input(data):
                    thread_table = Table(box=None, show_header=False)
                    thread_table.add_column("")
                    thread_table.add_column("", overflow="fold")

                    thread_id = data
                    thread_data: ProxySQLProcesslistThread = dolphie.processlist_threads_snapshot.get(thread_id)
                    if not thread_data:
                        self.app.notify(
                            f"Thread ID [$highlight]{thread_id}[/$highlight] was not found",
                            severity="error",
                        )
                        return

                    thread_table.add_row("[label]Process ID", thread_id)
                    thread_table.add_row("[label]Hostgroup", str(thread_data.hostgroup))
                    thread_table.add_row("[label]User", thread_data.user)
                    thread_table.add_row("[label]Frontend Host", thread_data.frontend_host)
                    thread_table.add_row("[label]Backend Host", thread_data.host)
                    thread_table.add_row("[label]Database", thread_data.db)
                    thread_table.add_row("[label]Command", thread_data.command)
                    thread_table.add_row("[label]Time", str(timedelta(seconds=thread_data.time)).zfill(8))

                    formatted_query = None
                    if thread_data.formatted_query.code:
                        query = sqlformat(thread_data.formatted_query.code, reindent_aligned=True)
                        formatted_query = format_query(query, minify=False)

                    self.app.app.push_screen(
                        ProxySQLThreadScreen(
                            connection_status=dolphie.connection_status,
                            app_version=dolphie.app_version,
                            host=dolphie.host_with_port,
                            thread_table=thread_table,
                            query=formatted_query,
                            extended_info=thread_data.extended_info,
                        )
                    )

            else:

                def command_get_input(data):
                    self.execute_command_in_thread(key=key, additional_data=data)

            self.app.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.show_thread,
                    message="Thread Details",
                    processlist_data=dolphie.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "T":
            if dolphie.show_trxs_only:
                dolphie.show_trxs_only = False
                dolphie.show_idle_threads = False
                self.app.notify("Processlist will no longer only show threads that have an active transaction")
            else:
                dolphie.show_trxs_only = True
                dolphie.show_idle_threads = True
                self.app.notify("Processlist will only show threads that have an active transaction")

            self.app.force_refresh_for_replay(need_current_data=True)

        elif key == "u":
            if not dolphie.performance_schema_enabled and dolphie.connection_source != ConnectionSource.proxysql:
                self.app.notify("User statistics command requires Performance Schema to be enabled")
                return

            self.execute_command_in_thread(key=key)

        elif key == "V":
            global_variable_changes = tab.replay_manager.fetch_all_global_variable_changes()

            if global_variable_changes:
                table = Table(
                    box=box.SIMPLE_HEAVY,
                    show_edge=False,
                    style="table_border",
                )
                table.add_column("Timestamp")
                table.add_column("Variable")
                table.add_column("Old Value", overflow="fold")
                table.add_column("New Value", overflow="fold")

                for (
                    timestamp,
                    variable,
                    old_value,
                    new_value,
                ) in global_variable_changes:
                    table.add_row(
                        f"[dark_gray]{timestamp}",
                        f"[light_blue]{variable}",
                        old_value,
                        new_value,
                    )

                screen_data = Group(
                    Align.center(
                        "[b light_blue]Global Variable Changes[/b light_blue] "
                        f"([b highlight]{table.row_count}[/b highlight])\n"
                    ),
                    table,
                )
            else:
                self.app.notify("There are no global variable changes in this replay")

        elif key == "v":

            def command_get_input(input_variable):
                table_grid = Table.grid()
                table_counter = 1
                variable_counter = 1
                row_counter = 1
                variable_num = 1
                all_tables = []
                tables = {}
                display_global_variables = {}

                for variable, value in dolphie.global_variables.items():
                    if input_variable == "all":
                        display_global_variables[variable] = dolphie.global_variables[variable]
                    else:
                        if input_variable and input_variable in variable:
                            display_global_variables[variable] = dolphie.global_variables[variable]

                max_num_tables = 1 if len(display_global_variables) <= 50 else 2

                # Create the number of tables we want
                while table_counter <= max_num_tables:
                    tables[table_counter] = Table(box=box.HORIZONTALS, show_header=False, style="table_border")
                    tables[table_counter].add_column("")
                    tables[table_counter].add_column("")

                    table_counter += 1

                # Calculate how many global_variables per table
                row_per_count = len(display_global_variables) // max_num_tables

                # Loop global_variables
                for variable, value in display_global_variables.items():
                    tables[variable_num].add_row(f"[label]{variable}", str(value))

                    if variable_counter == row_per_count and row_counter != max_num_tables:
                        row_counter += 1
                        variable_counter = 0
                        variable_num += 1

                    variable_counter += 1

                # Put all the variable data from dict into an array
                all_tables = [table_data for table_data in tables.values() if table_data]

                # Add the data into a single tuple for add_row
                if display_global_variables:
                    table_grid.add_row(*all_tables)
                    screen_data = Align.center(table_grid)

                    self.app.app.push_screen(
                        CommandScreen(
                            dolphie.connection_status,
                            dolphie.app_version,
                            dolphie.host_with_port,
                            screen_data,
                        )
                    )
                else:
                    if input_variable:
                        self.app.notify(
                            f"No variable(s) found that match [$b_highlight]{input_variable}[/$b_highlight]"
                        )

            self.app.app.push_screen(
                CommandModal(
                    HotkeyCommands.variable_search,
                    message="Specify a variable to wildcard search",
                ),
                command_get_input,
            )

        elif key == "Z":
            if dolphie.is_mysql_version_at_least("5.7"):
                self.execute_command_in_thread(key=key)
            else:
                self.app.notify("Table size command requires MySQL 5.7+")

        elif key == "z":
            if dolphie.host_cache:
                table = Table(
                    box=box.SIMPLE_HEAVY,
                    show_edge=False,
                    style="table_border",
                )
                table.add_column("Host/IP")
                table.add_column("Hostname (if resolved)")

                for ip, addr in dolphie.host_cache.items():
                    if ip:
                        table.add_row(ip, addr)

                screen_data = Group(
                    Align.center(
                        f"[b light_blue]Host Cache[/b light_blue] "
                        f"([b highlight]{len(dolphie.host_cache)}[/b highlight])\n"
                    ),
                    table,
                )
            else:
                self.app.notify("There are currently no hosts resolved")

        if screen_data:
            self.app.app.push_screen(
                CommandScreen(
                    dolphie.connection_status,
                    dolphie.app_version,
                    dolphie.host_with_port,
                    screen_data,
                )
            )

    def execute_command_in_thread(self, key: str, additional_data=None) -> None:
        """Execute a command in a background thread.

        This method creates a daemon thread to run the command without blocking
        the UI, using call_from_thread to safely interact with Textual.

        Args:
            key: The command key that was pressed
            additional_data: Optional additional data for the command
        """

        def _run_command():
            """Internal worker function that executes in a background thread."""
            self._execute_command(key, additional_data)

        thread = threading.Thread(target=_run_command, daemon=True)
        thread.start()

    def _execute_command(self, key: str, additional_data=None) -> None:
        """Internal implementation of command execution."""
        tab = self.app.tab_manager.active_tab
        dolphie = tab.dolphie

        # These are the screens to display we use for the commands
        def show_command_screen():
            self.app.app.push_screen(
                CommandScreen(
                    dolphie.connection_status,
                    dolphie.app_version,
                    dolphie.host_with_port,
                    screen_data,
                )
            )

        def show_thread_screen():
            self.app.app.push_screen(
                ThreadScreen(
                    connection_status=dolphie.connection_status,
                    app_version=dolphie.app_version,
                    host=dolphie.host_with_port,
                    thread_table=thread_table,
                    user_thread_attributes_table=user_thread_attributes_table,
                    query=formatted_query,
                    explain_data=explain_data,
                    explain_json_data=explain_json_data,
                    explain_failure=explain_failure,
                    transaction_history_table=transaction_history_table,
                )
            )

        self.app.call_from_thread(tab.spinner.show)

        try:
            if key == "d":
                tables = {}
                all_tables = []

                db_count = dolphie.secondary_db_connection.execute(MySQLQueries.databases)
                databases = dolphie.secondary_db_connection.fetchall()

                # Determine how many tables to provide data
                max_num_tables = 1 if db_count <= 20 else 3

                # Calculate how many databases per table
                row_per_count = db_count // max_num_tables

                # Create dictionary of tables
                for table_counter in range(1, max_num_tables + 1):
                    table_box = box.HORIZONTALS
                    if max_num_tables == 1:
                        table_box = None

                    tables[table_counter] = Table(box=table_box, show_header=False, style="table_border")
                    tables[table_counter].add_column("")

                # Loop over databases
                db_counter = 1
                table_counter = 1

                # Sort the databases by name
                for database in databases:
                    tables[table_counter].add_row(database["SCHEMA_NAME"])
                    db_counter += 1

                    if db_counter > row_per_count and table_counter < max_num_tables:
                        table_counter += 1
                        db_counter = 1

                # Collect table data into an array
                all_tables = [table_data for table_data in tables.values() if table_data]

                table_grid = Table.grid()
                table_grid.add_row(*all_tables)

                screen_data = Group(
                    Align.center(f"[b light_blue]Databases[/b light_blue] ([b highlight]{db_count}[/b highlight])\n"),
                    Align.center(table_grid),
                )

                self.app.call_from_thread(show_command_screen)

            elif key == "e":
                header_style = Style(bold=True)
                table = Table(box=box.SIMPLE_HEAVY, style="table_border", show_edge=False)
                table.add_column("Hostgroup", header_style=header_style)
                table.add_column("Backend Host", max_width=35, header_style=header_style)
                table.add_column("Username", header_style=header_style)
                table.add_column("Schema", header_style=header_style)
                table.add_column("First Seen", header_style=header_style)
                table.add_column("Last Seen", header_style=header_style)
                table.add_column("Count", header_style=header_style)
                table.add_column("Error", header_style=header_style, overflow="fold")

                dolphie.secondary_db_connection.execute(ProxySQLQueries.query_errors)
                data = dolphie.secondary_db_connection.fetchall()

                for row in data:
                    table.add_row(
                        row.get("hostgroup"),
                        f"{dolphie.get_hostname(row.get('hostname'))}:{row.get('port')}",
                        row.get("username"),
                        row.get("schemaname"),
                        str(datetime.fromtimestamp(int(row.get("first_seen", 0))).astimezone()),
                        str(datetime.fromtimestamp(int(row.get("last_seen", 0))).astimezone()),
                        format_number(int(row.get("count_star", 0))),
                        "[b][highlight]{}[/b][/highlight]: {}".format(row.get("errno", 0), row.get("last_error")),
                    )

                screen_data = Group(
                    Align.center(f"[b light_blue]Query Errors ([highlight]{table.row_count}[/highlight])\n"),
                    Align.center(table),
                )

                self.app.call_from_thread(show_command_screen)

            elif key == "k":
                # Unpack the data from the modal
                (
                    kill_by_id,
                    kill_by_username,
                    kill_by_host,
                    kill_by_age_range,
                    age_range_lower_limit,
                    age_range_upper_limit,
                    kill_by_query_text,
                    include_sleeping_queries,
                ) = additional_data

                if kill_by_id:
                    try:
                        query = dolphie.build_kill_query(kill_by_id)
                        dolphie.secondary_db_connection.execute(query)

                        self.app.notify(
                            f"Killed Thread ID [$b_highlight]{kill_by_id}[/$b_highlight]",
                            severity="success",
                        )
                    except ManualException as e:
                        self.app.notify(e.reason, title="Error killing Thread ID", severity="error")
                else:
                    threads_killed = 0
                    commands_to_kill = ["Query", "Execute"]

                    if include_sleeping_queries:
                        commands_to_kill.append("Sleep")

                    # Make a copy of the threads snapshot to avoid modification during next refresh polling
                    threads = dolphie.processlist_threads_snapshot.copy()

                    for thread_id, thread in threads.items():
                        thread: ProcesslistThread
                        try:
                            # Check if the thread matches all conditions
                            if (
                                thread.command in commands_to_kill
                                and (not kill_by_username or kill_by_username == thread.user)
                                and (not kill_by_host or kill_by_host == thread.host)
                                and (
                                    not kill_by_age_range
                                    or age_range_lower_limit <= thread.time <= age_range_upper_limit
                                )
                                and (not kill_by_query_text or kill_by_query_text in thread.formatted_query.code)
                            ):
                                query = dolphie.build_kill_query(thread_id)
                                dolphie.secondary_db_connection.execute(query)

                                threads_killed += 1
                        except ManualException as e:
                            self.app.notify(
                                e.reason,
                                title=f"Error Killing Thread ID {thread_id}",
                                severity="error",
                            )

                    if threads_killed:
                        self.app.notify(f"Killed [$highlight]{threads_killed}[/$highlight] thread(s)")
                    else:
                        self.app.notify("No threads were killed")

            elif key == "l":
                status = dolphie.secondary_db_connection.fetch_value_from_field(MySQLQueries.innodb_status, "Status")
                # Extract the most recent deadlock info from the output of SHOW ENGINE INNODB STATUS
                match = re.search(
                    r"------------------------\nLATEST\sDETECTED\sDEADLOCK\n------------------------"
                    r"\n(.*?)------------\nTRANSACTIONS",
                    status,
                    flags=re.S,
                )

                if match:
                    screen_data = escape_markup(match.group(1)).replace("***", "[yellow]*****[/yellow]")
                else:
                    screen_data = Align.center("No deadlock detected")

                self.app.call_from_thread(show_command_screen)

            elif key == "o":
                screen_data = escape_markup(
                    dolphie.secondary_db_connection.fetch_value_from_field(MySQLQueries.innodb_status, "Status")
                )
                self.app.call_from_thread(show_command_screen)

            elif key == "m":
                header_style = Style(bold=True)

                if dolphie.connection_source == ConnectionSource.proxysql:
                    table = Table(box=box.SIMPLE_HEAVY, style="table_border", show_edge=False)
                    table.add_column("Variable", header_style=header_style)
                    table.add_column("Value", header_style=header_style)

                    dolphie.secondary_db_connection.execute(ProxySQLQueries.memory_metrics)
                    data = dolphie.secondary_db_connection.fetchall()

                    for row in data:
                        if row["Variable_Name"]:
                            table.add_row(
                                f"{row['Variable_Name']}",
                                f"{format_bytes(int(row['Variable_Value']))}",
                            )

                    screen_data = Group(
                        Align.center("[b light_blue]Memory Usage[/b light_blue]"),
                        Align.center(table),
                    )

                    self.app.call_from_thread(show_command_screen)
                else:
                    table_grid = Table.grid()
                    table1 = Table(box=box.SIMPLE_HEAVY, style="table_border")

                    table1.add_column("User", header_style=header_style)
                    table1.add_column("Current", header_style=header_style)
                    table1.add_column("Total", header_style=header_style)

                    dolphie.secondary_db_connection.execute(MySQLQueries.memory_by_user)
                    data = dolphie.secondary_db_connection.fetchall()
                    for row in data:
                        table1.add_row(
                            row["user"],
                            format_sys_table_memory(row["current_allocated"]),
                            format_sys_table_memory(row["total_allocated"]),
                        )

                    table2 = Table(box=box.SIMPLE_HEAVY, style="table_border")
                    table2.add_column("Code Area", header_style=header_style)
                    table2.add_column("Current", header_style=header_style)

                    dolphie.secondary_db_connection.execute(MySQLQueries.memory_by_code_area)
                    data = dolphie.secondary_db_connection.fetchall()
                    for row in data:
                        table2.add_row(
                            row["code_area"],
                            format_sys_table_memory(row["current_allocated"]),
                        )

                    table3 = Table(box=box.SIMPLE_HEAVY, style="table_border")
                    table3.add_column("Host", header_style=header_style)
                    table3.add_column("Current", header_style=header_style)
                    table3.add_column("Total", header_style=header_style)

                    dolphie.secondary_db_connection.execute(MySQLQueries.memory_by_host)
                    data = dolphie.secondary_db_connection.fetchall()
                    for row in data:
                        table3.add_row(
                            dolphie.get_hostname(row["host"]),
                            format_sys_table_memory(row["current_allocated"]),
                            format_sys_table_memory(row["total_allocated"]),
                        )

                    table_grid.add_row("", Align.center("[b light_blue]Memory Allocation"), "")
                    table_grid.add_row(table1, table3, table2)

                    screen_data = Align.center(table_grid)

                    self.app.call_from_thread(show_command_screen)
            elif key == "t":
                formatted_query = ""
                explain_failure = ""
                explain_data = ""
                explain_json_data = ""

                thread_table = Table(box=None, show_header=False)
                thread_table.add_column("")
                thread_table.add_column("", overflow="fold")

                thread_id = additional_data
                thread_data: ProcesslistThread = dolphie.processlist_threads_snapshot.get(thread_id)
                if not thread_data:
                    self.app.notify(
                        f"Thread ID [$highlight]{thread_id}[/$highlight] was not found",
                        severity="error",
                    )
                    tab.spinner.hide()
                    return

                thread_table.add_row("[label]Thread ID", thread_id)
                thread_table.add_row("[label]User", thread_data.user)
                thread_table.add_row("[label]Host", thread_data.host)
                thread_table.add_row("[label]Database", thread_data.db)
                thread_table.add_row("[label]Command", thread_data.command)
                thread_table.add_row("[label]State", thread_data.state)
                thread_table.add_row("[label]Time", str(timedelta(seconds=thread_data.time)).zfill(8))
                thread_table.add_row("[label]Rows Locked", format_number(thread_data.trx_rows_locked))
                thread_table.add_row("[label]Rows Modified", format_number(thread_data.trx_rows_modified))

                thread_table.add_row("", "")
                thread_table.add_row("[label]TRX Time", thread_data.trx_time)
                thread_table.add_row("[label]TRX State", thread_data.trx_state)
                thread_table.add_row("[label]TRX Operation", thread_data.trx_operation_state)

                if thread_data.formatted_query.code:
                    query = sqlformat(thread_data.formatted_query.code, reindent_aligned=True)
                    query_db = thread_data.db

                    formatted_query = format_query(query, minify=False)

                    if query_db:
                        try:
                            dolphie.secondary_db_connection.execute(f"USE {query_db}")

                            dolphie.secondary_db_connection.execute(f"EXPLAIN {query}")
                            explain_data = dolphie.secondary_db_connection.fetchall()

                            dolphie.secondary_db_connection.execute(f"EXPLAIN FORMAT=JSON {query}")
                            explain_fetched_json_data = dolphie.secondary_db_connection.fetchone()
                            if explain_fetched_json_data:
                                explain_json_data = explain_fetched_json_data.get("EXPLAIN")
                        except ManualException as e:
                            # Error 1054 means unknown column which would result in a truncated query
                            # Error 1064 means bad syntax which would result in a truncated query
                            tip = (
                                ":bulb: [b][yellow]Tip![/b][/yellow] If the query is truncated, consider "
                                "increasing [dark_yellow]performance_schema_max_digest_length[/dark_yellow]/"
                                "[dark_yellow]max_digest_length[/dark_yellow] as a preventive measure. "
                                "If adjusting those settings isn't an option, then use command "
                                "[dark_yellow]P[/dark_yellow]. "
                                "This will switch to using SHOW PROCESSLIST instead of the Performance Schema, "
                                "which does not truncate queries.\n\n"
                                if e.code in (1054, 1064)
                                else ""
                            )

                            explain_failure = (
                                f"{tip}[b][indian_red]EXPLAIN ERROR ({e.code}):[/b] [indian_red]{e.reason}"
                            )

                user_thread_attributes_table = None
                if dolphie.performance_schema_enabled:
                    user_thread_attributes_table = Table(box=None, show_header=False, expand=True)

                    dolphie.secondary_db_connection.execute(
                        MySQLQueries.user_thread_attributes.replace("$1", thread_id)
                    )

                    user_thread_attributes = dolphie.secondary_db_connection.fetchall()
                    if user_thread_attributes:
                        user_thread_attributes_table.add_column("")
                        user_thread_attributes_table.add_column("", overflow="fold")

                        for attribute in user_thread_attributes:
                            user_thread_attributes_table.add_row(
                                f"[label]{attribute['ATTR_NAME']}",
                                attribute["ATTR_VALUE"],
                            )
                    else:
                        user_thread_attributes_table.add_column(justify="center")
                        user_thread_attributes_table.add_row("[b][label]None found")

                # Transaction history
                transaction_history_table = None
                if (
                    dolphie.is_mysql_version_at_least("5.7")
                    and dolphie.performance_schema_enabled
                    and thread_data.mysql_thread_id
                ):
                    query = MySQLQueries.thread_transaction_history.replace("$1", str(thread_data.mysql_thread_id))
                    dolphie.secondary_db_connection.execute(query)
                    transaction_history = dolphie.secondary_db_connection.fetchall()

                    if transaction_history:
                        transaction_history_table = Table(box=None)
                        transaction_history_table.add_column("Start Time")
                        transaction_history_table.add_column("Query", overflow="fold")

                        for query in transaction_history:
                            trx_history_formatted_query = ""
                            if query["sql_text"]:
                                trx_history_formatted_query = format_query(
                                    sqlformat(query["sql_text"], reindent_aligned=True),
                                    minify=False,
                                )

                            transaction_history_table.add_row(
                                query["start_time"].strftime("%Y-%m-%d %H:%M:%S"),
                                trx_history_formatted_query,
                            )

                self.app.call_from_thread(show_thread_screen)

            elif key == "u":
                if dolphie.connection_source == ConnectionSource.proxysql:
                    title = "Frontend Users"

                    dolphie.secondary_db_connection.execute(ProxySQLQueries.user_stats)
                    users = dolphie.secondary_db_connection.fetchall()

                    columns = {
                        "User": {"field": "username", "format_number": False},
                        "Active": {
                            "field": "frontend_connections",
                            "format_number": True,
                        },
                        "Max": {
                            "field": "frontend_max_connections",
                            "format_number": True,
                        },
                        "Default HG": {
                            "field": "default_hostgroup",
                            "format_number": False,
                        },
                        "Default Schema": {
                            "field": "default_schema",
                            "format_number": False,
                        },
                        "SSL": {"field": "use_ssl", "format_number": False},
                    }

                    table = Table(
                        header_style="b",
                        box=box.SIMPLE_HEAVY,
                        show_edge=False,
                        style="table_border",
                    )
                    for column, data in columns.items():
                        table.add_column(column, no_wrap=True)

                    for user in users:
                        row_values = []

                        for column, data in columns.items():
                            value = user.get(data["field"], "N/A")

                            if data["format_number"]:
                                row_values.append(format_number(value) if value else "0")
                            elif column == "SSL":
                                row_values.append("ON" if value == "1" else "OFF")
                            else:
                                row_values.append(value or "")

                        table.add_row(*row_values)
                else:
                    title = "Users"

                    if dolphie.is_mysql_version_at_least("5.7"):
                        dolphie.secondary_db_connection.execute(MySQLQueries.ps_user_statisitics)
                    else:
                        dolphie.secondary_db_connection.execute(MySQLQueries.ps_user_statisitics_56)

                    users = dolphie.secondary_db_connection.fetchall()

                    columns = {
                        "User": {"field": "user", "format_number": False},
                        "Active": {
                            "field": "current_connections",
                            "format_number": True,
                        },
                        "Total": {"field": "total_connections", "format_number": True},
                        "Rows Read": {"field": "rows_examined", "format_number": True},
                        "Rows Sent": {"field": "rows_sent", "format_number": True},
                        "Rows Updated": {
                            "field": "rows_affected",
                            "format_number": True,
                        },
                        "Tmp Tables": {
                            "field": "created_tmp_tables",
                            "format_number": True,
                        },
                        "Tmp Disk Tables": {
                            "field": "created_tmp_disk_tables",
                            "format_number": True,
                        },
                        "Plugin": {"field": "plugin", "format_number": False},
                    }

                    table = Table(
                        header_style="b",
                        box=box.SIMPLE_HEAVY,
                        show_edge=False,
                        style="table_border",
                    )
                    for column, data in columns.items():
                        table.add_column(column, no_wrap=True)

                    for user in users:
                        row_values = []

                        for column, data in columns.items():
                            value = user.get(data.get("field"), "N/A")

                            if data["format_number"]:
                                row_values.append(format_number(value) if value else "0")
                            else:
                                row_values.append(value or "")

                        table.add_row(*row_values)

                screen_data = Group(
                    Align.center(f"[b light_blue]{title} Connected ([highlight]{len(users)}[/highlight])\n"),
                    Align.center(table),
                )

                self.app.call_from_thread(show_command_screen)

            elif key == "Z":
                query = MySQLQueries.table_sizes.replace("$1", "INNODB_SYS_TABLESPACES")
                if dolphie.is_mysql_version_at_least("8.0") and dolphie.connection_source_alt == ConnectionSource.mysql:
                    query = MySQLQueries.table_sizes.replace("$1", "INNODB_TABLESPACES")

                dolphie.secondary_db_connection.execute(query)
                database_tables_data = dolphie.secondary_db_connection.fetchall()

                columns = {
                    "Table": {"field": "DATABASE_TABLE", "format_bytes": False},
                    "Engine": {"field": "ENGINE", "format_bytes": False},
                    "Row Format": {"field": "ROW_FORMAT", "format_bytes": False},
                    "File Size": {"field": "FILE_SIZE", "format_bytes": True},
                    "Allocated Size": {"field": "ALLOCATED_SIZE", "format_bytes": True},
                    "Clustered Index": {"field": "DATA_LENGTH", "format_bytes": True},
                    "Secondary Indexes": {
                        "field": "INDEX_LENGTH",
                        "format_bytes": True,
                    },
                    "Free Space": {"field": "DATA_FREE", "format_bytes": True},
                    "Frag Ratio": {
                        "field": "fragmentation_ratio",
                        "format_bytes": False,
                    },
                }

                table = Table(
                    header_style="b",
                    box=box.SIMPLE_HEAVY,
                    show_edge=False,
                    style="table_border",
                )
                for column, data in columns.items():
                    table.add_column(column, no_wrap=True)

                for database_table_row in database_tables_data:
                    row_values = []

                    for column, data in columns.items():
                        value = database_table_row.get(data.get("field"), "N/A")

                        if data["format_bytes"]:
                            row_values.append(format_bytes(value) if value else "0")
                        elif column == "Frag Ratio":
                            if value:
                                if value >= 30:
                                    row_values.append(f"[red]{value}%[/red]")
                                elif value >= 20:
                                    row_values.append(f"[yellow]{value}%[/yellow]")
                                else:
                                    row_values.append(f"[green]{value}%[/green]")
                            else:
                                row_values.append("[green]0%[/green]")

                        elif column == "Table":
                            # Color the database name. Format is database/table
                            database_table = value.split(".")
                            row_values.append(f"[dark_gray]{database_table[0]}[/dark_gray].{database_table[1]}")

                        else:
                            row_values.append(str(value) or "")

                    table.add_row(*row_values)

                screen_data = Group(
                    Align.center(
                        f"[b light_blue]Table Sizes & Fragmentation[/b light_blue] "
                        f"([highlight]{len(database_tables_data)}[/highlight])\n"
                    ),
                    Align.center(table),
                )

                self.app.call_from_thread(show_command_screen)
        except ManualException as e:
            self.app.notify(
                e.reason,
                title=f"Error running command '{key}'",
                severity="error",
                timeout=10,
            )

        self.app.call_from_thread(tab.spinner.hide)
