from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING

from textual.worker import Worker, WorkerState, get_current_worker

import dolphie.Modules.MetricManager as MetricManager
from dolphie.DataTypes import ConnectionSource, ConnectionStatus
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.ReplayManager import ReplayManager
from dolphie.Panels import Replication as ReplicationPanel

if TYPE_CHECKING:
    from dolphie.App import DolphieApp


class WorkerManager:
    """This module handles all worker management operations.

    This includes main refresh worker, replicas worker, and replay worker, along with
    their state change handlers.
    """

    def __init__(self, app: "DolphieApp"):
        """Initialize the WorkerManager.

        Args:
            app: Reference to the main DolphieApp instance
        """
        self.app = app

    async def run_worker_replay(self, tab_id: str, manual_control: bool = False):
        tab = self.app.tab_manager.get_tab(tab_id)
        if not tab:
            return

        try:
            # Get our worker thread
            tab.worker = get_current_worker()
            tab.worker.name = tab_id

            dolphie = tab.dolphie

            tab.replay_manual_control = manual_control
            if (
                len(self.app.screen_stack) > 1
                or (dolphie.pause_refresh and not manual_control)
                or tab.id != self.app.tab_manager.active_tab.id
            ):
                return

            # Get the next event from the replay file
            replay_event_data = tab.replay_manager.get_next_refresh_interval()
            # If there's no more events, stop here and cancel the worker
            if not replay_event_data:
                tab.worker.cancel()

                return

            tab.replay_manager.fetch_global_variable_changes_for_current_replay_id()

            # Common data for refreshing
            dolphie.system_utilization = replay_event_data.system_utilization
            dolphie.global_variables = replay_event_data.global_variables
            dolphie.global_status = replay_event_data.global_status
            common_metrics = {
                "system_utilization": dolphie.system_utilization,
                "global_variables": dolphie.global_variables,
                "global_status": dolphie.global_status,
            }

            dolphie.worker_processing_time = dolphie.global_status.get("replay_polling_latency", 0)

            if dolphie.connection_source == ConnectionSource.mysql:
                dolphie.host_version = dolphie.parse_server_version(dolphie.global_variables.get("version"))
                dolphie.binlog_status = replay_event_data.binlog_status
                dolphie.innodb_metrics = replay_event_data.innodb_metrics
                dolphie.replica_manager.available_replicas = replay_event_data.replica_manager
                dolphie.processlist_threads = replay_event_data.processlist
                dolphie.replication_status = replay_event_data.replication_status
                dolphie.replication_applier_status = replay_event_data.replication_applier_status
                dolphie.metadata_locks = replay_event_data.metadata_locks
                dolphie.group_replication_members = replay_event_data.group_replication_members
                dolphie.group_replication_data = replay_event_data.group_replication_data
                dolphie.file_io_data = replay_event_data.file_io_data
                dolphie.table_io_waits_data = replay_event_data.table_io_waits_data
                dolphie.statements_summary_data = replay_event_data.statements_summary_data

                dolphie.pfs_metrics_last_reset_time = dolphie.global_status.get("pfs_metrics_last_reset_time", 0)

                connection_source_metrics = {
                    "innodb_metrics": dolphie.innodb_metrics,
                    "replication_status": dolphie.replication_status,
                }

                if not dolphie.server_uuid:
                    dolphie.configure_mysql_variables()
            elif dolphie.connection_source == ConnectionSource.proxysql:
                dolphie.host_version = dolphie.parse_server_version(dolphie.global_variables.get("admin-version"))
                dolphie.proxysql_command_stats = replay_event_data.command_stats
                dolphie.proxysql_hostgroup_summary = replay_event_data.hostgroup_summary
                dolphie.processlist_threads = replay_event_data.processlist

                connection_source_metrics = {"proxysql_command_stats": dolphie.proxysql_command_stats}

            # Refresh the metric manager metrics to the state of the replay event
            dolphie.metric_manager.refresh_data(
                worker_start_time=datetime.now().astimezone(),
                **common_metrics,
                **connection_source_metrics,
            )

            # Metrics data is already calculated in the replay event data so we just need to update the values
            dolphie.metric_manager.datetimes = replay_event_data.metric_manager.get("datetimes")
            for metric_name, metric_data in replay_event_data.metric_manager.items():
                metric_instance = dolphie.metric_manager.metrics.__dict__.get(metric_name)
                if metric_instance:
                    for metric_name, metric_values in metric_data.items():
                        metric: MetricManager.MetricData = metric_instance.__dict__.get(metric_name)
                        if metric:
                            metric.values = metric_values
                            metric.last_value = metric_values[-1]

        except Exception as e:
            # Catch any errors during replay and log them without crashing the app
            self.app.notify(
                f"Error during replay: {str(e)}",
                title="Replay Error",
                severity="error",
            )
            if tab.worker:
                tab.worker.cancel()

    async def run_worker_main(self, tab_id: str):
        tab = self.app.tab_manager.get_tab(tab_id)
        if not tab:
            return

        # Get our worker thread
        tab.worker = get_current_worker()
        tab.worker.name = tab_id

        dolphie = tab.dolphie
        try:
            if not dolphie.main_db_connection.is_connected():
                self.app.tab_manager.update_connection_status(tab=tab, connection_status=ConnectionStatus.connecting)

                tab.replay_manager = None
                if not dolphie.daemon_mode and tab == self.app.tab_manager.active_tab:
                    tab.loading_indicator.display = True

                dolphie.db_connect()

            worker_start_time = datetime.now().astimezone()
            dolphie.polling_latency = (worker_start_time - dolphie.worker_previous_start_time).total_seconds()
            dolphie.worker_previous_start_time = worker_start_time

            dolphie.collect_system_utilization()
            if dolphie.connection_source == ConnectionSource.mysql:
                self.app.worker_data_processor.process_mysql_data(tab)
            elif dolphie.connection_source == ConnectionSource.proxysql:
                self.app.worker_data_processor.process_proxysql_data(tab)

            dolphie.worker_processing_time = (datetime.now().astimezone() - worker_start_time).total_seconds()

            dolphie.metric_manager.refresh_data(
                worker_start_time=worker_start_time,
                polling_latency=dolphie.polling_latency,
                system_utilization=dolphie.system_utilization,
                global_variables=dolphie.global_variables,
                global_status=dolphie.global_status,
                innodb_metrics=dolphie.innodb_metrics,
                disk_io_metrics=dolphie.disk_io_metrics,
                metadata_lock_metrics=dolphie.metadata_locks,
                replication_status=dolphie.replication_status,
                proxysql_command_stats=dolphie.proxysql_command_stats,
            )

            # We initalize this here so we have the host version from process_{mysql,proxysql}_data
            if not tab.replay_manager:
                tab.replay_manager = ReplayManager(dolphie)

            tab.replay_manager.capture_state()
        except ManualException as exception:
            # This will set up the worker state change function below to trigger the
            # tab setup modal with the error
            tab.worker_cancel_error = exception

            await self.app.tab_manager.disconnect_tab(tab)

    def run_worker_replicas(self, tab_id: str):
        tab = self.app.tab_manager.get_tab(tab_id)
        if not tab:
            return

        # Get our worker thread
        tab.replicas_worker = get_current_worker()
        tab.replicas_worker.name = tab_id

        dolphie = tab.dolphie

        if dolphie.panels.replication.visible:
            if tab.id != self.app.tab_manager.active_tab.id:
                return

            if dolphie.replica_manager.available_replicas:
                if not dolphie.replica_manager.replicas:
                    tab.replicas_container.display = True
                    tab.replicas_loading_indicator.display = True
                    tab.replicas_title.update(
                        f"[$white][b]Loading [$highlight]{len(dolphie.replica_manager.available_replicas)}"
                        "[/$highlight] replicas...\n"
                    )

                ReplicationPanel.fetch_replicas(tab)
            else:
                tab.replicas_container.display = False
        else:
            # If we're not displaying the replication panel, remove all replica connections
            dolphie.replica_manager.remove_all_replicas()

    def on_worker_state_changed(self, event: Worker.StateChanged):
        if event.state not in [WorkerState.SUCCESS, WorkerState.CANCELLED]:
            return

        tab = self.app.tab_manager.get_tab(event.worker.name)
        if not tab:
            return

        dolphie = tab.dolphie

        if event.worker.group == "main":
            if event.state == WorkerState.SUCCESS:
                self.app.worker_data_processor.monitor_read_only_change(tab)

                refresh_interval = dolphie.refresh_interval
                if dolphie.connection_source == ConnectionSource.proxysql:
                    refresh_interval = dolphie.determine_proxysql_refresh_interval()

                # Skip this if the conditions are right
                if (
                    len(self.app.screen_stack) > 1
                    or dolphie.pause_refresh
                    or not dolphie.main_db_connection.is_connected()
                    or dolphie.daemon_mode
                    or tab.id != self.app.tab_manager.active_tab.id
                ):
                    tab.worker_timer = self.app.set_timer(refresh_interval, partial(self.app.run_worker_main, tab.id))

                    return

                if not tab.main_container.display:
                    tab.toggle_metric_graph_tabs_display()
                    tab.layout_graphs()

                if dolphie.connection_source == ConnectionSource.mysql:
                    self.app.worker_data_processor.refresh_screen_mysql(tab)
                elif dolphie.connection_source == ConnectionSource.proxysql:
                    self.app.worker_data_processor.refresh_screen_proxysql(tab)

                # Update the topbar with the latest replay file size
                if dolphie.record_for_replay:
                    self.app.tab_manager.update_topbar(tab=tab)

                tab.toggle_entities_displays()

                tab.worker_timer = self.app.set_timer(refresh_interval, partial(self.app.run_worker_main, tab.id))
            elif event.state == WorkerState.CANCELLED:
                # Only show the modal if there's a worker cancel error
                if tab.worker_cancel_error:
                    from loguru import logger

                    logger.critical(tab.worker_cancel_error)

                    if self.app.tab_manager.active_tab.id != tab.id or self.app.tab_manager.loading_hostgroups:
                        self.app.notify(
                            (
                                f"[$b_light_blue]{dolphie.host}:{dolphie.port}[/$b_light_blue]: "
                                f"{tab.worker_cancel_error.reason}"
                            ),
                            title="Connection Error",
                            severity="error",
                            timeout=10,
                        )

                    if not self.app.tab_manager.loading_hostgroups:
                        self.app.tab_manager.switch_tab(tab.id)

                        self.app.tab_manager.setup_host_tab(tab)
                        self.app.bell()
        elif event.worker.group == "replicas":
            if event.state == WorkerState.SUCCESS:
                # Skip this if the conditions are right
                if (
                    len(self.app.screen_stack) > 1
                    or dolphie.pause_refresh
                    or tab.id != self.app.tab_manager.active_tab.id
                ):
                    tab.replicas_worker_timer = self.app.set_timer(
                        dolphie.refresh_interval,
                        partial(self.app.run_worker_replicas, tab.id),
                    )
                    return

                if dolphie.panels.replication.visible and dolphie.replica_manager.available_replicas:
                    ReplicationPanel.create_replica_panel(tab)

                tab.replicas_worker_timer = self.app.set_timer(
                    dolphie.refresh_interval,
                    partial(self.app.run_worker_replicas, tab.id),
                )
        elif event.worker.group == "replay" and event.state == WorkerState.SUCCESS:
            if tab.id == self.app.tab_manager.active_tab.id:
                if len(self.app.screen_stack) > 1 or (dolphie.pause_refresh and not tab.replay_manual_control):
                    tab.worker_timer = self.app.set_timer(
                        dolphie.refresh_interval,
                        partial(self.app.run_worker_replay, tab.id),
                    )

                    return
            else:
                # If the tab isn't active, stop the loop
                return

            self.app.worker_data_processor.monitor_read_only_change(tab)

            if not tab.main_container.display:
                tab.toggle_metric_graph_tabs_display()
                tab.layout_graphs()

            if dolphie.connection_source == ConnectionSource.mysql:
                self.app.worker_data_processor.refresh_screen_mysql(tab)
                ReplicationPanel.create_replica_panel(tab)
            elif dolphie.connection_source == ConnectionSource.proxysql:
                self.app.worker_data_processor.refresh_screen_proxysql(tab)

            tab.toggle_entities_displays()

            tab.worker_timer = self.app.set_timer(
                dolphie.refresh_interval,
                partial(self.app.run_worker_replay, tab.id),
            )
