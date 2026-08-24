from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any, cast

from loguru import logger
from textual.worker import Worker, WorkerState, get_current_worker

import dolphie.Modules.MetricManager as MetricManager
from dolphie.DataTypes import ConnectionSource, ConnectionStatus
from dolphie.Modules.Functions import coerce_float, coerce_str
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.MetricDefinitions import MetricData, MetricValue, parse_metric_datetime
from dolphie.Modules.ReplayManager import MySQLReplayData, ProxySQLReplayData, ReplayManager
from dolphie.Modules.WorkerDataProcessor import is_group_replication_primary
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

    @staticmethod
    def _replay_datetimes(metric_payload: Mapping[str, Any]) -> list[str]:
        """Return normalized timestamps from a replay metric payload."""
        datetimes = metric_payload.get("datetimes", [])
        return [coerce_str(value) for value in datetimes] if isinstance(datetimes, list) else []

    @staticmethod
    def _iter_replay_metric_values(
        metric_manager: MetricManager.MetricManager,
        metric_payload: Mapping[str, Any],
    ) -> Iterator[tuple[MetricData, list[MetricValue]]]:
        """Yield known metric fields and their numeric replay values."""
        for metric_name, raw_metric_data in metric_payload.items():
            metric_instance = metric_manager.get_metric_instance(metric_name)
            if metric_instance is None or not isinstance(raw_metric_data, dict):
                continue

            for field_name, raw_values in raw_metric_data.items():
                metric_data = metric_manager.get_metric_data(metric_instance, field_name)
                if metric_data is None or not isinstance(raw_values, list):
                    continue

                values = [value for value in raw_values if isinstance(value, (int, float))]
                if values:
                    yield metric_data, values

    async def run_worker_replay(self, tab_id: str, manual_control: bool = False):
        tab = self.app.tab_manager.get_tab(tab_id)
        if not tab:
            return

        # Bail out if the tab has been reconfigured out of replay mode while a stale
        # worker_timer was still in flight.
        if not tab.dolphie.replay_file or not tab.replay_manager:
            return

        try:
            # Get our worker thread
            tab.worker = get_current_worker()
            tab.worker.name = tab_id

            dolphie = tab.dolphie

            tab.replay_manual_control = manual_control
            active_tab = self.app.tab_manager.active_tab
            if (
                len(self.app.screen_stack) > 1
                or (dolphie.pause_refresh and not manual_control)
                or active_tab is None
                or tab.id != active_tab.id
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
            # global_variables/global_status/innodb_metrics are always int/float/str: MySQL
            # curates them (see fetch_status_and_variables), and the replay file round-trips
            # that same curated data through JSON. Their replay-dataclass fields are typed as
            # the generic DatabaseRow only because that's what a JSON blob deserializes to.
            dolphie.global_variables = cast(dict[str, int | str], replay_event_data.global_variables)
            dolphie.global_status = cast(dict[str, int | float | str], replay_event_data.global_status)
            common_metrics: dict[str, Any] = {
                "system_utilization": dolphie.system_utilization,
                "global_variables": dolphie.global_variables,
                "global_status": dolphie.global_status,
            }

            dolphie.worker_processing_time = coerce_float(dolphie.global_status.get("replay_polling_latency"))

            if dolphie.connection_source == ConnectionSource.mysql:
                if not isinstance(replay_event_data, MySQLReplayData):
                    raise TypeError("MySQL replay returned a non-MySQL payload")

                dolphie.host_version = dolphie.parse_server_version(coerce_str(dolphie.global_variables.get("version")))
                dolphie.binlog_status = replay_event_data.binlog_status
                dolphie.innodb_metrics = cast(dict[str, int | str], replay_event_data.innodb_metrics)
                dolphie.replica_manager.available_replicas = replay_event_data.replica_manager
                dolphie.processlist_threads = dict(replay_event_data.processlist)
                dolphie.replication_status = replay_event_data.replication_status
                dolphie.replication_applier_status = replay_event_data.replication_applier_status
                dolphie.metadata_locks = replay_event_data.metadata_locks
                dolphie.group_replication_members = replay_event_data.group_replication_members
                dolphie.group_replication_data = replay_event_data.group_replication_data
                dolphie.clusterset_instances = replay_event_data.clusterset_instances
                dolphie.galera_cluster_members = replay_event_data.galera_cluster_members
                dolphie.file_io_data = replay_event_data.file_io_data
                dolphie.table_io_waits_data = replay_event_data.table_io_waits_data
                dolphie.statements_summary_data = replay_event_data.statements_summary_data

                reset_age = coerce_float(replay_event_data.global_status.get("replay_pfs_metrics_last_reset_time"))
                dolphie.pfs_metrics_last_reset_time = datetime.now().astimezone() - timedelta(seconds=reset_age)

                connection_source_metrics: dict[str, Any] = {
                    "innodb_metrics": dolphie.innodb_metrics,
                    "replication_status": dolphie.replication_status,
                }

                if not dolphie.server_uuid:
                    dolphie.configure_mysql_variables()
                dolphie.is_group_replication_primary = is_group_replication_primary(
                    dolphie.group_replication_members,
                    dolphie.server_uuid,
                )
            elif dolphie.connection_source == ConnectionSource.proxysql:
                if not isinstance(replay_event_data, ProxySQLReplayData):
                    raise TypeError("ProxySQL replay returned a non-ProxySQL payload")

                dolphie.host_version = dolphie.parse_server_version(
                    coerce_str(dolphie.global_variables.get("admin-version"))
                )
                # stats_mysql_commands_counters is Command (str) plus bigint counters, so this
                # is always int/str even though command_stats is typed as the generic DatabaseRow.
                dolphie.proxysql_command_stats = cast(list[dict[str, int | str]], replay_event_data.command_stats)
                dolphie.proxysql_hostgroup_summary = replay_event_data.hostgroup_summary
                dolphie.processlist_threads = dict(replay_event_data.processlist)

                connection_source_metrics = {"proxysql_command_stats": dolphie.proxysql_command_stats}
            else:
                raise ValueError(f"Unsupported replay connection source: {dolphie.connection_source}")

            # Refresh the metric manager metrics to the state of the replay event
            dolphie.metric_manager.refresh_data(
                worker_start_time=datetime.now().astimezone(),
                **common_metrics,
                **connection_source_metrics,
            )

            # Metrics data is already calculated in the replay event data so we just need to update the values
            is_delta_metrics = replay_event_data.metric_manager.get("_delta", False)
            new_datetimes = self._replay_datetimes(replay_event_data.metric_manager)

            if is_delta_metrics:
                # Delta format from daemon mode
                new_dt = new_datetimes[0] if new_datetimes else None
                last_dt = dolphie.metric_manager.latest_datetime()

                is_sequential = False
                new_dt_parsed = parse_metric_datetime(new_dt) if new_dt is not None else None
                if last_dt is not None and new_dt is not None:
                    last_dt_parsed = parse_metric_datetime(last_dt)
                    if new_dt_parsed is not None and last_dt_parsed is not None:
                        gap = (new_dt_parsed - last_dt_parsed).total_seconds()
                        is_sequential = 0 < gap <= dolphie.refresh_interval * 10

                if is_sequential and new_dt is not None:
                    # Normal forward step: append new delta value
                    metric_values = [
                        (metric_data, values[0])
                        for metric_data, values in self._iter_replay_metric_values(
                            dolphie.metric_manager,
                            replay_event_data.metric_manager,
                        )
                    ]
                    dolphie.metric_manager.append_replay_history(new_dt, metric_values, new_dt_parsed)
                else:
                    # Non-sequential (backward, seek, or first event): rebuild the rolling window.
                    # A disabled window (0) would decompress the entire file on every seek, so
                    # bound the rebuild to the default and let history accumulate forward.
                    metrics_list = tab.replay_manager.fetch_delta_metrics_for_window(
                        tab.replay_manager.current_replay_id,
                        window_minutes=dolphie.metric_manager.rolling_window_minutes
                        or MetricManager.MetricManager.DEFAULT_ROLLING_WINDOW_MINUTES,
                    )

                    # Old format entries contain complete snapshots so skip to the last one
                    # and only accumulate delta entries after it
                    last_full_idx = -1
                    for i, entry in enumerate(metrics_list):
                        if not entry.get("_delta", False):
                            last_full_idx = i
                    if last_full_idx >= 0:
                        metrics_list = metrics_list[last_full_idx:]

                    # Rebuild from the window entries
                    replay_history = []
                    for entry in metrics_list:
                        entry_datetimes = self._replay_datetimes(entry)
                        metric_values = list(self._iter_replay_metric_values(dolphie.metric_manager, entry))
                        replay_history.append((entry_datetimes, metric_values))
                    dolphie.metric_manager.rebuild_replay_history(replay_history)
            else:
                # Full format: replace values entirely
                metric_values = list(
                    self._iter_replay_metric_values(
                        dolphie.metric_manager,
                        replay_event_data.metric_manager,
                    )
                )
                dolphie.metric_manager.replace_replay_history(new_datetimes, metric_values)

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

        # Bail out if the tab has been reconfigured into replay mode while a stale
        # worker_timer was still in flight. The replay path is driven by
        # run_worker_replay; letting run_worker_main proceed here races with it on
        # the same Dolphie state (host_version, global_variables, connection) and
        # has been observed to crash fetch_replication_data with host_version=None.
        if tab.dolphie.replay_file:
            return

        # Get our worker thread
        tab.worker = get_current_worker()
        tab.worker.name = tab_id

        dolphie = tab.dolphie
        try:
            if not dolphie.main_db_connection.is_connected():
                # Update connection status from worker thread
                self.app.call_from_thread(
                    self.app.tab_manager.update_connection_status,
                    tab=tab,
                    connection_status=ConnectionStatus.connecting,
                )

                tab.replay_manager = None
                if not dolphie.daemon_mode and tab == self.app.tab_manager.active_tab:
                    # Display property triggers UI updates, must be called from main thread
                    def show_loading():
                        tab.loading_indicator.display = True

                    self.app.call_from_thread(show_loading)

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

            # Disconnect from worker thread - call_from_thread handles async functions
            # wait_for_workers=False to avoid deadlock since we're calling from within the worker
            self.app.call_from_thread(self.app.tab_manager.disconnect_tab, tab, wait_for_workers=False)

    def run_worker_replicas(self, tab_id: str):
        tab = self.app.tab_manager.get_tab(tab_id)
        if not tab:
            return

        # Bail out if the tab has been reconfigured into replay mode while a stale
        # replicas_worker_timer was still in flight.
        if tab.dolphie.replay_file:
            return

        # Get our worker thread
        tab.replicas_worker = get_current_worker()
        tab.replicas_worker.name = tab_id

        dolphie = tab.dolphie

        if dolphie.panels.replication.visible:
            active_tab = self.app.tab_manager.active_tab
            if active_tab is None or tab.id != active_tab.id:
                return

            replica_count = dolphie.replica_manager.discovery_count
            if replica_count:
                if not dolphie.replica_manager.active_count:

                    def update_replicas_ui():
                        tab.replicas_container.display = True
                        tab.replicas_loading_indicator.display = True
                        tab.replicas_title.update(
                            f"[$white][b]Loading [$highlight]{replica_count}[/$highlight] replicas...\n"
                        )

                    self.app.call_from_thread(update_replicas_ui)

            # Reconcile even an empty discovery snapshot so stale connections and
            # widgets are removed in the same cycle.
            ReplicationPanel.fetch_replicas(tab)
        else:
            # If we're not displaying the replication panel, remove all replica connections
            dolphie.replica_manager.remove_all_replicas()

    def on_worker_state_changed(self, event: Worker.StateChanged):
        if event.state not in [WorkerState.SUCCESS, WorkerState.CANCELLED, WorkerState.ERROR]:
            return

        tab = self.app.tab_manager.get_tab(event.worker.name)
        if not tab:
            return

        dolphie = tab.dolphie
        active_tab = self.app.tab_manager.active_tab

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
                    or active_tab is None
                    or tab.id != active_tab.id
                ):
                    tab.worker_timer = self.app.set_timer(refresh_interval, partial(self.app.run_worker_main, tab.id))

                    return

                if not tab.main_container.display:
                    tab.sync_shared_ui()

                self.app.worker_data_processor.refresh_screen(tab)

                # Update the topbar with the latest replay file size
                if dolphie.record_for_replay:
                    self.app.tab_manager.update_topbar(tab=tab)

                tab.toggle_entities_displays()

                tab.worker_timer = self.app.set_timer(refresh_interval, partial(self.app.run_worker_main, tab.id))
            elif event.state == WorkerState.CANCELLED:
                # Only show the modal if there's a worker cancel error
                if tab.worker_cancel_error:
                    logger.critical(tab.worker_cancel_error)

                    if active_tab is None or active_tab.id != tab.id or self.app.tab_manager.loading_hostgroups:
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
            elif event.state == WorkerState.ERROR:
                # An unhandled exception (anything but ManualException, which
                # is handled above via CANCELLED) would otherwise leave this
                # tab's polling loop dead forever with no retry. Back off and
                # retry like the replicas worker does, instead of silently
                # stopping.
                refresh_interval = dolphie.refresh_interval
                if dolphie.connection_source == ConnectionSource.proxysql:
                    refresh_interval = dolphie.determine_proxysql_refresh_interval()
                retry_interval = min(max(refresh_interval * 2, 5), 30)
                logger.error(f"Main worker failed for {dolphie.host_with_port}: {event.worker.error}")
                tab.worker_timer = self.app.set_timer(retry_interval, partial(self.app.run_worker_main, tab.id))
        elif event.worker.group == "replicas":
            if event.state == WorkerState.SUCCESS:
                # Skip this if the conditions are right
                if (
                    len(self.app.screen_stack) > 1
                    or dolphie.pause_refresh
                    or active_tab is None
                    or tab.id != active_tab.id
                ):
                    tab.replicas_worker_timer = self.app.set_timer(
                        dolphie.refresh_interval,
                        partial(self.app.run_worker_replicas, tab.id),
                    )
                    return

                if dolphie.panels.replication.visible:
                    ReplicationPanel.create_replica_panel(tab)

                tab.replicas_worker_timer = self.app.set_timer(
                    dolphie.refresh_interval,
                    partial(self.app.run_worker_replicas, tab.id),
                )
            elif event.state == WorkerState.ERROR:
                retry_interval = min(max(dolphie.refresh_interval * 2, 5), 30)
                logger.error(f"Replica worker failed for {dolphie.host_with_port}: {event.worker.error}")
                tab.replicas_worker_timer = self.app.set_timer(
                    retry_interval,
                    partial(self.app.run_worker_replicas, tab.id),
                )
        elif event.worker.group == "replay" and event.state == WorkerState.SUCCESS:
            if active_tab is not None and tab.id == active_tab.id:
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
                tab.sync_shared_ui()

            self.app.worker_data_processor.refresh_screen(tab)
            if dolphie.connection_source == ConnectionSource.mysql:
                ReplicationPanel.create_replica_panel(tab)

            tab.toggle_entities_displays()

            tab.worker_timer = self.app.set_timer(
                dolphie.refresh_interval,
                partial(self.app.run_worker_replay, tab.id),
            )
