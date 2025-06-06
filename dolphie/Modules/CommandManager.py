from typing import Dict

from dolphie.DataTypes import ConnectionSource


class CommandManager:
    def __init__(self):
        self.command_keys = {
            ConnectionSource.mysql: {
                "Commands": {
                    "1": {"human_key": "1", "description": "Toggle panel Dashboard"},
                    "2": {"human_key": "2", "description": "Toggle panel Processlist"},
                    "3": {"human_key": "3", "description": "Toggle panel Metric Graphs"},
                    "4": {"human_key": "4", "description": "Toggle panel Replication/Replicas"},
                    "5": {"human_key": "5", "description": "Toggle panel Metadata Locks"},
                    "6": {"human_key": "6", "description": "Toggle panel DDLs"},
                    "7": {"human_key": "7", "description": "Toggle panel Performance Schema Metrics"},
                    "8": {"human_key": "8", "description": "Toggle panel Statements Summary Metrics"},
                    "placeholder_1": {"human_key": "", "description": ""},
                    "grave_accent": {"human_key": "`", "description": "Open tab setup"},
                    "plus": {"human_key": "+", "description": "Create a new tab"},
                    "minus": {"human_key": "-", "description": "Remove the current tab"},
                    "equals_sign": {"human_key": "=", "description": "Rename the current tab"},
                    "D": {"human_key": "D", "description": "Disconnect from the tab's host"},
                    "ctrl+a": {"human_key": "ctrl+a", "description": "Switch to the previous tab"},
                    "ctrl+d": {"human_key": "ctrl+d", "description": "Switch to the next tab"},
                    "placeholder_2": {"human_key": "", "description": ""},
                    "a": {"human_key": "a", "description": "Toggle additional processlist columns"},
                    "A": {
                        "human_key": "A",
                        "description": "Toggle query digest text/sample text in Statements Summary panel",
                    },
                    "C": {
                        "human_key": "C",
                        "description": "Toggle display of threads with concurrency tickets in Processlist panel",
                    },
                    "i": {"human_key": "i", "description": "Toggle display of idle threads"},
                    "T": {
                        "human_key": "T",
                        "description": "Toggle display of threads that only have an active transaction",
                    },
                    "p": {"human_key": "p", "description": "Toggle pause for refreshing of panels"},
                    "P": {
                        "human_key": "P",
                        "description": (
                            "Toggle between Information Schema and Performance Schema for the Processlist panel"
                        ),
                    },
                    "s": {"human_key": "s", "description": "Toggle sorting for Age in Processlist panel"},
                    "placeholder_3": {"human_key": "", "description": ""},
                    "l": {"human_key": "l", "description": "Display the most recent deadlock"},
                    "o": {"human_key": "o", "description": "Display output from SHOW ENGINE INNODB STATUS"},
                    "m": {"human_key": "m", "description": "Display memory usage"},
                    "d": {"human_key": "d", "description": "Display all databases"},
                    "e": {"human_key": "e", "description": "Display error log from Performance Schema"},
                    "t": {
                        "human_key": "t",
                        "description": "Display details of a thread along with an EXPLAIN of its query",
                    },
                    "u": {"human_key": "u", "description": "Display active connected users and their statistics"},
                    "v": {"human_key": "v", "description": "Display variables from SHOW GLOBAL VARIABLES"},
                    "z": {"human_key": "z", "description": "Display all entries in the host cache"},
                    "Z": {
                        "human_key": "Z",
                        "description": (
                            "Display table sizes and fragmentation for all databases - "
                            "[$yellow]Heed caution if you have a lot of tables![/$yellow]"
                        ),
                    },
                    "placeholder_4": {"human_key": "", "description": ""},
                    "c": {"human_key": "c", "description": "Clear all filters set"},
                    "f": {"human_key": "f", "description": "Filter threads by field(s)"},
                    "E": {"human_key": "E", "description": "Export the processlist to a CSV file"},
                    "k": {"human_key": "k", "description": "Kill thread(s)"},
                    "M": {"human_key": "M", "description": "Maximize a panel"},
                    "q": {"human_key": "q", "description": "Quit"},
                    "r": {"human_key": "r", "description": "Set the refresh interval"},
                    "R": {"human_key": "R", "description": "Reset all metrics"},
                    "space": {
                        "human_key": "space",
                        "description": "Force a manual refresh of all panels except replicas",
                    },
                }
            },
            ConnectionSource.proxysql: {
                "Commands": {
                    "1": {"human_key": "1", "description": "Toggle panel Dashboard"},
                    "2": {"human_key": "2", "description": "Toggle panel Processlist"},
                    "3": {"human_key": "3", "description": "Toggle panel Metric Graphs"},
                    "4": {"human_key": "4", "description": "Toggle panel Hostgroup Summary"},
                    "5": {"human_key": "5", "description": "Toggle panel Query Rules"},
                    "6": {"human_key": "6", "description": "Toggle panel Command Statistics"},
                    "placeholder_1": {"human_key": "", "description": ""},
                    "grave_accent": {"human_key": "`", "description": "Open tab setup"},
                    "plus": {"human_key": "+", "description": "Create a new tab"},
                    "minus": {"human_key": "-", "description": "Remove the current tab"},
                    "equals_sign": {"human_key": "=", "description": "Rename the current tab"},
                    "D": {"human_key": "D", "description": "Disconnect from the tab's host"},
                    "ctrl+a": {"human_key": "ctrl+a", "description": "Switch to the previous tab"},
                    "ctrl+d": {"human_key": "ctrl+d", "description": "Switch to the next tab"},
                    "placeholder_2": {"human_key": "", "description": ""},
                    "a": {"human_key": "a", "description": "Toggle additional processlist columns"},
                    "i": {"human_key": "i", "description": "Toggle display of idle threads"},
                    "p": {"human_key": "p", "description": "Toggle pause for refreshing of panels"},
                    "s": {"human_key": "s", "description": "Toggle sorting for Age in Processlist panel"},
                    "placeholder_3": {"human_key": "", "description": ""},
                    "e": {
                        "human_key": "e",
                        "description": "Display errors reported by backend servers during query execution",
                    },
                    "m": {"human_key": "m", "description": "Display memory usage"},
                    "t": {"human_key": "t", "description": "Display details of a thread"},
                    "u": {"human_key": "u", "description": "Display frontend users connected"},
                    "v": {"human_key": "v", "description": "Display variables from SHOW GLOBAL VARIABLES"},
                    "z": {"human_key": "z", "description": "Display all entries in the host cache"},
                    "placeholder_4": {"human_key": "", "description": ""},
                    "c": {"human_key": "c", "description": "Clear all filters set"},
                    "f": {"human_key": "f", "description": "Filter threads by field(s)"},
                    "E": {"human_key": "E", "description": "Export the processlist to a CSV file"},
                    "k": {"human_key": "k", "description": "Kill thread(s)"},
                    "M": {"human_key": "M", "description": "Maximize a panel"},
                    "q": {"human_key": "q", "description": "Quit"},
                    "r": {"human_key": "r", "description": "Set the refresh interval"},
                    "R": {"human_key": "R", "description": "Reset all metrics"},
                    "space": {
                        "human_key": "space",
                        "description": "Force a manual refresh of all panels except replicas",
                    },
                },
                "Terminology": {
                    "FE": {"description": "Frontend"},
                    "BE": {"description": "Backend"},
                    "Conn": {"description": "Connection"},
                    "CP": {"description": "Connection Pool"},
                    "MP": {"description": "Multiplex"},
                },
            },
            "mysql_replay": {
                "Commands": {
                    "1": {"human_key": "1", "description": "Toggle panel Dashboard"},
                    "2": {"human_key": "2", "description": "Toggle panel Processlist"},
                    "3": {"human_key": "3", "description": "Toggle panel Metric Graphs"},
                    "4": {"human_key": "4", "description": "Toggle panel Replication/Replicas"},
                    "5": {"human_key": "5", "description": "Toggle panel Metadata Locks"},
                    "7": {"human_key": "7", "description": "Toggle panel Performance Schema Metrics"},
                    "8": {"human_key": "8", "description": "Toggle panel Statements Summary Metrics"},
                    "placeholder_1": {"human_key": "", "description": ""},
                    "grave_accent": {"human_key": "`", "description": "Open tab setup"},
                    "plus": {"human_key": "+", "description": "Create a new tab"},
                    "minus": {"human_key": "-", "description": "Remove the current tab"},
                    "equals_sign": {"human_key": "=", "description": "Rename the current tab"},
                    "ctrl+a": {"human_key": "ctrl+a", "description": "Switch to the previous tab"},
                    "ctrl+d": {"human_key": "ctrl+d", "description": "Switch to the next tab"},
                    "placeholder_2": {"human_key": "", "description": ""},
                    "a": {"human_key": "a", "description": "Toggle additional processlist columns"},
                    "A": {
                        "human_key": "A",
                        "description": "Toggle query digest text/sample text in Statements Summary panel",
                    },
                    "C": {
                        "human_key": "C",
                        "description": "Toggle display of concurrency threads with tickets in Processlist panel",
                    },
                    "T": {
                        "human_key": "T",
                        "description": "Toggle display of threads that only have an active transaction",
                    },
                    "s": {"human_key": "s", "description": "Toggle sorting for Age in Processlist panel"},
                    "placeholder_3": {"human_key": "", "description": ""},
                    "t": {"human_key": "t", "description": "Display details of a thread"},
                    "v": {"human_key": "v", "description": "Display global variables from SHOW GLOBAL VARIABLES"},
                    "V": {"human_key": "V", "description": "Display global variables that changed during recording"},
                    "placeholder_4": {"human_key": "", "description": ""},
                    "p": {"human_key": "p", "description": "Toggle pause of replay"},
                    "S": {"human_key": "S", "description": "Seek to a specific time in the replay"},
                    "left_square_bracket": {
                        "human_key": "\\[",
                        "description": " Seek to previous refresh interval in the replay",
                    },
                    "right_square_bracket": {
                        "human_key": "]",
                        "description": "Seek to next refresh interval in the replay",
                    },
                    "placeholder_5": {"human_key": "", "description": ""},
                    "c": {"human_key": "c", "description": "Clear all filters set"},
                    "f": {"human_key": "f", "description": "Filter threads by field(s)"},
                    "E": {"human_key": "E", "description": "Export the processlist to a CSV file"},
                    "M": {"human_key": "M", "description": "Maximize a panel"},
                    "q": {"human_key": "q", "description": "Quit"},
                    "r": {"human_key": "r", "description": "Set the refresh interval"},
                }
            },
            "proxysql_replay": {
                "Commands": {
                    "1": {"human_key": "1", "description": "Toggle panel Dashboard"},
                    "2": {"human_key": "2", "description": "Toggle panel Processlist"},
                    "3": {"human_key": "3", "description": "Toggle panel Metric Graphs"},
                    "4": {"human_key": "4", "description": "Toggle panel Hostgroup Summary"},
                    "placeholder_1": {"human_key": "", "description": ""},
                    "grave_accent": {"human_key": "`", "description": "Open tab setup"},
                    "plus": {"human_key": "+", "description": "Create a new tab"},
                    "minus": {"human_key": "-", "description": "Remove the current tab"},
                    "equals_sign": {"human_key": "=", "description": "Rename the current tab"},
                    "ctrl+a": {"human_key": "ctrl+a", "description": "Switch to the previous tab"},
                    "ctrl+d": {"human_key": "ctrl+d", "description": "Switch to the next tab"},
                    "placeholder_2": {"human_key": "", "description": ""},
                    "a": {"human_key": "a", "description": "Toggle additional processlist columns"},
                    "s": {"human_key": "s", "description": "Toggle sorting for Age in Processlist panel"},
                    "placeholder_3": {"human_key": "", "description": ""},
                    "t": {"human_key": "t", "description": "Display details of a thread"},
                    "v": {"human_key": "v", "description": "Display global variables from SHOW GLOBAL VARIABLES"},
                    "V": {"human_key": "V", "description": "Display global variables that changed during recording"},
                    "placeholder_4": {"human_key": "", "description": ""},
                    "p": {"human_key": "p", "description": "Toggle pause of replay"},
                    "S": {"human_key": "S", "description": "Seek to a specific time in the replay"},
                    "left_square_bracket": {
                        "human_key": "[",
                        "description": "Seek to previous refresh interval in the replay",
                    },
                    "right_square_bracket": {
                        "human_key": "]",
                        "description": "Seek to next refresh interval in the replay",
                    },
                    "placeholder_5": {"human_key": "", "description": ""},
                    "c": {"human_key": "c", "description": "Clear all filters set"},
                    "f": {"human_key": "f", "description": "Filter threads by field(s)"},
                    "E": {"human_key": "E", "description": "Export the processlist to a CSV file"},
                    "M": {"human_key": "M", "description": "Maximize a panel"},
                    "q": {"human_key": "q", "description": "Quit"},
                    "r": {"human_key": "r", "description": "Set the refresh interval"},
                }
            },
        }

        # These are keys that we let go through no matter what
        self.exclude_keys = [
            "up",
            "down",
            "left",
            "right",
            "pageup",
            "pagedown",
            "home",
            "end",
            "tab",
            "enter",
            "grave_accent",
            "q",
            "question_mark",
            "plus",
            "minus",
            "equals_sign",
            "ctrl+a",
            "ctrl+d",
        ]

    def get_commands(self, replay_file: str, connection_source: ConnectionSource) -> Dict[str, Dict[str, str]]:
        if replay_file:
            key = {ConnectionSource.mysql: "mysql_replay", ConnectionSource.proxysql: "proxysql_replay"}.get(
                connection_source, connection_source
            )
        else:
            key = connection_source

        return self.command_keys.get(key, {}).get("Commands")
