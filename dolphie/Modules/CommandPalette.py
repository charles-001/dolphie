from functools import partial
from typing import TYPE_CHECKING

from textual.command import DiscoveryHit, Hit, Provider

if TYPE_CHECKING:
    from dolphie.App import DolphieApp


class CommandPaletteCommands(Provider):
    """Command palette commands based on connection source."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dolphie_app: DolphieApp = self.app

    def async_command(self, key: str):
        """Helper function to call the process_key_event command asynchronously."""
        self.app.call_later(self.dolphie_app.key_event_manager.process_key_event, key)

    def get_command_hits(self):
        """Helper function to get all commands and format them for discovery or search."""
        commands = self.dolphie_app.command_manager.get_commands(
            self.dolphie_app.tab_manager.active_tab.dolphie.replay_file,
            self.dolphie_app.tab_manager.active_tab.dolphie.connection_source,
        )

        # Find the longest human_key length
        max_key_length = max(len(data["human_key"]) for data in commands.values())

        # Format the commands with dynamic spacing
        return {
            key: {
                # Center the human_key based on the max length and pad spaces after it
                "display": (
                    f"[$b_highlight]{data['human_key'].center(max_key_length)}[/$b_highlight]  {data['description']}"
                ),
                "text": f"{data['human_key']} {data['description']}",
                "command": partial(self.async_command, key),
                "human_key": data["human_key"],
            }
            for key, data in commands.items()
        }

    async def discover(self):
        for data in self.get_command_hits().values():
            yield DiscoveryHit(
                display=data["display"],
                text=data["text"],
                command=data["command"],
            )

    async def search(self, query: str):
        hits = []

        # Gather all hits and calculate their scores
        for data in self.get_command_hits().values():
            score = self.matcher(query).match(data["text"])
            if score > 0:
                hits.append(
                    Hit(
                        score=score,
                        match_display=data["display"],
                        text=data["text"],
                        command=data["command"],
                    )
                )

        # Sort the hits by score, descending order
        hits.sort(key=lambda hit: hit.score, reverse=True)

        for hit in hits:
            yield hit
