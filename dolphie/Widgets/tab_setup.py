from typing import Dict

from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    Checkbox,
    Input,
    Label,
    RadioButton,
    RadioSet,
    Rule,
    Select,
    Static,
)

from dolphie.Modules.ArgumentParser import CredentialProfile
from dolphie.Modules.ManualException import ManualException
from dolphie.Widgets.autocomplete import AutoComplete, Dropdown, DropdownItem


class TabSetupModal(ModalScreen):
    CSS = """
        TabSetupModal > Vertical {
            background: #131626;
            border: tall #384673;
            height: auto;
            width: 70;
        }
        TabSetupModal > Vertical > * {
            width: auto;
            height: auto;
            align: center middle;
        }
        TabSetupModal > Vertical > Vertical  > Container {
            width: 100%;
            height: auto;
        }
        TabSetupModal Label {
            text-style: bold;
            width: 100%;
            content-align: center middle;
            padding-bottom: 1;
        }
        TabSetupModal Rule {
            width: 100%;
            margin-bottom: 1;
        }
        TabSetupModal Input {
            width: 100%;
            border-title-color: #d2d2d2;
        }
        TabSetupModal .main_container {
            width: 100%;
            content-align: center middle;
        }
        TabSetupModal RadioSet {
            background: #131626;
            border: none;
            padding-bottom: 1;
            align: center middle;
            width: 100%;
            layout: horizontal;
        }
        TabSetupModal AutoComplete {
            width: 100%;
            height: auto;
        }
        TabSetupModal #password {
            width: 53;
        }
        TabSetupModal #show_password {
            max-width: 8;
            height: 3;
            background: #262c4b;
            border: blank #344063;
        }
        TabSetupModal #show_password:hover {
            background:  #313960;
            border: blank #344063;
        }
        TabSetupModal #show_password:focus {
            background:  #313960;
            border: blank #344063;
        }
        TabSetupModal #modal_footer {
            color: #d3565c;
            width: 100%;
            padding-bottom: 0;
            padding-top: 1;
            margin: 0 2;
        }
        TabSetupModal #replay_directory {
            padding-left: 3;
            padding-bottom: 1;
        }
        TabSetupModal Checkbox {
            background: #131626;
            border: none;
            padding-left: 2;
            padding-bottom: 1;
            content-align: left middle;
        }
        TabSetupModal Select {
            margin: 0 2;
            margin-bottom: 1;
            width: 100%;
        }
        TabSetupModal SelectCurrent Static#label {
            color: #606e88;
        }
        TabSetupModal SelectCurrent.-has-value Static#label {
            color: #e9e9e9;
        }
        TabSetupModal Select:focus > SelectCurrent {
            border: tall #384673;
        }
        TabSetupModal SelectCurrent {
            background: #111322;
            border: tall #252e49;
        }
        TabSetupModal Select > OptionList:focus {
            margin: 0 0 0 0;
            height: auto;
            max-height: 15;
            border: tall #3c476b;
        }
        TabSetupModal OptionList {
            background: #111322;
            border: tall #252e49;
            width: 100%;
            height: 15;
            margin: 0 1 0 1;
        }
        TabSetupModal #replay_file > SelectOverlay > .option-list--option {
            padding: 0;
        }
        TabSetupModal #replay_file > OptionList {
            width: auto;
            min-width: 100%;
        }
        TabSetupModal OptionList:focus {
            border: tall #475484;
        }
        TabSetupModal OptionList > .option-list--option-highlighted {
            text-style: none;
            background: #131626;
        }
        TabSetupModal OptionList:focus > .option-list--option-highlighted {
            background: #283048;
        }
        TabSetupModal OptionList > .option-list--option-hover {
            background: #283048;
        }
        TabSetupModal OptionList > .option-list--option-hover-highlighted {
            background: #283048;
            text-style: none;
        }
        TabSetupModal OptionList:focus > .option-list--option-hover-highlighted {
            background: #283048;
            text-style: none;
        }
    """
    BINDINGS = [
        Binding("escape", "app.pop_screen", "", show=False),
    ]

    def __init__(
        self,
        credential_profile: str,
        credential_profiles: Dict[str, CredentialProfile],
        host: str,
        port: int,
        username: str,
        password: str,
        ssl: dict,
        socket_file: str,
        available_hosts: list,
        hostgroups: dict,
        replay_files: list,
        replay_directory: str,
        error_message: ManualException = None,
    ):
        super().__init__()

        self.credential_profile = credential_profile
        if not self.credential_profile:
            self.credential_profile = Select.BLANK
        self.credential_profiles = credential_profiles

        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssl = ssl
        self.socket_file = socket_file
        self.replay_directory = replay_directory

        if self.host and self.port:
            self.host = f"{self.host}:{self.port}"

        self.options_available_hosts = []
        if available_hosts:
            self.options_available_hosts = [DropdownItem(id) for id in sorted(available_hosts)]

        self.options_hostgroups = []
        if hostgroups:
            self.options_hostgroups = [(host, host) for host in hostgroups]

        self.options_credential_profiles = []
        if credential_profiles:
            self.options_credential_profiles = [(profile, profile) for profile in credential_profiles.keys()]

        self.options_replay_files = []
        if replay_files:
            self.options_replay_files = [(replay_file, replay_path) for replay_path, replay_file in replay_files]

        self.error_message = error_message

    def on_mount(self) -> None:
        footer = self.query_one("#modal_footer", Static)

        footer.display = False
        if self.error_message:
            footer.update(self.error_message.output())
            footer.display = True

        self.query_one("#host", Input).border_title = "Host"
        self.query_one("#username", Input).border_title = "Username"
        self.query_one("#password", Input).border_title = "Password"
        self.query_one("#socket_file", Input).border_title = "Socket File [dark_gray](optional)[/dark_gray]"
        self.query_one("#ssl_ca", Input).border_title = "CA File [dark_gray](optional)[/dark_gray]"
        self.query_one("#ssl_cert", Input).border_title = "Client Certificate File [dark_gray](optional)[/dark_gray]"
        self.query_one("#ssl_key", Input).border_title = "Client Key File [dark_gray](optional)[/dark_gray]"

        if self.ssl:
            self.query_one("#ssl", Checkbox).value = True

            if self.ssl.get("required"):
                ssl_mode = "REQUIRED"
            elif self.ssl.get("check_hostname") is False:
                ssl_mode = "VERIFY_CA"
            elif self.ssl.get("check_hostname") is True:
                ssl_mode = "VERIFY_IDENTITY"

            self.query_one(f"#{ssl_mode}", RadioButton).value = True
            self.query_one("#ssl_ca", Input).value = self.ssl.get("ca", "")
            self.query_one("#ssl_cert", Input).value = self.ssl.get("cert", "")
            self.query_one("#ssl_key", Input).value = self.ssl.get("key", "")
        else:
            self.query_one("#container_ssl", Container).display = False

        if self.socket_file:
            self.query_one("#socket_file", Input).value = self.socket_file

    def compose(self) -> ComposeResult:
        with Vertical():
            with Vertical(classes="main_container"):
                yield Label("Connect to a Host")

                yield Select(
                    options=self.options_credential_profiles,
                    id="credential_profile",
                    value=self.credential_profile,
                    prompt="Select a credential profile (optional)",
                )

                yield AutoComplete(
                    Input(value=self.host, id="host", placeholder="Host:Port"),
                    Dropdown(id="dropdown_items", items=self.options_available_hosts),
                )

                yield Input(id="username", value=self.username)

                with Horizontal():
                    yield Input(id="password", value=self.password, password=True)
                    yield Button("Show", id="show_password")
                yield Input(id="socket_file")
                yield Checkbox("Enable SSL", id="ssl")
                with Container(id="container_ssl"):
                    yield RadioSet(
                        *(
                            [
                                RadioButton(ssl_mode, id=ssl_mode)
                                for ssl_mode in ["REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"]
                            ]
                        ),
                        id="ssl_mode",
                    )
                    yield Input(id="ssl_ca")
                    yield Input(id="ssl_cert")
                    yield Input(id="ssl_key")
                yield Rule(line_style="heavy")
                yield Label("Connect with a Hostgroup")
                yield Select(
                    options=self.options_hostgroups,
                    id="hostgroup",
                    prompt="Select a hostgroup",
                )
                yield Rule(line_style="heavy")
                yield Label("Load a Replay File")
                yield Static(f"[dark_gray][b]Directory[/b]: {self.replay_directory}", id="replay_directory")
                yield Select(
                    options=self.options_replay_files,
                    id="replay_file",
                    prompt="Select a replay file",
                )
            with Horizontal(classes="button_container"):
                yield Button("Submit", id="submit", variant="primary")
                yield Button("Cancel", id="cancel")
            yield Label(id="modal_footer")

    @on(Select.Changed, "#credential_profile")
    def credential_profile_changed(self, event: Select.Changed):
        def set_field(selector, value=None, default=""):
            self.query_one(selector, Input).value = value or default

        # Reset fields if no profile selected
        if event.value == Select.BLANK:
            self.query_one("#hostgroup", Select).disabled = False
            self.query_one("#ssl", Checkbox).value = False
            for selector in ["#username", "#password", "#socket_file", "#ssl_ca", "#ssl_cert", "#ssl_key"]:
                set_field(selector)
            return

        # Load selected credential profile
        credential_profile = self.credential_profiles.get(event.value)
        if not credential_profile:
            return

        set_field("#username", credential_profile.user, default=self.username or "")
        set_field("#password", credential_profile.password, default=self.password or "")
        set_field("#socket_file", credential_profile.socket, default=self.socket_file or "")

        if credential_profile.ssl_mode:
            self.query_one("#ssl", Checkbox).value = True
            self.query_one(f"#{credential_profile.ssl_mode}", RadioButton).value = True

            for field, ssl_key in [("#ssl_ca", "ca"), ("#ssl_cert", "cert"), ("#ssl_key", "key")]:
                set_field(field, getattr(credential_profile, f"ssl_{ssl_key}"), default=self.ssl.get(ssl_key, ""))
        else:
            self.query_one("#ssl", Checkbox).value = False

    def update_inputs(self, disable: bool = None, exclude: list = []):
        inputs = {
            "#host": Input,
            "#username": Input,
            "#password": Input,
            "#socket_file": Input,
            "#ssl": Checkbox,
            "#show_password": Button,
            "#credential_profile": Select,
            "#hostgroup": Select,
        }
        for key, widget in inputs.items():
            if key not in exclude:
                input_element = self.query_one(key, widget)
                if disable is not None:
                    input_element.disabled = disable
                    if key == "#ssl" and disable:  # Reset SSL checkbox when disabled
                        input_element.value = False

    @on(Select.Changed, "#hostgroup")
    def hostgroup_changed(self, event: Select.Changed):
        self.update_inputs(disable=event.value != Select.BLANK, exclude=["#hostgroup"])

    @on(Select.Changed, "#replay_file")
    def replay_file_changed(self, event: Select.Changed):
        self.update_inputs(disable=event.value != Select.BLANK, exclude=["#replay_file"])

    @on(RadioSet.Changed, "#ssl_mode")
    def ssl_mode_changed(self, event: RadioSet.Changed):
        if event.pressed.id in ["VERIFY_CA", "VERIFY_IDENTITY"]:
            self.query_one("#ssl_ca", Input).border_title = "CA File"
            self.query_one("#ssl_cert", Input).border_title = (
                "Client Certificate File [dark_gray](optional)[/dark_gray]"
            )
            self.query_one("#ssl_key", Input).border_title = "Client Key File [dark_gray](optional)[/dark_gray]"
        else:
            self.query_one("#ssl_ca", Input).border_title = "CA File [dark_gray](optional)[/dark_gray]"
            self.query_one("#ssl_cert", Input).border_title = (
                "Client Certificate File [dark_gray](optional)[/dark_gray]"
            )
            self.query_one("#ssl_key", Input).border_title = "Client Key File [dark_gray](optional)[/dark_gray]"

    @on(Checkbox.Changed, "#ssl")
    def ssl_changed(self, event: Checkbox.Changed):
        if event.value:
            self.query_one("#container_ssl", Container).display = True
        else:
            self.query_one("#container_ssl", Container).display = False

    @on(Button.Pressed, "#show_password")
    def on_show_password_pressed(self, event: Button.Pressed) -> None:
        password = self.query_one("#password", Input)
        show_password_button = self.query_one("#show_password", Button)

        if password.password:
            show_password_button.label = "Hide"
        else:
            show_password_button.label = "Show"

        password.password = not password.password

    @on(Button.Pressed, "#submit")
    def on_submit_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "submit":
            error_message = None

            credential_profile = self.query_one("#credential_profile", Select)
            replay_file = self.query_one("#replay_file", Select)
            host = self.query_one("#host", Input)
            hostgroup = self.query_one("#hostgroup", Select)
            username = self.query_one("#username", Input)
            password = self.query_one("#password", Input)

            ssl_mode = (
                self.query_one("#ssl_mode", RadioSet).pressed_button.id
                if self.query_one("#ssl_mode", RadioSet).pressed_button
                else None
            )
            ssl_ca = self.query_one("#ssl_ca", Input).value
            ssl_cert = self.query_one("#ssl_cert", Input).value
            ssl_key = self.query_one("#ssl_key", Input).value

            ssl = {}
            if self.query_one("#ssl", Checkbox).value:
                if ssl_mode == "REQUIRED":
                    ssl["required"] = True
                elif ssl_mode == "VERIFY_CA":
                    if not ssl_ca:
                        error_message = "SSL mode VERIFY_CA requires CA File to be specified"

                    ssl["check_hostname"] = False
                    ssl["verify_mode"] = True
                elif ssl_mode == "VERIFY_IDENTITY":
                    if not ssl_ca:
                        error_message = "SSL mode VERIFY_IDENTITY requires CA File to be specified"

                    ssl["check_hostname"] = True
                    ssl["verify_mode"] = True
                else:
                    error_message = "SSL mode must be specified"

                if ssl_ca:
                    ssl["ca"] = ssl_ca
                if ssl_cert:
                    ssl["cert"] = ssl_cert
                if ssl_key:
                    ssl["key"] = ssl_key

            socket_file = self.query_one("#socket_file", Input)
            hostgroup_value = None if hostgroup.value == Select.BLANK else hostgroup.value
            replay_file_value = None if replay_file.value == Select.BLANK else replay_file.value

            modal_footer = self.query_one("#modal_footer", Label)
            host_split = host.value.split(":")
            if len(host_split) == 2:
                try:
                    int(host_split[1])
                except ValueError:
                    error_message = "Port must be a valid integer"
            elif len(host_split) > 2:
                error_message = "Host cannot contain more than one colon"

            if not host.value and not hostgroup_value and not replay_file_value:
                error_message = "You must specify either a host, hostgroup, or replay file"

            if error_message:
                modal_footer.update(error_message)
                modal_footer.display = True
                return

            self.dismiss(
                {
                    "credential_profile": credential_profile.value,
                    "replay_file": replay_file_value,
                    "host": host.value,
                    "hostgroup": hostgroup_value,
                    "username": username.value,
                    "password": password.value,
                    "ssl": ssl,
                    "socket_file": socket_file.value,
                }
            )
        else:
            self.app.pop_screen()

    @on(Input.Submitted, "#password")
    def on_input_submitted(self):
        self.query_one("#submit", Button).press()

    @on(Button.Pressed, "#cancel")
    def on_cancel_pressed(self, event: Button.Pressed) -> None:
        self.app.pop_screen()
