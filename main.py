#!/usr/bin/env python3
import argparse
import csv
import datetime
import inspect
import json
import math
import statistics
import sys
import time
from collections.abc import Iterable
from itertools import pairwise
from numbers import Number
from os.path import expanduser
from typing import Any, SupportsComplex, TypedDict, TypeVar, cast

import requests
import tqdm
from lxml import html

Numeric = SupportsComplex | Number | int | float


def eprint(*args, **kwargs) -> None:  # type: ignore  # pyright: ignore[reportMissingParameterType, reportUnknownParameterType]
    print(f"{inspect.stack()[1][1]}:{inspect.stack()[1][2]};{inspect.stack()[1][3]}", *args, file=sys.stderr, **kwargs)  # pyright: ignore[reportUnknownArgumentType]


def fib(n: int) -> int:
    p = (1 + math.sqrt(5)) / 2
    q = (1 - math.sqrt(5)) / 2
    return int((p**n - q**n) / math.sqrt(5))


T = TypeVar("T")


def triplewise(iterable: Iterable[T]) -> Iterable[tuple[T, T, T]]:
    "Return overlapping triplets from an iterable"
    # triplewise('ABCDEFG') --> ABC BCD CDE DEF EFG
    for (a, _), (b, c) in pairwise(pairwise(iterable)):
        yield a, b, c


class StatsDict(TypedDict):
    time: str  # datetime.datetime as iso formatted str
    last_sent: str  # datetime.datetime as iso formatted str
    power_total: float
    notification: dict[str, int]


class ConfigDict(TypedDict):
    off_power: float
    max_idle_power: float
    re_remind: bool
    re_remind_counter: int
    stats: dict[str, StatsDict]

    min_data_window_minutes: float
    min_runtime_minutes: float


T_val = TypeVar("T_val")


def update_dict_recursive(config: dict[str, T_val], default: dict[str, T_val], reset: bool = False) -> dict[str, T_val]:
    for default_key, default_value in default.items():
        if isinstance(default_value, dict):
            if default_key not in config:
                config[default_key] = cast(T_val, {})
            config[default_key] = cast(
                T_val,
                update_dict_recursive(
                    cast(
                        dict[str, T_val],
                        config[default_key]
                    ),
                    cast(
                        dict[str, T_val],
                        default_value
                    )
                )
            )
        else:
            if reset:
                config[default_key] = default_value
            else:
                config[default_key] = config.get(default_key, default_value)
    return config


class Config():
    json_name: str

    config: ConfigDict
    # config: dict[str, float | str | bool | int | datetime.datetime | datetime.timedelta]

    @property
    def min_off_power(self: 'Config') -> float:
        return float(self.config["off_power"])

    @min_off_power.setter
    def min_off_power(self: 'Config', value: float) -> None:
        self.config["off_power"] = value

    @property
    def max_idle_power(self: 'Config') -> float:
        return float(self.config["max_idle_power"])

    @max_idle_power.setter
    def max_idle_power(self: 'Config', value: float) -> None:
        self.config["max_idle_power"] = value

    @property
    def re_remind(self: 'Config') -> bool:
        return bool(self.config["re_remind"])

    @re_remind.setter
    def re_remind(self: 'Config', value: bool) -> None:
        self.config["re_remind"] = value

    @property
    def re_remind_counter(self: 'Config') -> int:
        return int(self.config["re_remind_counter"])

    @re_remind_counter.setter
    def re_remind_counter(self: 'Config', value: int) -> None:
        self.config["re_remind_counter"] = value

    # region on
    @property
    def stats_power_on_time(self: 'Config') -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.config["stats"]["on"].get("time", datetime.datetime.min.isoformat()))

    @stats_power_on_time.setter
    def stats_power_on_time(self: 'Config', value: datetime.datetime) -> None:
        self.config["stats"]["on"]["time"] = value.isoformat()

    @property
    def stats_power_on_last_sent(self: 'Config') -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.config["stats"]["on"].get("last_sent", datetime.datetime.min.isoformat()))

    @stats_power_on_last_sent.setter
    def stats_power_on_last_sent(self: 'Config', value: datetime.datetime) -> None:
        self.config["stats"]["on"]["last_sent"] = value.isoformat()

    @property
    def stats_on_power_total(self: 'Config') -> float:
        return float(self.config["stats"]["on"].get("power_total", 0))

    @stats_on_power_total.setter
    def stats_on_power_total(self: 'Config', value: float) -> None:
        self.config["stats"]["on"]["power_total"] = value

    @property
    def stats_on_notification_server_mail(self: 'Config') -> int:
        return int(self.config["stats"]["on"]["notification"]["server-mail"])

    @stats_on_notification_server_mail.setter
    def stats_on_notification_server_mail(self: 'Config', value: int) -> None:
        self.config["stats"]["on"]["notification"]["server-mail"] = value

    @property
    def stats_on_notification_todo(self: 'Config') -> int:
        return int(self.config["stats"]["on"]["notification"]["todo"])

    @stats_on_notification_todo.setter
    def stats_on_notification_todo(self: 'Config', value: int) -> None:
        self.config["stats"]["on"]["notification"]["todo"] = value

    @property
    def stats_on_notification_jo_private(self: 'Config') -> int:
        return int(self.config["stats"]["on"]["notification"]["jo_private"])

    @stats_on_notification_jo_private.setter
    def stats_on_notification_jo_private(self: 'Config', value: int) -> None:
        self.config["stats"]["on"]["notification"]["jo_private"] = value
    # endregion on

    # region off
    @property
    def stats_power_off_time(self: 'Config') -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.config["stats"]["off"].get("time", datetime.datetime.min.isoformat()))

    @stats_power_off_time.setter
    def stats_power_off_time(self: 'Config', value: datetime.datetime) -> None:
        self.config["stats"]["off"]["time"] = value.isoformat()

    @property
    def stats_power_off_last_sent(self: 'Config') -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.config["stats"]["off"].get("last_sent", datetime.datetime.min.isoformat()))

    @stats_power_off_last_sent.setter
    def stats_power_off_last_sent(self: 'Config', value: datetime.datetime) -> None:
        self.config["stats"]["off"]["last_sent"] = value.isoformat()

    @property
    def stats_off_power_total(self: 'Config') -> float:
        return float(self.config["stats"]["off"].get("power_total", 0))

    @stats_off_power_total.setter
    def stats_off_power_total(self: 'Config', value: float) -> None:
        self.config["stats"]["off"]["power_total"] = value

    @property
    def stats_off_notification_server_mail(self: 'Config') -> int:
        return int(self.config["stats"]["off"]["notification"]["server-mail"])

    @stats_off_notification_server_mail.setter
    def stats_off_notification_server_mail(self: 'Config', value: int) -> None:
        self.config["stats"]["off"]["notification"]["server-mail"] = value

    @property
    def stats_off_notification_todo(self: 'Config') -> int:
        return int(self.config["stats"]["off"]["notification"]["todo"])

    @stats_off_notification_todo.setter
    def stats_off_notification_todo(self: 'Config', value: int) -> None:
        self.config["stats"]["off"]["notification"]["todo"] = value

    @property
    def stats_off_notification_jo_private(self: 'Config') -> int:
        return int(self.config["stats"]["off"]["notification"]["jo_private"])

    @stats_off_notification_jo_private.setter
    def stats_off_notification_jo_private(self: 'Config', value: int) -> None:
        self.config["stats"]["off"]["notification"]["jo_private"] = value
    # endregion off

    # region done
    @property
    def stats_done_time(self: 'Config') -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.config["stats"]["done"].get("time", datetime.datetime.min.isoformat()))

    @stats_done_time.setter
    def stats_done_time(self: 'Config', value: datetime.datetime) -> None:
        self.config["stats"]["done"]["time"] = value.isoformat()

    @property
    def stats_done_last_sent(self: 'Config') -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.config["stats"]["done"].get("last_sent", datetime.datetime.min.isoformat()))

    @stats_done_last_sent.setter
    def stats_done_last_sent(self: 'Config', value: datetime.datetime) -> None:
        self.config["stats"]["done"]["last_sent"] = value.isoformat()

    @property
    def stats_done_power_total(self: 'Config') -> float:
        return float(self.config["stats"]["done"].get("power_total", 0))

    @stats_done_power_total.setter
    def stats_done_power_total(self: 'Config', value: float) -> None:
        self.config["stats"]["done"]["power_total"] = value

    @property
    def stats_done_notification_server_mail(self: 'Config') -> int:
        return int(self.config["stats"]["done"]["notification"]["server-mail"])

    @stats_done_notification_server_mail.setter
    def stats_done_notification_server_mail(self: 'Config', value: int) -> None:
        self.config["stats"]["done"]["notification"]["server-mail"] = value

    @property
    def stats_done_notification_todo(self: 'Config') -> int:
        return int(self.config["stats"]["done"]["notification"]["todo"])

    @stats_done_notification_todo.setter
    def stats_done_notification_todo(self: 'Config', value: int) -> None:
        self.config["stats"]["done"]["notification"]["todo"] = value

    @property
    def stats_done_notification_jo_private(self: 'Config') -> int:
        return int(self.config["stats"]["done"]["notification"]["jo_private"])

    @stats_done_notification_jo_private.setter
    def stats_done_notification_jo_private(self: 'Config', value: int) -> None:
        self.config["stats"]["done"]["notification"]["jo_private"] = value
    # endregion done

    # region running
    @property
    def stats_running_time(self: 'Config') -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.config["stats"]["running"].get("time", datetime.datetime.min.isoformat()))

    @stats_running_time.setter
    def stats_running_time(self: 'Config', value: datetime.datetime) -> None:
        self.config["stats"]["running"]["time"] = value.isoformat()

    @property
    def stats_running_last_sent(self: 'Config') -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.config["stats"]["running"].get("last_sent", datetime.datetime.min.isoformat()))

    @stats_running_last_sent.setter
    def stats_running_last_sent(self: 'Config', value: datetime.datetime) -> None:
        self.config["stats"]["running"]["last_sent"] = value.isoformat()

    @property
    def stats_running_power_total(self: 'Config') -> float:
        return float(self.config["stats"]["running"].get("power_total", 0))

    @stats_running_power_total.setter
    def stats_running_power_total(self: 'Config', value: float) -> None:
        self.config["stats"]["running"]["power_total"] = value

    @property
    def stats_running_notification_server_mail(self: 'Config') -> int:
        return int(self.config["stats"]["running"]["notification"]["server-mail"])

    @stats_running_notification_server_mail.setter
    def stats_running_notification_server_mail(self: 'Config', value: int) -> None:
        self.config["stats"]["running"]["notification"]["server-mail"] = value

    @property
    def stats_running_notification_todo(self: 'Config') -> int:
        return int(self.config["stats"]["running"]["notification"]["todo"])

    @stats_running_notification_todo.setter
    def stats_running_notification_todo(self: 'Config', value: int) -> None:
        self.config["stats"]["running"]["notification"]["todo"] = value

    @property
    def stats_running_notification_jo_private(self: 'Config') -> int:
        return int(self.config["stats"]["running"]["notification"]["jo_private"])

    @stats_running_notification_jo_private.setter
    def stats_running_notification_jo_private(self: 'Config', value: int) -> None:
        self.config["stats"]["running"]["notification"]["jo_private"] = value
    # endregion running

    @property
    def min_data_window(self: 'Config') -> datetime.timedelta:
        return datetime.timedelta(minutes=float(self.config["min_data_window_minutes"]))

    @min_data_window.setter
    def min_data_window(self: 'Config', value: datetime.timedelta) -> None:
        self.config["min_data_window_minutes"] = value.total_seconds() / 60

    @property
    def min_runtime(self: 'Config') -> datetime.timedelta:
        return datetime.timedelta(minutes=float(self.config["min_runtime_minutes"]))

    @min_runtime.setter
    def min_runtime(self: 'Config', value: datetime.timedelta) -> None:
        self.config["min_runtime_minutes"] = value.total_seconds() / 60

    def __init__(self: 'Config', json_name: str, reset: bool = False) -> None:
        self.config = cast(ConfigDict, {})
        self.json_name = json_name
        self.load_config(reset)

    def load_config(self: 'Config', reset: bool = False) -> None:
        try:
            with open(self.json_name, mode='r') as file:
                self.config = json.loads(file.read())
        except FileNotFoundError:
            pass

        default = {
            "off_power": 0,
            "max_idle_power": 5,
            "min_runtime_minutes": 20,
            "min_data_window_minutes": 0.9,
            "re_remind": False,

            "stats": {
                "on": {
                    "time": datetime.datetime.min.isoformat(),
                    "last_sent": datetime.datetime.min.isoformat(),
                    "power_total": 0.0,
                    "notification": {
                        # 0: do not send
                        # 1: send, muted
                        # 2: send, with notification
                        "server-mail": 1,
                        "todo": 0,
                        "jo_private": 0,
                    },
                },
                "running": {
                    "time": datetime.datetime.min.isoformat(),
                    "last_sent": datetime.datetime.min.isoformat(),
                    "power_total": 0.0,
                    "notification": {
                        "server-mail": 0,
                        "todo": 0,
                        "jo_private": 0,
                    },
                },
                "done": {
                    "time": datetime.datetime.min.isoformat(),
                    "last_sent": datetime.datetime.min.isoformat(),
                    "power_total": 0.0,
                    "notification": {
                        "server-mail": 1,
                        "todo": 1,
                        "jo_private": 2,
                    },
                },
                "off": {
                    "time": datetime.datetime.min.isoformat(),
                    "last_sent": datetime.datetime.min.isoformat(),
                    "power_total": 0.0,
                    "notification": {
                        "server-mail": 1,
                        "todo": 0,
                        "jo_private": 0,
                    },
                },
            }
        }

        self.config = cast(ConfigDict, update_dict_recursive(cast(dict[str, Any], self.config), default, reset))  # type: ignore

    def save_config(self: 'Config') -> None:
        deprecated_keys = ["min_idle_minutes", {"stats": ["skipped_print_count"]}, "min_done_count", "min_idle_count"]
        for key in deprecated_keys:
            if isinstance(key, dict):
                for parent_key, child_keys in key.items():
                    if parent_key in self.config:
                        for child_key in child_keys:
                            if child_key in self.config[parent_key]:  # type: ignore
                                del self.config[parent_key][child_key]  # type: ignore
            else:
                if key in self.config:
                    del self.config[key]  # type: ignore

        with open(self.json_name, mode='w') as file:
            dump = json.dumps(self.config, indent=4)
            file.write(dump)


class Tasmota:
    # Copied from Felix Weichselgartner at <https://github.com/FelixWeichselgartner/Tasmota-HTTP-python>
    # Modified by me
    # GPLv3

    # Doc: https://tasmota.github.io/docs/Commands/#management
    def __init__(self: 'Tasmota', ipv4: str) -> None:
        self.ipv4 = ipv4
        self.url = f'http://{self.ipv4}/'
        self.stream_open = False

    def _get_from_xpath(self: 'Tasmota', x: str | Numeric) -> str:
        r = requests.get(self.url + '', timeout=10, )
        tree = html.fromstring(r.content)
        c: str = tree.xpath(f'{x}/text()')
        return c

    def get_name(self: 'Tasmota') -> str:
        text = self._get_from_xpath('/html/body/div/div[1]/h3')[0]
        return str(text)

    def check_output(self: 'Tasmota', number: str | Numeric) -> bytes:
        r = requests.get(f'{self.url}cm?cmnd=Power{number}%20')
        return bytes(r.content)

    def set_output(self: 'Tasmota', number: str | Numeric, state: str | Numeric) -> bytes:
        r = requests.get(f'{self.url}cm?cmnd=Power{number}%20{state}')
        return bytes(r.content)

    def get_stream_url(self: 'Tasmota') -> str:
        if not self.stream_open:
            requests.get(self.url)
            self.stream_open = True
        return f'http://{self.ipv4}:81/stream'

    def get_all_monitoring(self: 'Tasmota') -> dict[str, str]:
        r = requests.get(f'{self.url}cm?cmnd=Status%208')
        text = str(r.content)
        j = json.loads(text[2:-1])
        data: dict[str, str] = {}
        data["Time"] = j['StatusSNS']['Time']
        data["Temperature1"] = j['StatusSNS']['ANALOG']["Temperature1"]
        for k, v in j['StatusSNS']['ENERGY'].items():
            data[k] = v
        data["power1"] = json.loads(str(self.check_output(1))[2:-1])["POWER"]
        return data


def log_to_csv(ipv4: str, suppress_saving: bool = False) -> str | None:
    dev = Tasmota(ipv4)

    attribute_unit = {
        "Time": "",
        "Voltage": "V",
        "Current": "A",
        "Power": "W",
        "ApparentPower": "VA",
        "ReactivePower": "VAr",
        "Factor": "",
        "Today": "kWh",
        "Yesterday": "kWh",
        "Total": "kWh",
        "Temperature1": "°C",
        "TotalStartTime": "",
        "power1": "bool",
    }

    header = [attribute for attribute in attribute_unit.keys()]

    try:
        device_name = dev.get_name()
    except Exception as e:
        print(f"Device {ipv4} not reachable")
        print(type(e), e)
        return None

    file_name = f"{device_name}_{ipv4}_log.csv"
    if suppress_saving:
        return file_name

    # read header line from existing file
    try:
        with open(file_name, mode='r') as file:
            csv_reader = csv.reader(file, delimiter=',')
            for line in csv_reader:
                if len(line) == 0:
                    print("No header found, line empty")
                    raise FileNotFoundError
                for item in line:
                    if item not in header:
                        print("No header found, item not in header")
                        raise FileNotFoundError
                header = line
                break
            else:
                print("No header found, file empty")
                raise FileNotFoundError
    except FileNotFoundError:
        # write header line
        with open(file_name, mode='a') as file:
            csv_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(header)

    # write new line
    with open(file_name, mode='a') as file:
        csv_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        row: list[str] = []
        data = dev.get_all_monitoring()
        for attribute in header:
            if attribute in attribute_unit.keys():
                row.append(data[attribute])
        csv_writer.writerow(row)

    return file_name


def telegram_bot_sendtext(message: str, chat_id: str, disable_notification: bool = True, message_thread_id: str | None = None) -> dict[str, object]:
    escape_chars = ['_', '*', '[', ']', '(', ')', '~', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']  # not including "`", which I use to format as code
    for char in escape_chars:
        message = message.replace(char, f"\\{char}")

    home = expanduser("~")
    with open(f"{home}/Documents/erinner_bot/TOKEN", 'r') as f:
        bot_token = f.read()

    notify = "&disable_notification=true" if disable_notification else ""
    message_thread = f"&message_thread_id={message_thread_id}" if message_thread_id is not None else ""

    sendable_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=MarkdownV2&text={message}{notify}{message_thread}'

    eprint(f"{sendable_text=}")
    response = requests.get(sendable_text)
    response_json: dict[str, object] = response.json()
    eprint(type(response), response_json)
    return response_json


home = expanduser("~")
with open(f"{home}/Documents/erinner_bot/server-mail.id", 'r') as f:
    server_mail_id = f.read()
server_mail_tasmota_thread_id = "4061"

with open(f"{home}/Documents/erinner_bot/todo_group.id", 'r') as f:
    todo_id = f.read()
todo_tasmota_thread_id = None

with open(f"{home}/Documents/erinner_bot/jo_private.id", 'r') as f:
    jo_private_id = f.read()
jo_private_tasmota_thread_id = None


# region print done
def print_done(
    config: Config,
    current_done_time: datetime.datetime,
    latest_total_power: float,
    csv_log_name: str,
    suppress_message: bool,
) -> bool:
    eprint("Done")
    last_on_or_off = max(config.stats_power_on_time, config.stats_power_off_time)
    if current_done_time - last_on_or_off < config.min_runtime:
        eprint("too short")
        return False
    elif current_done_time - config.stats_running_time < config.min_data_window:
        eprint("time since actively-running less than minimum-data-window")
        return False
    else:
        eprint(f"{current_done_time=} - {last_on_or_off=} < {config.min_data_window=}     {current_done_time - last_on_or_off=}")

    # region re_remind (done)
    re_remind_now = False
    if config.re_remind:
        n_th_fib = max(300, fib(config.re_remind_counter))  # increase by Fibonacci, minimum 5 minutes
        fib_delta = datetime.timedelta(seconds=n_th_fib)
        time_since_last_sent = current_done_time - config.stats_done_last_sent
        if time_since_last_sent >= fib_delta:
            re_remind_now = True
        eprint(f"{current_done_time=} - {config.stats_done_last_sent=} = {time_since_last_sent=},  {fib_delta=}, {config.re_remind_counter=} {re_remind_now=}")
    # endregion re_remind (done)

    if last_on_or_off <= config.stats_done_last_sent:
        if not re_remind_now:
            eprint("do not re-send done message")
            return False

    if config.re_remind_counter == 0:
        config.stats_done_time = current_done_time
        config.stats_done_power_total = latest_total_power

    sending_message = config.stats_done_last_sent.year == 1 or config.stats_done_last_sent < config.stats_done_time or re_remind_now

    if not sending_message:
        eprint("already sent")
        return False
    else:
        eprint(f"{config.stats_done_last_sent=} < {config.stats_done_time=}; {re_remind_now=}")

    result_ok = True
    if suppress_message is False:
        message = f"{config.config.get('device_name', f'`{csv_log_name}`')} Fertig"
        if re_remind_now and config.re_remind_counter > 0:
            time_since_done = current_done_time - config.stats_done_time
            message += f" seit {time_since_done}\nErinnerung Nr. {config.re_remind_counter}"
        else:
            power_used = float(config.stats_done_power_total) - float(config.stats_on_power_total)
            time_used = config.stats_done_time - config.stats_power_on_time
            message += f"\n{power_used:4.2f}kWh verbraucht in {time_used}"

        if config.stats_done_notification_server_mail > 0:
            result = telegram_bot_sendtext(
                message,
                chat_id=server_mail_id,
                disable_notification=config.stats_done_notification_server_mail == 1,
                message_thread_id=server_mail_tasmota_thread_id,
            )
            result_ok = result_ok and bool(result.get("ok"))

        if config.stats_done_notification_todo > 0 and "Erinnerung" not in message:
            result = telegram_bot_sendtext(
                message,
                chat_id=todo_id,
                disable_notification=config.stats_done_notification_todo == 1,
                message_thread_id=todo_tasmota_thread_id,
            )
            result_ok = result_ok and bool(result.get("ok"))

        if config.stats_done_notification_jo_private > 0:
            result = telegram_bot_sendtext(
                message,
                chat_id=jo_private_id,
                disable_notification=config.stats_done_notification_jo_private == 1,
                message_thread_id=jo_private_tasmota_thread_id,
            )
            result_ok = result_ok and bool(result.get("ok"))
    else:
        result_ok = True

    if result_ok:
        config.stats_done_last_sent = datetime.datetime.now()
        config.re_remind_counter += 1

    return True
# endregion print done


# region print off
def print_off(
    config: Config,
    current_power_off_time: datetime.datetime,
    latest_total_power: float,
    csv_log_name: str,
    suppress_message: bool,
) -> bool:
    eprint("Off")
    last_on_or_done = max(config.stats_power_on_time, config.stats_done_time)
    if current_power_off_time - last_on_or_done < config.min_runtime:
        eprint("too short")
        return False

    if current_power_off_time - config.stats_running_time < config.min_data_window:
        eprint("time since actively-running less than minimum-data-window")
        return False

    if last_on_or_done <= config.stats_power_off_last_sent:
        eprint("do not re-send off message")
        return False

    config.stats_power_off_time = current_power_off_time
    config.stats_off_power_total = latest_total_power

    sending_message = config.stats_power_off_last_sent.year == 1 or config.stats_power_off_last_sent < config.stats_power_off_time
    if not sending_message:
        return False

    result_ok = True
    if suppress_message is False:
        if config.stats_off_notification_server_mail > 0:
            result = telegram_bot_sendtext(
                f"{config.config.get('device_name', f'`{csv_log_name}`')} aus",
                chat_id=server_mail_id,
                disable_notification=config.stats_done_notification_server_mail == 1,
                message_thread_id=server_mail_tasmota_thread_id,
            )
            result_ok = result_ok and bool(result.get("ok"))

        if config.stats_off_notification_todo > 0:
            result = telegram_bot_sendtext(
                f"{config.config.get('device_name', f'`{csv_log_name}`')} aus",
                chat_id=todo_id,
                disable_notification=config.stats_done_notification_todo == 1,
                message_thread_id=todo_tasmota_thread_id,
            )
            result_ok = result_ok and bool(result.get("ok"))

        if config.stats_off_notification_jo_private > 0:
            result = telegram_bot_sendtext(
                f"{config.config.get('device_name', f'`{csv_log_name}`')} aus",
                chat_id=jo_private_id,
                disable_notification=config.stats_done_notification_jo_private == 1,
                message_thread_id=jo_private_tasmota_thread_id,
            )
            result_ok = result_ok and bool(result.get("ok"))
    else:
        result_ok = True

    if result_ok:
        config.stats_power_off_last_sent = datetime.datetime.now()

    config.re_remind_counter = 0

    return True
# endregion print off


# region print on
def print_on(
    config: Config,
    current_power_on_time: datetime.datetime,
    csv_log_name: str,
    lines: list[list[str]],
    header: list[str],
    suppress_message: bool,
) -> bool:
    eprint("On")
    if config.stats_power_on_last_sent >= current_power_on_time:
        eprint("already sent for current on-event")
        return False

    last_off_or_done = max(config.stats_power_off_time, config.stats_done_time)
    if last_off_or_done <= config.stats_power_on_last_sent:
        eprint("do not re-send on message")
        return False

    config.stats_power_on_time = current_power_on_time
    config.stats_on_power_total = float(lines[-1][header.index("Total")])

    result_ok = True
    if suppress_message is False:
        if config.stats_on_notification_server_mail > 0:
            result = telegram_bot_sendtext(
                f"{config.config.get('device_name', f'`{csv_log_name}`')} gestartet",
                chat_id=server_mail_id,
                disable_notification=config.stats_done_notification_server_mail == 1,
                message_thread_id=server_mail_tasmota_thread_id,
            )
            result_ok = result_ok and bool(result.get("ok"))

        if config.stats_on_notification_todo > 0:
            result = telegram_bot_sendtext(
                f"{config.config.get('device_name', f'`{csv_log_name}`')} gestartet",
                chat_id=todo_id,
                disable_notification=config.stats_done_notification_todo == 1,
                message_thread_id=todo_tasmota_thread_id,
            )
            result_ok = result_ok and bool(result.get("ok"))

        if config.stats_on_notification_jo_private > 0:
            result = telegram_bot_sendtext(
                f"{config.config.get('device_name', f'`{csv_log_name}`')} gestartet",
                chat_id=jo_private_id,
                disable_notification=config.stats_done_notification_jo_private == 1,
                message_thread_id=jo_private_tasmota_thread_id,
            )
            result_ok = result_ok and bool(result.get("ok"))
    else:
        result_ok = True

    if result_ok:
        config.stats_power_on_last_sent = datetime.datetime.now()

    config.re_remind_counter = 0

    return True
# endregion print on


def check_status(csv_log_name: str, mock_run_offset_from_end: int = 0, mock_reset_stats: bool = False, interval: float = 10) -> None:
    # read csv file
    with open(csv_log_name, mode='r') as file:
        csv_reader = csv.reader(file, delimiter=',')
        header = next(csv_reader)
        lines = list(csv_reader)
        lines = lines[:len(lines) - mock_run_offset_from_end + 1]

    json_name = csv_log_name.replace(".csv", ".json")

    if mock_run_offset_from_end > 0:
        json_name = csv_log_name.replace(".csv", ".mock_run.json")

    config = Config(json_name, mock_reset_stats)

    time_earliest = datetime.datetime.max
    time_latest = datetime.datetime.min
    last_total_power = 0.0
    power_list: list[float] = []
    for count, line in enumerate(lines[::-1]):
        power = float(line[header.index("Power")])
        power_list.append(power)
        time = datetime.datetime.fromisoformat(line[header.index("Time")])
        time_latest = max(time_latest or time, time)
        time_earliest = min(time_earliest or time, time)

        delta = time_latest - time_earliest
        if count > config.min_data_window.total_seconds() / interval and delta > config.min_data_window:
            break

        last_total_power = float(line[header.index("Total")])
        # print(f"{count=}, {min_time=}, {max_time=}, {delta=}")

    power_list = power_list[::-1]  # reverse back

    min_power = min(power_list)
    median_power = statistics.median(power_list)
    mean_power = statistics.mean(power_list)
    max_power = max(power_list)

    sent_on = sent_off = sent_done = sent_running = False
    fall_through = False
    if all(power <= config.min_off_power for power in power_list[:-1]) and power_list[-1] > config.min_off_power:
        sent_on = print_on(config, time_latest, csv_log_name, lines, header, mock_run_offset_from_end > 0)
    elif all(power >= config.max_idle_power for power in power_list):
        config.stats_running_time = time_latest
        config.re_remind_counter = 0  # always reset re-remind counter if running
        if config.stats_power_on_time < config.stats_done_time and config.stats_power_on_time < config.stats_power_off_time:
            eprint("fallback set on-time anyway if missed", f"{config.stats_power_on_time=} < {config.stats_done_time=} and {config.stats_power_on_time=} < {config.stats_power_off_time=}")
            sent_on = print_on(config, time_latest, csv_log_name, lines, header, mock_run_offset_from_end > 0)

        sent_running = True
    elif median_power <= config.min_off_power:
        sent_off = print_off(config, time_earliest, last_total_power, csv_log_name, mock_run_offset_from_end > 0)
    elif config.min_off_power <= median_power <= config.max_idle_power:
        sent_done = print_done(config, time_earliest, last_total_power, csv_log_name, mock_run_offset_from_end > 0)
    else:
        fall_through = True

    if fall_through:
        eprint(csv_log_name, f"{power_list=} {min_power=}, {median_power=}, {mean_power=}, {max_power=}   FALLTHROUGH   FALLTHROUGH   FALLTHROUGH")
    else:
        eprint(csv_log_name, f"{power_list=} {min_power=}, {median_power=}, {mean_power=}, {max_power=} — {sent_on=}, {sent_off=}, {sent_done=}, {sent_running=}")

    config.save_config()

    # eprint(f"{len(lines)=}")
    if mock_reset_stats:
        print(",".join(header + ["sent_on", "sent_off", "sent_done", "sent_running"]))
    if mock_run_offset_from_end:
        print(",".join(lines[-1] + [str(sent_on), str(sent_off), str(sent_done), str(sent_running)]))

    # print(csv_log_name)
    # print(header)
    # for line in lines[-5:]:
    #     print(line)


def prune_file(csv_log_name: str, _is_implemented: bool = True) -> None:
    # open file
    # compare triplets of lines values
    # if center line is the same as the one above and below, remove it
    # always keep last N lines from the original, so as not to confuse recency logic in other places

    if not _is_implemented:
        return

    interesting_keys = ["Power", "Total", "TotalStartTime", "power1"]

    keep_n_lines = 100

    with open(csv_log_name, mode='r') as file:
        csv_reader = csv.reader(file, delimiter=',')
        header = next(csv_reader)
        all_lines = list(csv_reader)
        lines = all_lines[:-keep_n_lines]
        lines_kept_back = all_lines[-keep_n_lines:]

    kept_lines = [lines[0]]

    for line_triplet in triplewise(lines):
        for key in interesting_keys:
            prev_val = line_triplet[0][header.index(key)]
            curr_val = line_triplet[1][header.index(key)]

            if prev_val != curr_val:
                kept_lines.append(line_triplet[1])
                break

            next_val = line_triplet[2][header.index(key)]

            if curr_val != next_val:
                kept_lines.append(line_triplet[1])
                break

    kept_lines.append(lines[-1])
    kept_lines.extend(lines_kept_back)

    lines = kept_lines

    with open(csv_log_name, mode='w') as file:
        csv_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(header)
        for line in lines:
            csv_writer.writerow(line)


def do_once(ipv4: str, debug: bool = False, interval: float = 10) -> None:
    file_name = log_to_csv(ipv4, suppress_saving=debug)
    if file_name is None:
        return

    if debug:
        length_of_log = None
        with open(file_name, mode='r') as f:
            length_of_log = len(f.readlines())

        if length_of_log:
            eprint(f"debugging ip {ipv4=} with {file_name=}")
            for offset_from_end in tqdm.tqdm(range(length_of_log - 1, -1, -1), dynamic_ncols=True):
                # print()
                # eprint(f"{offset_from_end=}")
                check_status(file_name, offset_from_end, offset_from_end == length_of_log - 1, interval=interval)
    else:
        check_status(file_name, interval=interval)
        prune_file(file_name)


def main() -> None:
    parser = argparse.ArgumentParser(description="log tasmota devices info to csv")
    parser.add_argument("-d", help="debug, print for all lines", default=False, action="store_true",)

    args = parser.parse_args()
    debug = args.d

    ips = [
        "192.168.2.77",  # WMS
        "192.168.2.107",  # TRK
        "192.168.2.134",  # SPM
    ]

    total_start = time.time()
    interval = 10
    for i in range(interval, 60 + 61 % interval, interval):
        end = float("inf")
        for ip in ips:
            start = time.time()
            print()
            do_once(ip, debug, interval)
            end = time.time()
            eprint(ip, end - total_start, end - start)
            eprint()
        if i < 60:
            sleep_length = i - (end - total_start)

            if sleep_length > 0:
                time.sleep(sleep_length)


if __name__ == "__main__":
    main()
