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
from numbers import Number
from os.path import expanduser
from typing import Dict, List, Optional, SupportsComplex

import requests
import tqdm
from lxml import html

Numeric = SupportsComplex | Number | int | float


def eprint(*args, **kwargs) -> None:  # type: ignore
    print(f"{inspect.stack()[1][1]}:{inspect.stack()[1][2]};{inspect.stack()[1][3]}", *args, file=sys.stderr, **kwargs)


def fib(n: int) -> int:
    p = (1 + math.sqrt(5)) / 2
    q = (1 - math.sqrt(5)) / 2
    return int((p**n - q**n) / math.sqrt(5))


def update_dict_recursive(config: Dict, default: Dict, reset: bool = False) -> Dict:
    for default_key, default_value in default.items():
        if isinstance(default_value, dict):
            if default_key not in config:
                config[default_key] = {}
            foo = config[default_key]
            config[default_key] = update_dict_recursive(foo, default_value)
        else:
            if reset:
                config[default_key] = default_value
            else:
                config[default_key] = config.get(default_key, default_value)
    return config


class Config():
    json_name: str

    config: Dict
    min_off_power: float
    max_idle_power: float
    min_data_window_minutes: float
    min_idle_count: int
    min_done_count: int

    re_remind: bool
    re_remind_counter: int

    last_power_on_time: datetime.datetime = datetime.datetime.min
    last_power_off_time: datetime.datetime = datetime.datetime.min
    last_done_time: datetime.datetime = datetime.datetime.min
    min_data_window: datetime.timedelta
    min_runtime: datetime.timedelta

    def __init__(self: 'Config', json_name: str, reset: bool = False) -> None:
        self.config = {}
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
            "min_idle_count": 5,
            "min_done_count": 4,
            "re_remind": False,
            "re_remind_counter": 0,
            "stats": {
                "skipped_print_count": 0,
                "on": {
                    "last_sent": datetime.datetime.min.isoformat(),
                    "power_total": 0
                },
                "off": {
                    "last_sent": datetime.datetime.min.isoformat(),
                    "power_total": 0
                },
                "done": {
                    "last_sent": datetime.datetime.min.isoformat(),
                    "power_total": 0
                },
            }
        }

        self.config = update_dict_recursive(self.config, default, reset)

        self.min_off_power           = float(self.config["off_power"])                # noqa: E221
        self.max_idle_power          = float(self.config["max_idle_power"])           # noqa: E221
        self.min_data_window_minutes = float(self.config["min_data_window_minutes"])  # noqa: E221
        self.min_idle_count          = int(self.config["min_idle_count"])           # noqa: E221
        self.min_done_count          = int(self.config["min_done_count"])           # noqa: E221

        self.re_remind = bool(self.config["re_remind"])
        self.re_remind_counter = int(self.config["re_remind_counter"])

        self.last_power_on_time   = datetime.datetime.fromisoformat(self.config["stats"]["on"  ].get("time", datetime.datetime.min.isoformat()))  # noqa E221
        self.last_power_off_time  = datetime.datetime.fromisoformat(self.config["stats"]["off" ].get("time", datetime.datetime.min.isoformat()))  # noqa E221
        self.last_done_time       = datetime.datetime.fromisoformat(self.config["stats"]["done"].get("time", datetime.datetime.min.isoformat()))  # noqa E221
        self.min_data_window      = datetime.timedelta(minutes=self.min_data_window_minutes)        # noqa E221
        self.min_runtime          = datetime.timedelta(minutes=self.config["min_runtime_minutes"])  # noqa E221

    def save_config(self: 'Config') -> None:
        self.config["off_power"              ] = self.min_off_power                     # noqa: E221, E202
        self.config["max_idle_power"         ] = self.max_idle_power                    # noqa: E221, E202
        self.config["min_data_window_minutes"] = self.min_data_window_minutes           # noqa: E221, E202
        self.config["min_idle_count"         ] = self.min_idle_count                    # noqa: E221, E202
        self.config["min_done_count"         ] = self.min_done_count                    # noqa: E221, E202
        self.config["min_runtime_minutes"    ] = self.min_runtime.total_seconds() / 60  # noqa: E221, E202

        self.config["re_remind"             ] = self.re_remind                 # noqa: E221, E202
        self.config["re_remind_counter"     ] = self.re_remind_counter         # noqa: E221, E202

        self.config["stats"]["on"  ]["time"] = self.last_power_on_time.isoformat()   # noqa: E221, E202
        self.config["stats"]["off" ]["time"] = self.last_power_off_time.isoformat()  # noqa: E221, E202
        self.config["stats"]["done"]["time"] = self.last_done_time.isoformat()       # noqa: E221, E202

        deprecated_keys = ["min_idle_minutes"]
        for key in deprecated_keys:
            if key in self.config:
                del self.config[key]

        with open(self.json_name, mode='w') as file:
            dump = json.dumps(self.config, indent=4)
            file.write(dump)

    def increase_skipped_count(self: 'Config') -> None:
        self.config["stats"]["skipped_print_count"] = self.config["stats"].get("skipped_print_count", 0) + 1

    def reset_skipped_count(self: 'Config') -> None:
        self.config["stats"]["skipped_print_count"] = 0

    @property
    def skipped_count(self: 'Config') -> int:
        return int(self.config["stats"].get("skipped_print_count", 0))


class Tasmota:
    # Copied from Felix Weichselgartner at <https://github.com/FelixWeichselgartner/Tasmota-HTTP-python>
    # Modified by me
    # GPLv3

    # Doc: https://tasmota.github.io/docs/Commands/#management
    def __init__(self: 'Tasmota', ipv4: str) -> None:
        self.ipv4 = ipv4
        self.url = f'http://{self.ipv4}/'
        self.stream_open = False

    def _get_from_xpath(self: 'Tasmota', x: str | Numeric):  # type: ignore
        r = requests.get(self.url + '', timeout=10, )
        tree = html.fromstring(r.content)
        c = tree.xpath(f'{x}/text()')
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

    def get_all_monitoring(self: 'Tasmota') -> Dict:
        r = requests.get(f'{self.url}cm?cmnd=Status%208')
        text = str(r.content)
        j = json.loads(text[2:-1])
        data = {}
        data["Time"] = j['StatusSNS']['Time']
        data["Temperature1"] = j['StatusSNS']['ANALOG']["Temperature1"]
        for k, v in j['StatusSNS']['ENERGY'].items():
            data[k] = v
        data["power1"] = json.loads(str(self.check_output(1))[2:-1])["POWER"]
        return data


def log_to_csv(ipv4: str, suppress_saving: bool = False) -> Optional[str]:
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
        row = []
        data = dev.get_all_monitoring()
        for attribute in header:
            if attribute in attribute_unit.keys():
                row.append(data[attribute])
        csv_writer.writerow(row)

    return file_name


def telegram_bot_sendtext(message: str, chat_id: str, disable_notification: bool = True, message_thread_id: Optional[str] = None) -> Dict:
    escape_chars = ['_', '*', '[', ']', '(', ')', '~', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']  # not including "`", which I use to format as code
    for char in escape_chars:
        message = message.replace(char, f"\\{char}")

    home = expanduser("~")
    with open(f"{home}/Documents/erinner_bot/TOKEN", 'r') as f:
        bot_token = f.read()
    if message_thread_id is not None:
        send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=MarkdownV2&text={message}&disable_notification={disable_notification}&message_thread_id={message_thread_id}'
    else:
        send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=MarkdownV2&text={message}&disable_notification={disable_notification}'

    print(f"{send_text=}")
    response = requests.get(send_text)
    response_json: Dict = response.json()
    print(type(response), response_json)
    return response_json


home = expanduser("~")
with open(f"{home}/Documents/erinner_bot/server-mail.id", 'r') as f:
    server_mail_id = f.read()
tasmota_thread_id = "4061"
# print(telegram_bot_sendtext("Test", server_mail_id, True, tasmota_thread_id))

# exit()


def print_done(
    config: Config,
    min_time: datetime.datetime,
    last_total_power: str,
    csv_log_name: str,
    suppress_message: bool,
) -> bool:
    eprint("Done")
    last_sent_time = datetime.datetime.fromisoformat(config.config["stats"]["done"].get("last_sent", datetime.datetime.min.isoformat()))

    comparable_time = max(config.last_done_time, last_sent_time)
    if (
        config.last_power_on_time + config.min_data_window > comparable_time
        or
        config.last_power_off_time + config.min_data_window > comparable_time
        or
        config.last_done_time.year == 1  # done is missing
    ):
        config.config["stats"]["done"]["time"] = min_time.isoformat()
        config.config["stats"]["done"]["power_total"] = last_total_power

    sending_message = last_sent_time.year == 1 or last_sent_time < config.last_done_time
    if sending_message:
        power_done = config.config["stats"]["done"]["power_total"]
        power_start = config.config["stats"]["on"].get("power_total", 0)
        power_used = float(power_done) - float(power_start)

        time_on = datetime.datetime.fromisoformat(config.config["stats"]["on"].get("time", datetime.datetime.min.isoformat()))
        time_done = datetime.datetime.fromisoformat(config.config["stats"]["done"]["time"])
        time_used = time_done - time_on

        if suppress_message is False:
            result = telegram_bot_sendtext(f"{config.config.get('device_name', f'`{csv_log_name}`')} Fertig\n{power_used:4.2f}kWh verbraucht in {time_used}", server_mail_id, False, tasmota_thread_id)
            if result.get("ok"):
                config.config["stats"]["done"]["last_sent"] = datetime.datetime.now().isoformat()
        else:
            config.config["stats"]["done"]["last_sent"] = datetime.datetime.now().isoformat()
    # config.save_config()
    # config.load_config()
    return sending_message


def print_off(
    config: Config,
    min_time: datetime.datetime,
    last_total_power: str,
    csv_log_name: str,
    suppress_message: bool,
) -> bool:
    eprint("Off")
    last_sent_time = datetime.datetime.fromisoformat(config.config["stats"]["off"].get("last_sent", datetime.datetime.min.isoformat()))

    comparable_time = max(config.last_power_off_time, last_sent_time)
    # eprint(f"{config.last_power_off_time=}, {last_sent_time=}, {comparable_time=}")
    if (
        config.last_power_on_time + config.min_data_window > comparable_time
        or
        config.last_done_time + config.min_data_window > comparable_time
        or
        config.last_power_off_time.year == 1
    ):
        eprint(f"{config.last_power_on_time=}, {config.last_done_time=}, {config.last_power_off_time=}")
        eprint(f"{config.last_power_on_time + config.min_data_window=}, {config.last_done_time + config.min_data_window=}, {config.last_power_off_time.year=}")
        config.config["stats"]["off"]["time"] = min_time.isoformat()
        config.config["stats"]["off"]["power_total"] = last_total_power

    sending_message = last_sent_time.year == 1 or last_sent_time < datetime.datetime.fromisoformat(config.config["stats"]["off"].get("time", datetime.datetime.min.isoformat()))
    if sending_message:
        if suppress_message is False:
            result = telegram_bot_sendtext(f"{config.config.get('device_name', f'`{csv_log_name}`')} aus", server_mail_id, True, tasmota_thread_id)
            if result.get("ok"):
                config.config["stats"]["off"]["last_sent"] = datetime.datetime.now().isoformat()
        else:
            config.config["stats"]["off"]["last_sent"] = datetime.datetime.now().isoformat()
    # config.save_config()
    # config.load_config()
    return sending_message


def print_on(
    config: Config,
    max_time: datetime.datetime,
    csv_log_name: str,
    lines: List[List[str]],
    header: List[str],
    suppress_message: bool,
) -> bool:
    eprint("On")
    last_sent_time = datetime.datetime.fromisoformat(config.config["stats"]["on"].get("last_sent", datetime.datetime.min.isoformat()))

    comparable_time = max(config.last_power_on_time, last_sent_time)
    if (
        config.last_power_off_time > comparable_time
        or
        config.last_done_time > comparable_time
        or
        config.last_power_on_time.year == 1
    ):
        config.config["stats"]["on"]["time"] = max_time.isoformat()
        config.config["stats"]["on"]["power_total"] = lines[-1][header.index("Total")]

    sending_message = last_sent_time.year == 1 or last_sent_time < datetime.datetime.fromisoformat(config.config["stats"]["on"].get("time", datetime.datetime.min.isoformat()))
    if sending_message:
        if suppress_message is False:
            result = telegram_bot_sendtext(f"{config.config.get('device_name', f'`{csv_log_name}`')} gestartet", server_mail_id, True, tasmota_thread_id)
            if result.get("ok"):
                config.config["stats"]["on"]["last_sent"] = datetime.datetime.now().isoformat()
        else:
            config.config["stats"]["on"]["last_sent"] = datetime.datetime.now().isoformat()
    # config.save_config()
    # config.load_config()
    return sending_message


def check_status(csv_log_name: str, mock_run_offset_from_end: int = 0, mock_reset_stats: bool = False) -> None:
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

    done_count = 0
    off_count = 0
    time_earliest = datetime.datetime.max
    time_latest = datetime.datetime.min
    last_total_power = "0"
    power_lst = []
    for count, line in enumerate(lines[::-1]):
        power = float(line[header.index("Power")])
        power_lst += [power]
        time = datetime.datetime.fromisoformat(line[header.index("Time")])
        time_latest = max(time_latest or time, time)
        time_earliest = min(time_earliest or time, time)

        delta = time_latest - time_earliest
        idle_delta = datetime.timedelta(minutes=config.min_data_window_minutes)
        # print(f"{count=}, {min_idle_count=}, {delta=}, {idle_delta=}")
        if count > config.min_idle_count and delta > idle_delta:
            break

        last_total_power = line[header.index("Total")]
        if config.min_off_power < power <= config.max_idle_power:
            done_count += 1
        if power <= config.min_off_power:
            off_count += 1
        # print(f"{count=}, {done_count=}, {min_time=}, {max_time=}, {delta=}")

    min_power = min(power_lst)
    max_power = max(power_lst)
    mean_power = statistics.mean(power_lst)
    median_power = statistics.median(power_lst)

    eprint(csv_log_name, f"{power_lst=} {min_power=}, {median_power=}, {mean_power=}, {max_power=}")

    if (
        # time_latest - config.last_power_on_time < config.min_runtime
        # or
        time_latest - config.last_power_off_time < config.min_runtime
        or
        time_latest - config.last_done_time < config.min_runtime
    ):
        config.increase_skipped_count()
    else:
        config.reset_skipped_count()

    sent_on = sent_off = sent_done = False
    eprint(csv_log_name, f"{config.skipped_count < config.min_done_count=}, {config.skipped_count=} < {config.min_done_count=}")
    if config.skipped_count < config.min_done_count:
        eprint(csv_log_name, f"{off_count >= config.min_done_count - 1 and float(lines[-1][header.index('Power')]) > config.min_off_power=}, {off_count=} >= {config.min_done_count - 1=} and {float(lines[-1][header.index('Power')])=} > {config.min_off_power=}")
        if off_count >= config.min_done_count - 1 and float(lines[-1][header.index("Power")]) > config.min_off_power:
            eprint(csv_log_name, "calling print_on")
            sent_on = print_on(config, time_latest, csv_log_name, lines, header, mock_run_offset_from_end > 0)
            eprint(csv_log_name, f"        print_on: {sent_on=}")

        eprint(csv_log_name, f"{off_count >= config.min_done_count=}, {off_count=} >= {config.min_done_count=}")
        if off_count >= config.min_done_count:
            eprint(csv_log_name, "calling print_off")
            sent_off = print_off(config, time_earliest, last_total_power, csv_log_name, mock_run_offset_from_end > 0)
            eprint(csv_log_name, f"        print_off: {sent_off=}")

        eprint(csv_log_name, f"{done_count >= config.min_done_count=}, {done_count=} >= {config.min_done_count=}")
        if done_count >= config.min_done_count:
            eprint(csv_log_name, "calling print_done")
            sent_done = print_done(config, time_earliest, last_total_power, csv_log_name, mock_run_offset_from_end > 0)
            eprint(csv_log_name, f"        print_done: {sent_done=}")

    config.save_config()

    # eprint(f"{len(lines)=}")
    if mock_reset_stats:
        print(",".join(header + ["sent_on", "sent_off", "sent_done"]))
    if mock_run_offset_from_end:
        print(",".join(lines[-1] + [str(sent_on), str(sent_off), str(sent_done)]))

    # print(csv_log_name)
    # print(header)
    # for line in lines[-5:]:
    #     print(line)


def prune_file(csv_log_name: str) -> None:
    pass


def do_once(ipv4: str, debug: bool = False) -> None:
    file_name = log_to_csv(ipv4, suppress_saving=debug)
    if file_name is None:
        return

    if debug:
        length_of_log = None
        with open(file_name, mode='r') as f:
            length_of_log = len(f.readlines())

        if length_of_log:
            print(f"debugging ip {ipv4=} with {file_name=}")
            for offset_from_end in tqdm.tqdm(range(length_of_log - 1, -1, -1), dynamic_ncols=True):
                # print()
                # eprint(f"{offset_from_end=}")
                check_status(file_name, offset_from_end, offset_from_end == length_of_log - 1)
    else:
        check_status(file_name)
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
    for i in range(10, 61, 10):
        for ip in ips:
            start = time.time()
            do_once(ip, debug)
            end = time.time()
            eprint(ip, end - total_start, end - start)
            eprint()
        if i < 60:
            time.sleep(i - (end - total_start))


if __name__ == "__main__":
    main()
