#!/usr/bin/env python3

import csv
import datetime
import json
import time
from numbers import Number
from os.path import expanduser
from typing import Dict, Optional, SupportsComplex

import requests
from lxml import html

Numeric = SupportsComplex | Number | int

# Doc: https://tasmota.github.io/docs/Commands/#management


class Tasmota:
    # Copied from Felix Weichselgartner at <https://github.com/FelixWeichselgartner/Tasmota-HTTP-python>
    # Modified by me
    # GPLv3

    def __init__(self: 'Tasmota', ipv4: str) -> None:
        self.ipv4 = ipv4
        self.url = f'http://{self.ipv4}/'
        self.stream_open = False

    def _get_from_xpath(self: 'Tasmota', x: str | Numeric):  # type: ignore[no-untyped-def]
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


def log_to_csv(ipv4: str) -> Optional[str]:
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


def telegram_bot_sendtext(message: str, chat_id: str, disable_notification: bool = True, message_thread_id: Optional[str] = None) -> None:
    message = message.replace(".", "\\.")

    home = expanduser("~")
    with open(f"{home}/Documents/erinner_bot/TOKEN", 'r') as f:
        bot_token = f.read()
    if message_thread_id is not None:
        send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=MarkdownV2&text={message}&disable_notification={disable_notification}&message_thread_id={message_thread_id}'
    else:
        send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=MarkdownV2&text={message}&disable_notification={disable_notification}'

    print(f"{send_text=}")
    response = requests.get(send_text)
    print(type(response), response)
    return response.json()

home = expanduser("~")
with open(f"{home}/Documents/erinner_bot/server-mail.id", 'r') as f:
    server_mail_id = f.read()
tasmota_thread_id = "4061"
# print(telegram_bot_sendtext("Test", server_mail_id, True, tasmota_thread_id))

# exit()

def check_done(csv_log_name: str, ipv4: str) -> None:
    # read csv file
    with open(csv_log_name, mode='r') as file:
        csv_reader = csv.reader(file, delimiter=',')
        header = next(csv_reader)
        lines = list(csv_reader)

    json_name = csv_log_name.replace(".csv", ".json")

    try:
        with open(json_name, mode='r') as file:
            config = json.loads(file.read())
    except FileNotFoundError:
        config = {}

    min_off_power = config.get("off_power", 0)
    max_idle_power = config.get("max_idle_power", 5)
    min_idle_minutes = config.get("min_idle_minutes", 1)
    min_idle_count = config.get("min_idle_count", 5)
    min_done_count = config.get("min_done_count", 4)

    # set defaults
    config["off_power"] = min_off_power
    config["max_idle_power"] = max_idle_power
    config["min_idle_minutes"] = min_idle_minutes
    config["min_idle_count"] = min_idle_count
    config["min_done_count"] = min_done_count


    done_count = 0
    off_count = 0
    min_time = None
    max_time = None
    last_total_power = None
    for count, line in enumerate(lines[::-1]):
        power = float(line[header.index("Power")])
        time = datetime.datetime.fromisoformat(line[header.index("Time")])
        max_time = max(max_time or time, time)
        min_time = min(min_time or time, time)

        delta = max_time - min_time
        idle_delta = datetime.timedelta(minutes=min_idle_minutes)
        # print(f"{count=}, {min_idle_count=}, {delta=}, {idle_delta=}")
        if count > min_idle_count and delta > idle_delta:
            break

        last_total_power = line[header.index("Total")]
        if min_off_power < power <= max_idle_power:
            done_count += 1
        if power <= min_off_power:
            off_count += 1
        # print(f"{count=}, {done_count=}, {min_time=}, {max_time=}, {delta=}")

    if "stats" not in config:
        config["stats"] = {}
    if "done" not in config["stats"]:
        config["stats"]["done"] = {}
    if "off" not in config["stats"]:
        config["stats"]["off"] = {}
    if "on" not in config["stats"]:
        config["stats"]["on"] = {}

    stats_time_on   = datetime.datetime.fromisoformat(config["stats"]["on"].get("time", datetime.datetime.min.isoformat()))
    stats_time_off  = datetime.datetime.fromisoformat(config["stats"]["off"].get("time", datetime.datetime.min.isoformat()))
    stats_time_done = datetime.datetime.fromisoformat(config["stats"]["done"].get("time", datetime.datetime.min.isoformat()))

    if done_count >= min_done_count:
        print("Done")
        if stats_time_on > stats_time_done or stats_time_off > stats_time_done or stats_time_done.year == 1:
            config["stats"]["done"]["time"] = min_time.isoformat()
            config["stats"]["done"]["power_total"] = last_total_power
        last_sent_time = datetime.datetime.fromisoformat(config["stats"]["done"].get("last_sent", datetime.datetime.min.isoformat()))
        if last_sent_time.year == 1 or last_sent_time < datetime.datetime.fromisoformat(config["stats"]["done"].get("time", datetime.datetime.min.isoformat())):
            power_done = config["stats"]["done"]["power_total"]
            power_start = config["stats"]["on"].get("power_total", 0)
            power_used = float(power_done) - float(power_start)

            time_on = datetime.datetime.fromisoformat(config["stats"]["on"].get("time", datetime.datetime.min.isoformat()))
            time_done = datetime.datetime.fromisoformat(config["stats"]["done"]["time"])
            time_used = time_done - time_on

            result = telegram_bot_sendtext(f"{config.get('device_name', f'`{csv_log_name}`')} Fertig\n{power_used}W verbraucht in {time_used}", server_mail_id, False, tasmota_thread_id)
            if result.get("ok"):
                config["stats"]["done"]["last_sent"] = datetime.datetime.now().isoformat()


    if off_count >= min_done_count:
        print("Off")
        if stats_time_on > stats_time_off or stats_time_done > stats_time_off or stats_time_off.year == 1:
            config["stats"]["off"]["time"] = min_time.isoformat()
            config["stats"]["off"]["power_total"] = last_total_power
        last_sent_time = datetime.datetime.fromisoformat(config["stats"]["off"].get("last_sent", datetime.datetime.min.isoformat()))
        if last_sent_time.year == 1 or last_sent_time < datetime.datetime.fromisoformat(config["stats"]["off"].get("time", datetime.datetime.min.isoformat())):
            result = telegram_bot_sendtext(f"{config.get('device_name', f'`{csv_log_name}`')} aus", server_mail_id, True, tasmota_thread_id)
            if result.get("ok"):
                config["stats"]["off"]["last_sent"] = datetime.datetime.now().isoformat()

    if off_count >= min_done_count -1 and float(lines[-1][header.index("Power")]) > min_off_power:
        print("On")
        if stats_time_off > stats_time_on or stats_time_done > stats_time_on or stats_time_on.year == 1:
            config["stats"]["on"]["time"] = max_time.isoformat()
            config["stats"]["on"]["power_total"] = lines[-1][header.index("Total")]
        last_sent_time = datetime.datetime.fromisoformat(config["stats"]["on"].get("last_sent", datetime.datetime.min.isoformat()))
        if last_sent_time.year == 1 or last_sent_time < datetime.datetime.fromisoformat(config["stats"]["on"].get("time", datetime.datetime.min.isoformat())):
            result = telegram_bot_sendtext(f"{config.get('device_name', f'`{csv_log_name}`')} gestartet", server_mail_id, True, tasmota_thread_id)
            if result.get("ok"):
                config["stats"]["on"]["last_sent"] = datetime.datetime.now().isoformat()

    print(config)
    with open(json_name, mode='w') as file:
        dump = json.dumps(config, indent=4)
        file.write(dump)

    print(csv_log_name)
    print(header)
    for line in lines[-5:]:
        print(line)


def prune_file(csv_log_name: str) -> None:
    pass


def do_once(ipv4: str) -> None:
    file = log_to_csv(ipv4)
    check_done(file, ipv4)
    prune_file(file)


def main():
    ips = [
        "192.168.2.77",  # WMS
        "192.168.2.107",  # TRK
        "192.168.2.134",  # SPM
    ]

    total_start = time.time()
    for i in range(10, 61, 10):
        for ip in ips:
            start = time.time()
            do_once(ip)
            end = time.time()
            print(end - total_start, end - start, ip)
            print()
        time.sleep(i - (end - total_start))


if __name__ == "__main__":
    main()

