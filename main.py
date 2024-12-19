#!/usr/bin/env python3

import csv
import json
from numbers import Number
from typing import Dict, SupportsComplex

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


def log_to_csv(ipv4: str) -> None:
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
        return

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


log_to_csv("192.168.2.107")
log_to_csv("192.168.2.77")
log_to_csv("192.168.2.134")
