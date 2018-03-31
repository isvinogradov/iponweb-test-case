import argparse
import os
import re
from collections import defaultdict
from datetime import datetime


CATCH_PATTERN = '\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+\d{2}:\d{2}).* event_type=(\w+)'
TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f+00:00'
FREQUENCY_PRECISION = 6


def process_log(filepath):
    ts_max = None
    ts_min = None
    type_data = defaultdict(int)
    compiled_patt = re.compile(CATCH_PATTERN)

    # parsing file
    with open(filepath, 'r') as f:
        for line in f:
            matched = compiled_patt.match(line)
            if not matched:
                continue  # ignoring invalid log record

            record_dt_str, event_type = matched.group(1), matched.group(2)
            try:
                ts = datetime.strptime(record_dt_str, TIME_FORMAT)
            except ValueError:
                continue  # ignoring records with invalid date format

            type_data[event_type] += 1
            if not ts_max or ts > ts_max:
                ts_max = ts
            if not ts_min or ts < ts_min:
                ts_min = ts

    # printing results
    if not type_data:
        return  # no data to print -> file is empty or does not contain any valid records

    duration = (ts_max - ts_min).total_seconds()
    if not duration:  # otherwise we will get ZeroDivisionError later
        print('WARNING: minimum timestamp is equal to maximum timestamp -> setting all type frequencies to zero')

    for e_type, num_records in type_data.items():
        print('event type: {}, records: {}, frequency: {} rec/s'.format(
            e_type,
            num_records,
            round(num_records / duration, FREQUENCY_PRECISION) if duration else 0,
        ))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filepath', help='Log file path')
    options = parser.parse_args()

    if not os.path.exists(options.filepath):
        print('error: specified file does not exist')
        exit(1)

    if not os.path.isfile(options.filepath):
        print('error: specified path is not a file')
        exit(1)

    process_log(options.filepath)

