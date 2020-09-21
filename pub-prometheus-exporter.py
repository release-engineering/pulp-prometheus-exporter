#!/usr/bin/env python3
""" A simple prometheus exporter for pub.

Scrapes pub on an interval and exposes metrics about pushes.
"""

import datetime
import json
import logging
import os
import time
import xmlrpc.client

import arrow

from prometheus_client.core import (
    REGISTRY,
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
)
from prometheus_client import start_http_server

START = None

PUB_URL = os.environ['PUB_URL']  # Required

p = xmlrpc.client.ServerProxy(PUB_URL + '/pub/xmlrpc/client')

PUSH_LABELS = ['target', 'source']

# In seconds
DURATION_BUCKETS = [
    10,
    30,
    60,  # 1 minute
    180,  # 3 minutes
    480,  # 8 minutes
    1200,  # 20 minutes
    3600,  # 1 hour
    7200,  # 2 hours
]

metrics = {}


error_states = [
    'FAILED',
    'INTERRUPTED',
    'TIMEOUT',
]
waiting_states = [
    'FREE',
    'CREATED',
    'ASSIGNED',
]
in_progress_states = [
    'OPEN',
]


class IncompletePush(Exception):
    """ Error raised when a pub push is not complete. """

    pass


def retrieve_recent_pub_pushes():
    pushes = json.loads(p.client.push_query({'date_from': START}))
    return pushes


def retrieve_open_pub_pushes():
    pushes = json.loads(p.client.push_query({'task_state': in_progress_states}))
    return pushes


def retrieve_waiting_pub_pushes():
    pushes = json.loads(p.client.push_query({'task_state': waiting_states}))
    return pushes


def pub_pushes_total(pushes):
    counts = {}
    for push in pushes:
        target = push['target']
        source = push['source']

        counts[target] = counts.get(target, {})
        counts[target][source] = counts[target].get(source, 0)
        counts[target][source] += 1

    for target in counts:
        for source in counts[target]:
            yield counts[target][source], [target, source]


def calculate_duration(push):
    if not push['dt_finished']:
        # Duration is undefined.
        # We could consider using `time.time()` as the duration, but that would produce durations
        # that change for incomlete pushes -- and changing durations like that is incompatible with
        # prometheus' histogram and counter model.  So - we just ignore pushes until they are
        # complete and have a final duration.
        raise IncompletePush("Push is not yet complete.  Duration is undefined.")
    return (arrow.get(push['dt_finished']) - arrow.get(push['dt_created'])).total_seconds()


def find_applicable_buckets(duration):
    buckets = DURATION_BUCKETS + ["+Inf"]
    for bucket in buckets:
        if duration < float(bucket):
            yield bucket


def pub_push_duration_seconds(pushes):
    duration_buckets = DURATION_BUCKETS + ["+Inf"]

    # Build counts of observations into histogram "buckets"
    counts = {}
    # Sum of all observed durations
    durations = {}

    for push in pushes:
        source = push['source']
        target = push['target']

        try:
            duration = calculate_duration(push)
        except IncompletePush:
            continue

        # Initialize structures
        durations[target] = durations.get(target, {})
        durations[target][source] = durations[target].get(source, 0)
        counts[target] = counts.get(target, {})
        counts[target][source] = counts[target].get(source, {})
        for bucket in duration_buckets:
            counts[target][source][bucket] = counts[target][source].get(bucket, 0)

        # Increment applicable bucket counts and duration sums
        durations[target][source] += duration
        for bucket in find_applicable_buckets(duration):
            counts[target][source][bucket] += 1

    for target in counts:
        for source in counts[target]:
            buckets = [
                (str(bucket), counts[target][source][bucket])
                for bucket in duration_buckets
            ]
            yield buckets, durations[target][source], [target, source]


def only(pushes, states):
    for push in pushes:
        state = push['state']
        if state in states:
            yield push


def scrape():
    global START
    START = datetime.datetime.utcnow().date().strftime('%Y-%m-%d %H:%M:%S')

    pushes = retrieve_recent_pub_pushes()

    pub_pushes_total_family = CounterMetricFamily(
        'pub_pushes_total', 'Count of all pub pushes', labels=PUSH_LABELS
    )
    for value, labels in pub_pushes_total(pushes):
        pub_pushes_total_family.add_metric(labels, value)

    pub_push_errors_total_family = CounterMetricFamily(
        'pub_push_errors_total', 'Count of all pub push errors', labels=PUSH_LABELS
    )
    error_pushes = only(pushes, states=error_states)
    for value, labels in pub_pushes_total(error_pushes):
        pub_push_errors_total_family.add_metric(labels, value)

    pub_in_progress_pushes_family = GaugeMetricFamily(
        'pub_in_progress_pushes',
        'Count of all in-progress pub pushes',
        labels=PUSH_LABELS,
    )
    in_progress_pushes = retrieve_open_pub_pushes()
    for value, labels in pub_pushes_total(in_progress_pushes):
        pub_in_progress_pushes_family.add_metric(labels, value)

    pub_waiting_pushes_family = GaugeMetricFamily(
        'pub_waiting_pushes',
        'Count of all waiting, unscheduled pub pushes',
        labels=PUSH_LABELS,
    )
    waiting_pushes = retrieve_waiting_pub_pushes()
    for value, labels in pub_pushes_total(waiting_pushes):
        pub_waiting_pushes_family.add_metric(labels, value)

    pub_push_duration_seconds_family = HistogramMetricFamily(
        'pub_push_duration_seconds',
        'Histogram of pub push durations',
        labels=PUSH_LABELS,
    )
    for buckets, duration_sum, labels in pub_push_duration_seconds(pushes):
        pub_push_duration_seconds_family.add_metric(labels, buckets, sum_value=duration_sum)

    # Replace this in one atomic operation to avoid race condition to the Expositor
    metrics.update(
        {
            'pub_pushes_total': pub_pushes_total_family,
            'pub_push_errors_total': pub_push_errors_total_family,
            'pub_in_progress_pushes': pub_in_progress_pushes_family,
            'pub_waiting_pushes': pub_waiting_pushes_family,
            'pub_push_duration_seconds': pub_push_duration_seconds_family,
        }
    )


class Expositor(object):
    """ Responsible for exposing metrics to prometheus """

    def collect(self):
        logging.info("Serving prometheus data")
        for key in sorted(metrics):
            yield metrics[key]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    for collector in list(REGISTRY._collector_to_names):
        REGISTRY.unregister(collector)
    REGISTRY.register(Expositor())

    # Popluate data before exposing over http
    scrape()
    start_http_server(8000)

    while True:
        time.sleep(int(os.environ.get('pub_POLL_INTERVAL', '3')))
        scrape()
