#!/usr/bin/env python3
""" A simple prometheus exporter for pulp.

Scrapes pulp on an interval and exposes metrics about tasks.

It would be better for pulp to offer a prometheus /metrics endpoint of its own.
"""

from datetime import datetime

import logging
import os
import time

import arrow
import requests

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from prometheus_client.core import (
    REGISTRY,
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
)
from prometheus_client import start_http_server

retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["POST", "HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

START = None

PULP_URL = os.environ['PULP_URL']  # Required
PULP_USERNAME = os.environ['PULP_USERNAME']  # Required
PULP_PASSWORD = os.environ['PULP_PASSWORD']  # Required
PULP_CERT = os.environ.get('PULP_CERT')  # Required
PULP_KEY = os.environ.get('PULP_KEY')  # Required
AUTH = (PULP_USERNAME, PULP_PASSWORD)

# Required to access pulp with a service account certificate
session.cert = (PULP_CERT, PULP_KEY)

TASK_LABELS = ['task_type']

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
    'error',
]
waiting_states = [
    'waiting',
]
in_progress_states = [
    'running',
]


class IncompleteTask(Exception):
    """ Error raised when a pulp task is not complete. """
    pass


def retrieve_recent_pulp_tasks():
    url = PULP_URL + '/pulp/api/v2/tasks/search/'
    query = {
        'criteria': {
            'fields': ['start_time', 'finish_time', 'state', 'task_type'],
            'filters': {
                'start_time': {
                    "$gte": START,
                },
            },
        },
    }
    response = session.post(url, json=query, auth=AUTH, timeout=30)
    response.raise_for_status()
    tasks = response.json()
    for task in tasks:
        # Replace None with the string 'undefined'
        task['task_type'] = task['task_type'] or 'undefined'
    return tasks


def retrieve_open_pulp_tasks():
    url = PULP_URL + '/pulp/api/v2/tasks/search/'
    query = {
        'criteria': {
            'fields': ['start_time', 'finish_time', 'state', 'task_type'],
            'filters': {
                'state': {
                    '$in': in_progress_states,
                },
            },
        },
    }
    response = session.post(url, json=query, auth=AUTH, timeout=30)
    response.raise_for_status()
    tasks = response.json()
    for task in tasks:
        # Replace None with the string 'undefined'
        task['task_type'] = task['task_type'] or 'undefined'
    return tasks


def retrieve_waiting_pulp_tasks():
    url = PULP_URL + '/pulp/api/v2/tasks/search/'
    query = {
        'criteria': {
            'fields': ['start_time', 'finish_time', 'state', 'task_type'],
            'filters': {
                'state': {
                    '$in': waiting_states,
                },
            },
        },
    }
    response = session.post(url, json=query, auth=AUTH, timeout=30)
    response.raise_for_status()
    tasks = response.json()
    for task in tasks:
        # Replace None with the string 'undefined'
        task['task_type'] = task['task_type'] or 'undefined'
    return tasks


def pulp_tasks_total(tasks):
    counts = {}
    for task in tasks:
        task_type = task['task_type']

        counts[task_type] = counts.get(task_type, 0)
        counts[task_type] += 1

    for task_type in counts:
        yield counts[task_type], [task_type]


def calculate_duration(task):
    if not task['finish_time']:
        # Duration is undefined.
        # We could consider using `time.time()` as the duration, but that would produce durations
        # that change for incomlete tasks -- and changing durations like that is incompatible with
        # prometheus' histogram and counter model.  So - we just ignore tasks until they are
        # complete and have a final duration.
        raise IncompleteTask("Task is not yet complete.  Duration is undefined.")
    return (arrow.get(task['finish_time']) - arrow.get(task['start_time'])).total_seconds()


def find_applicable_buckets(duration):
    buckets = DURATION_BUCKETS + ["+Inf"]
    for bucket in buckets:
        if duration < float(bucket):
            yield bucket


def pulp_task_duration_seconds(tasks):
    duration_buckets = DURATION_BUCKETS + ["+Inf"]

    # Build counts of observations into histogram "buckets"
    counts = {}
    # Sum of all observed durations
    durations = {}

    for task in tasks:
        task_type = task['task_type']

        try:
            duration = calculate_duration(task)
        except IncompleteTask:
            continue

        # Initialize structures
        durations[task_type] = durations.get(task_type, 0)
        counts[task_type] = counts.get(task_type, {})
        for bucket in duration_buckets:
            counts[task_type][bucket] = counts[task_type].get(bucket, 0)

        # Increment applicable bucket counts and duration sums
        durations[task_type] += duration
        for bucket in find_applicable_buckets(duration):
            counts[task_type][bucket] += 1

    for task_type in counts:
        buckets = [
            (str(bucket), counts[task_type][bucket])
            for bucket in duration_buckets
        ]
        yield buckets, durations[task_type], [task_type]


def only(tasks, states):
    for task in tasks:
        state = task['state']
        if state in states:
            yield task


def scrape():
    global START
    today = datetime.utcnow()
    START = datetime.combine(today, datetime.min.time()).isoformat()

    tasks = retrieve_recent_pulp_tasks()

    pulp_tasks_total_family = CounterMetricFamily(
        'pulp_tasks_total', 'Count of all pulp tasks', labels=TASK_LABELS
    )
    for value, labels in pulp_tasks_total(tasks):
        pulp_tasks_total_family.add_metric(labels, value)

    pulp_task_errors_total_family = CounterMetricFamily(
        'pulp_task_errors_total', 'Count of all pulp task errors', labels=TASK_LABELS
    )
    error_tasks = only(tasks, states=error_states)
    for value, labels in pulp_tasks_total(error_tasks):
        pulp_task_errors_total_family.add_metric(labels, value)

    pulp_in_progress_tasks_family = GaugeMetricFamily(
        'pulp_in_progress_tasks',
        'Count of all in-progress pulp tasks',
        labels=TASK_LABELS,
    )
    in_progress_tasks = retrieve_open_pulp_tasks()
    for value, labels in pulp_tasks_total(in_progress_tasks):
        pulp_in_progress_tasks_family.add_metric(labels, value)

    pulp_waiting_tasks_family = GaugeMetricFamily(
        'pulp_waiting_tasks',
        'Count of all waiting, unscheduled pulp tasks',
        labels=TASK_LABELS,
    )
    waiting_tasks = retrieve_waiting_pulp_tasks()
    for value, labels in pulp_tasks_total(waiting_tasks):
        pulp_waiting_tasks_family.add_metric(labels, value)

    pulp_task_duration_seconds_family = HistogramMetricFamily(
        'pulp_task_duration_seconds',
        'Histogram of pulp task durations',
        labels=TASK_LABELS,
    )
    for buckets, duration_sum, labels in pulp_task_duration_seconds(tasks):
        pulp_task_duration_seconds_family.add_metric(labels, buckets, sum_value=duration_sum)

    # Replace this in one atomic operation to avoid race condition to the Expositor
    metrics.update(
        {
            'pulp_tasks_total': pulp_tasks_total_family,
            'pulp_task_errors_total': pulp_task_errors_total_family,
            'pulp_in_progress_tasks': pulp_in_progress_tasks_family,
            'pulp_waiting_tasks': pulp_waiting_tasks_family,
            'pulp_task_duration_seconds': pulp_task_duration_seconds_family,
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
        time.sleep(int(os.environ.get('pulp_POLL_INTERVAL', '3')))
        scrape()
