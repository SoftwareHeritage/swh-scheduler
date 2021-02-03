# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import csv
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import textwrap
from typing import Dict, List, Optional, TextIO, Tuple
import uuid

import plotille
from simpy import Environment as _Environment
from simpy import Store

from swh.model.model import OriginVisitStatus
from swh.scheduler.interface import SchedulerInterface
from swh.scheduler.model import SchedulerMetrics


@dataclass
class SimulationReport:
    DURATION_THRESHOLD = 3600
    """Max duration for histograms"""

    total_visits: int = 0
    """Total count of finished visits"""

    visit_runtimes: Dict[Tuple[str, bool], List[float]] = field(default_factory=dict)
    """Collected visit runtimes for each (status, eventful) tuple"""

    scheduler_metrics: List[Tuple[datetime, List[SchedulerMetrics]]] = field(
        default_factory=list
    )
    """Collected scheduler metrics

    This is a list of couples (timestamp, [SchedulerMetrics,]): the list of
    scheduler metrics collected at given timestamp.
    """

    visit_metrics: List[Tuple[datetime, int]] = field(default_factory=list)
    """Collected visit metrics over time"""

    latest_snapshots: Dict[Tuple[str, str], bytes] = field(default_factory=dict)
    """Collected latest snapshots for origins"""

    def record_visit(
        self,
        origin: Tuple[str, str],
        duration: float,
        status: str,
        snapshot=Optional[bytes],
    ) -> None:
        eventful = False
        if status == "full":
            eventful = snapshot != self.latest_snapshots.get(origin)
            self.latest_snapshots[origin] = snapshot

        self.total_visits += 1
        self.visit_runtimes.setdefault((status, eventful), []).append(duration)

    def record_metrics(
        self, timestamp: datetime, scheduler_metrics: List[SchedulerMetrics]
    ):
        self.scheduler_metrics.append((timestamp, scheduler_metrics))
        self.visit_metrics.append((timestamp, self.total_visits))

    @property
    def uneventful_visits(self):
        """Number of uneventful, full visits"""
        return len(self.visit_runtimes.get(("full", False), []))

    def runtime_histogram(self, status: str, eventful: bool) -> str:
        runtimes = self.visit_runtimes.get((status, eventful), [])
        return plotille.hist(
            [runtime for runtime in runtimes if runtime <= self.DURATION_THRESHOLD]
        )

    def metrics_plot(self) -> str:
        timestamps, metric_lists = zip(*self.scheduler_metrics)
        known = [sum(m.origins_known for m in metrics) for metrics in metric_lists]
        never_visited = [
            sum(m.origins_never_visited for m in metrics) for metrics in metric_lists
        ]

        figure = plotille.Figure()
        figure.x_label = "simulated time"
        figure.y_label = "origins"
        figure.scatter(timestamps, known, label="Known origins")
        figure.scatter(timestamps, never_visited, label="Origins never visited")

        visit_timestamps, n_visits = zip(*self.visit_metrics)
        figure.scatter(visit_timestamps, n_visits, label="Visits over time")

        return figure.show(legend=True)

    def metrics_csv(self, fobj: TextIO) -> None:
        """Export scheduling metrics in a csv file"""
        csv_writer = csv.writer(fobj)
        csv_writer.writerow(
            [
                "timestamp",
                "known_origins",
                "enabled_origins",
                "never_visited_origins",
                "origins_with_pending_changes",
            ]
        )

        timestamps, metric_lists = zip(*self.scheduler_metrics)
        known = (sum(m.origins_known for m in metrics) for metrics in metric_lists)
        enabled = (sum(m.origins_enabled for m in metrics) for metrics in metric_lists)
        never_visited = (
            sum(m.origins_never_visited for m in metrics) for metrics in metric_lists
        )
        pending_changes = (
            sum(m.origins_with_pending_changes for m in metrics)
            for metrics in metric_lists
        )
        csv_writer.writerows(
            zip(timestamps, known, enabled, never_visited, pending_changes)
        )

    def format(self, with_plots=True):
        full_visits = self.visit_runtimes.get(("full", True), [])
        long_tasks = sum(runtime > self.DURATION_THRESHOLD for runtime in full_visits)

        output = textwrap.dedent(
            f"""\
                Total visits: {self.total_visits}
                Uneventful visits: {self.uneventful_visits}
                Eventful visits: {len(full_visits)}
                Very long running tasks: {long_tasks}
                """
        )
        if with_plots:
            histogram = self.runtime_histogram("full", True)
            plot = self.metrics_plot()
            output += (
                "Visit time histogram for eventful visits:"
                + histogram
                + "\n"
                + textwrap.dedent(
                    """\
                    Metrics over time:
                    """
                )
                + plot
            )
        return output


@dataclass
class Task:
    visit_type: str
    origin: str
    backend_id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class TaskEvent:
    task: Task
    status: OriginVisitStatus
    eventful: bool = field(default=False)


class Queue(Store):
    """Model a queue of objects to be passed between processes."""

    def __len__(self):
        return len(self.items or [])

    def slots_remaining(self):
        return self.capacity - len(self)


class Environment(_Environment):
    report: SimulationReport
    scheduler: SchedulerInterface

    def __init__(self, start_time: datetime):
        if start_time.tzinfo is None:
            raise ValueError("start_time must have timezone information")
        self.start_time = start_time
        super().__init__()

    @property
    def time(self):
        """Get the current simulated wall clock time"""
        return self.start_time + timedelta(seconds=self.now)
