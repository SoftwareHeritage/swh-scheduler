# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import textwrap
from typing import Dict, List, Tuple
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

    metrics: List[Tuple[datetime, List[SchedulerMetrics]]] = field(default_factory=list)
    """Collected scheduler metrics for every timestamp"""

    def record_visit(self, duration: float, eventful: bool, status: str) -> None:
        self.total_visits += 1
        self.visit_runtimes.setdefault((status, eventful), []).append(duration)

    def record_metrics(self, timestamp: datetime, metrics: List[SchedulerMetrics]):
        self.metrics.append((timestamp, metrics))

    @property
    def useless_visits(self):
        """Number of uneventful, full visits"""
        return len(self.visit_runtimes.get(("full", False), []))

    def runtime_histogram(self, status: str, eventful: bool) -> str:
        runtimes = self.visit_runtimes.get((status, eventful), [])
        return plotille.hist(
            [runtime for runtime in runtimes if runtime <= self.DURATION_THRESHOLD]
        )

    def metrics_plot(self) -> str:
        timestamps, metric_lists = zip(*self.metrics)
        known = [sum(m.origins_known for m in metrics) for metrics in metric_lists]
        never_visited = [
            sum(m.origins_never_visited for m in metrics) for metrics in metric_lists
        ]

        figure = plotille.Figure()
        figure.x_label = "simulated time"
        figure.y_label = "origins"
        figure.scatter(timestamps, known, label="Known origins")
        figure.scatter(timestamps, never_visited, label="Origins never visited")

        return figure.show(legend=True)

    def format(self):
        full_visits = self.visit_runtimes.get(("full", True), [])
        histogram = self.runtime_histogram("full", True)
        plot = self.metrics_plot()
        long_tasks = sum(runtime > self.DURATION_THRESHOLD for runtime in full_visits)

        return (
            textwrap.dedent(
                f"""\
                Total visits: {self.total_visits}
                Useless visits: {self.useless_visits}
                Eventful visits: {len(full_visits)}
                Very long running tasks: {long_tasks}
                Visit time histogram for eventful visits:
                """
            )
            + histogram
            + "\n"
            + textwrap.dedent(
                """\
                Metrics over time:
                """
            )
            + plot
        )


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
