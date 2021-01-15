# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta
import textwrap
from typing import Dict, List, Tuple

import attr
import plotille
from simpy import Environment as _Environment
from simpy import Store

from swh.scheduler.interface import SchedulerInterface


@attr.s
class SimulationReport:
    DURATION_THRESHOLD = 3600
    """Max duration for histograms"""

    total_visits = attr.ib(type=int, default=0)
    """Total count of finished visits"""

    visit_runtimes = attr.ib(type=Dict[Tuple[str, bool], List[float]], factory=dict)
    """Collect the visit runtimes for each (status, eventful) tuple"""

    def record_visit(self, duration: float, eventful: bool, status: str) -> None:
        self.total_visits += 1
        self.visit_runtimes.setdefault((status, eventful), []).append(duration)

    @property
    def useless_visits(self):
        """Number of uneventful, full visits"""
        return len(self.visit_runtimes.get(("full", False), []))

    def runtime_histogram(self, status: str, eventful: bool) -> str:
        runtimes = self.visit_runtimes.get((status, eventful), [])
        return plotille.hist(
            [runtime for runtime in runtimes if runtime <= self.DURATION_THRESHOLD]
        )

    def format(self):
        full_visits = self.visit_runtimes.get(("full", True), [])
        histogram = self.runtime_histogram("full", True)
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
        )


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
