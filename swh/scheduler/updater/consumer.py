# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from abc import ABCMeta, abstractmethod

from swh.scheduler.updater.backend import SchedulerUpdaterBackend


class UpdaterConsumer(metaclass=ABCMeta):
    """Event consumer

    """
    def __init__(self, batch=1000):
        super().__init__()
        self._reset_cache()
        self.backend = SchedulerUpdaterBackend()
        self.batch = batch

    def _reset_cache(self):
        self.count = 0
        self.seen_events = set()
        self.events = []

    def is_interesting(self, event):
        """Determine if an event is interesting or not.

        Args
            event (SWHEvent): SWH event

        """
        return event.is_interesting()

    @abstractmethod
    def convert_event(self, event):
        """Parse an event into an SWHEvent.

        """
        pass

    @abstractmethod
    def post_process_message(self, message):
        pass

    def process_message(self, body, message):
        try:
            event = self.convert_event(body)
            if self.debug:
                print('#### body', body)
            if self.is_interesting(event):
                if event.url in self.seen_events:
                    event.rate += 1
                else:
                    self.events.append(event)
                    self.seen_events.add(event.url)
                    self.count += 1
        finally:
            self.post_process_message(message)
            if self.count >= self.batch:
                if self.events:
                    self.backend.cache_put(self.events)
                self._reset_cache()

    def flush(self):
        if self.events:
            self.backend.cache_put(self.events)
            self._reset_cache()

    @abstractmethod
    def has_events(self):
        """Determine if there remains events to consume.

        """
        pass

    @abstractmethod
    def consume(self):
        """The main entry point to consume event.

        This should be defined per consumer and call the
        self.process_message function.

        """
        pass

    @abstractmethod
    def open_connection(self):
        """Open a connection to the remote system we are supposed to consume
           from.

        """
        pass

    @abstractmethod
    def close_connection(self):
        """Close opened connection to the remote system.

        """
        pass

    def run(self):
        """The main entry point to consume events.

        """
        self.open_connection()
        while self.has_events():
            self.consume()
        self.close_connection()
        self.flush()
