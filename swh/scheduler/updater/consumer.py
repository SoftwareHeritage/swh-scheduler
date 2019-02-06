# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from abc import ABCMeta, abstractmethod


class UpdaterConsumer(metaclass=ABCMeta):
    """Event consumer

    """
    def __init__(self, backend, batch_cache_write=1000):
        super().__init__()
        self._reset_cache()
        self.backend = backend
        self.batch = int(batch_cache_write)
        logging.basicConfig(level=logging.DEBUG)
        self.log = logging.getLogger('%s.%s' % (
            self.__class__.__module__, self.__class__.__name__))

    def _reset_cache(self):
        """Reset internal cache.

        """
        self.count = 0
        self.seen_events = set()
        self.events = []

    def is_interesting(self, event):
        """Determine if an event is interesting or not.

        Args:
            event (SWHEvent): SWH event

        """
        return event.is_interesting()

    @abstractmethod
    def convert_event(self, event):
        """Parse an event into an SWHEvent.

        """
        pass

    def process_event(self, event):
        """Process converted and interesting event.

        Args:
            event (SWHEvent): Event to process if deemed interesting

        """
        try:
            if event.url in self.seen_events:
                event.cnt += 1
            else:
                self.events.append(event)
                self.seen_events.add(event.url)
                self.count += 1
        finally:
            if self.count >= self.batch:
                if self.events:
                    self.backend.cache_put(self.events)
                self._reset_cache()

    def _flush(self):
        """Flush remaining internal cache if any.

        """
        if self.events:
            self.backend.cache_put(self.events)
            self._reset_cache()

    @abstractmethod
    def has_events(self):
        """Determine if there remains events to consume.

        Returns
            boolean value, true for remaining events, False otherwise

        """
        pass

    @abstractmethod
    def consume_events(self):
        """The main entry point to consume events.

        This should either yield or return message for consumption.

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
        try:
            self.open_connection()
            while self.has_events():
                for _event in self.consume_events():
                    event = self.convert_event(_event)
                    if not event:
                        self.log.warning(
                            'Incomplete event dropped %s' % _event)
                        continue
                    if not self.is_interesting(event):
                        continue
                    if self.debug:
                        self.log.debug('Event: %s' % event)
                    try:
                        self.process_event(event)
                    except Exception:
                        self.log.exception(
                            'Problem when processing event %s' % _event)
                        continue
        except Exception as e:
            self.log.error('Error raised during consumption: %s' % e)
            raise e
        finally:
            self.close_connection()
            self._flush()
