# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.scheduler.updater.events import SWHEvent, LISTENED_EVENTS
from hypothesis import given
from hypothesis.strategies import one_of, text, just, sampled_from


def event_values_ko():
    return {
        'repos', 'org_members', 'teamadd', 'geo_cache',
        'follow', 'issue_comments', 'followers', 'forks', 'pagebuild',
        'pullrequest', 'pull_requests', 'commit_comments', 'watch', 'fork',
        'forkapply', 'commits', 'release', 'gollum', 'membership', 'watchers',
        'pullrequestreviewcomment', 'deployment', 'issuecomment', 'status',
        'repo_labels', 'issue_events', 'commitcomment', 'issues', 'member',
        'users', 'download', 'repo_collaborators', 'repository',
        'deploymentstatus', 'pull_request_comments', 'gist'
    }


class EventTest(unittest.TestCase):
    def _make_event(self, event_name):
        return {
            'evt': event_name,
            'url': 'something'
        }

    @istest
    @given(sampled_from(LISTENED_EVENTS))
    def check_ok(self, event_name):
        evt = self._make_event(event_name)
        self.assertTrue(SWHEvent(evt).check())

    @istest
    @given(text())
    def check_with_noisy_event_should_be_ko(self, event_name):
        if event_name in LISTENED_EVENTS:
            # just in generation generates a real and correct name, skip it
            return
        evt = self._make_event(event_name)
        self.assertFalse(SWHEvent(evt).check())

    @istest
    @given(one_of(map(just, event_values_ko())))
    def check_ko(self, event_name):
        evt = self._make_event(event_name)
        self.assertFalse(SWHEvent(evt).check())
