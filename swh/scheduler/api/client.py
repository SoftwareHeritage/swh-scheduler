# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.core.api import RPCClient

from .. import exc
from ..interface import SchedulerInterface
from .serializers import DECODERS, ENCODERS


class RemoteScheduler(RPCClient):
    """Proxy to a remote scheduler API

    """

    backend_class = SchedulerInterface

    reraise_exceptions = [getattr(exc, a) for a in exc.__all__]

    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS
