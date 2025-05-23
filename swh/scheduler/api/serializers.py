# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Decoder and encoders for swh.scheduler.model objects."""

from typing import Callable, Dict, List, Tuple

import swh.scheduler.model as model


def _encode_model_object(obj):
    d = obj.to_dict(recurse=False)
    d["__type__"] = type(obj).__name__
    return d


def _encode_enum(obj):
    return obj.value


ENCODERS: List[Tuple[type, str, Callable]] = [
    (model.BaseSchedulerModel, "scheduler_model", _encode_model_object),
    (model.LastVisitStatus, "last_visit_status", _encode_enum),
]


DECODERS: Dict[str, Callable] = {
    "scheduler_model": lambda d: getattr(model, d.pop("__type__"))(**d),
    "last_visit_status": model.LastVisitStatus,
}
