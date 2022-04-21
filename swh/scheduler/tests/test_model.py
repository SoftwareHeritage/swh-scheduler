# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

import attr

from swh.scheduler import model


def test_select_columns():
    @attr.s
    class TestModel(model.BaseSchedulerModel):
        id = attr.ib(type=str)
        test1 = attr.ib(type=str)
        a_first_attr = attr.ib(type=str)

        @property
        def test2(self):
            """This property should not show up in the extracted columns"""
            return self.test1

    assert TestModel.select_columns() == ("a_first_attr", "id", "test1")


def test_insert_columns():
    @attr.s
    class TestModel(model.BaseSchedulerModel):
        id = attr.ib(type=str)
        test1 = attr.ib(type=str)

        @property
        def test2(self):
            """This property should not show up in the extracted columns"""
            return self.test1

    assert TestModel.insert_columns_and_metavars() == (
        ("id", "test1"),
        ("%(id)s", "%(test1)s"),
    )


def test_insert_columns_auto_now_add():
    @attr.s
    class TestModel(model.BaseSchedulerModel):
        id = attr.ib(type=str)
        test1 = attr.ib(type=str)
        added = attr.ib(type=datetime.datetime, metadata={"auto_now_add": True})

    assert TestModel.insert_columns_and_metavars() == (
        ("id", "test1"),
        ("%(id)s", "%(test1)s"),
    )


def test_insert_columns_auto_now():
    @attr.s
    class TestModel(model.BaseSchedulerModel):
        id = attr.ib(type=str)
        test1 = attr.ib(type=str)
        updated = attr.ib(type=datetime.datetime, metadata={"auto_now": True})

    assert TestModel.insert_columns_and_metavars() == (
        ("id", "test1", "updated"),
        ("%(id)s", "%(test1)s", "now()"),
    )


def test_insert_columns_primary_key():
    @attr.s
    class TestModel(model.BaseSchedulerModel):
        id = attr.ib(type=str, metadata={"auto_primary_key": True})
        test1 = attr.ib(type=str)

    assert TestModel.insert_columns_and_metavars() == (("test1",), ("%(test1)s",))


def test_insert_primary_key():
    @attr.s
    class TestModel(model.BaseSchedulerModel):
        id = attr.ib(type=str, metadata={"auto_primary_key": True})
        test1 = attr.ib(type=str)

    assert TestModel.primary_key_columns() == ("id",)

    @attr.s
    class TestModel2(model.BaseSchedulerModel):
        col1 = attr.ib(type=str, metadata={"primary_key": True})
        col2 = attr.ib(type=str, metadata={"primary_key": True})
        test1 = attr.ib(type=str)

    assert TestModel2.primary_key_columns() == ("col1", "col2")
