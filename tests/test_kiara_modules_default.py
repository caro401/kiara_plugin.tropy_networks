#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `kiara_plugin.tropy` package."""

import pytest  # noqa

import kiara_plugin.tropy


def test_assert():

    assert kiara_plugin.tropy.get_version() is not None
