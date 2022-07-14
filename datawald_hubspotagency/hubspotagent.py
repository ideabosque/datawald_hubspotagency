#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

from .hubspotagency import HubspotAgency


class HubspotAgent(HubspotAgency):
    def __init__(self, logger, **setting):
        HubspotAgency.__init__(self, logger, **setting)
