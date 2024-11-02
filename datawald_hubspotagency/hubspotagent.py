#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

from .hubspotagency import HubspotAgency


def deploy() -> list:
    return [
        {
            "service": "DataWald",
            "class": "HubspotAgent",
            "functions": {
                "stream_handle": {
                    "is_static": False,
                    "label": "hubspotagency",
                    "mutation": [],
                    "query": [],
                    "type": "Event",
                    "support_methods": [],
                    "is_auth_required": False,
                    "is_graphql": False,
                    "settings": "datawald_agency",
                    "disabled_in_resources": True,  # Ignore adding to resource list.
                },
                "insert_update_entities_to_target": {
                    "is_static": False,
                    "label": "hubspotagency",
                    "mutation": [],
                    "query": [],
                    "type": "Event",
                    "support_methods": [],
                    "is_auth_required": False,
                    "is_graphql": False,
                    "settings": "datawald_agency",
                    "disabled_in_resources": True,  # Ignore adding to resource list.
                },
                "update_sync_task": {
                    "is_static": False,
                    "label": "hubspotagency",
                    "mutation": [],
                    "query": [],
                    "type": "Event",
                    "support_methods": [],
                    "is_auth_required": False,
                    "is_graphql": False,
                    "settings": "datawald_agency",
                    "disabled_in_resources": True,  # Ignore adding to resource list.
                },
            },
        }
    ]


class HubspotAgent(HubspotAgency):
    def __init__(self, logger, **setting):
        HubspotAgency.__init__(self, logger, **setting)
