#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import traceback
from datawald_agency import Agency
from datawald_connector import DatawaldConnector
from hubspot_connector import HubspotConnector
from datetime import datetime, timedelta
from pytz import timezone


class HubspotAgency(Agency):
    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.hubspot_connector = HubspotConnector(logger, setting)
        self.datawald = DatawaldConnector(logger, **setting)
        Agency.__init__(self, logger, datawald=self.datawald)

    def tx_transaction_tgt(self, transaction):
        return transaction

    def tx_transaction_tgt_ext(self, new_transaction, transaction):
        pass

    def insert_update_transactions(self, transactions):
        for transaction in transactions:
            tx_type = transaction.get("tx_type_src_id").split("-")[0]
            try:
                if tx_type == "opportunity":
                    transaction["tgt_id"] = self.hubspot_connector.insert_update_deal(
                        transaction["data"],
                        id_property=self.setting["id_property"][tx_type],
                    )
                else:
                    raise Exception(f"{tx_type} is not supported.")
                transaction["tx_status"] = "S"
            except Exception:
                log = traceback.format_exc()
                transaction.update({"tx_status": "F", "tx_note": log, "tgt_id": "####"})
                self.logger.exception(
                    f"Failed to create transaction: {transaction['tx_type_src_id']} with error: {log}"
                )
        return transactions

    def tx_person_tgt(self, person):
        return person

    def tx_person_tgt_ext(self, new_person, person):
        pass

    def insert_update_persons(self, persons):
        for person in persons:
            tx_type = person.get("tx_type_src_id").split("-")[0]
            try:
                if tx_type == "contact":
                    person["tgt_id"] = self.hubspot_connector.insert_update_contact(
                        person["data"], id_property=self.setting["id_property"][tx_type]
                    )
                elif tx_type == "company":
                    person["tgt_id"] = self.hubspot_connector.insert_update_company(
                        person["data"], id_property=self.setting["id_property"][tx_type]
                    )
                else:
                    raise Exception(f"{tx_type} is not supported.")
                person["tx_status"] = "S"
            except Exception:
                log = traceback.format_exc()
                person.update({"tx_status": "F", "tx_note": log, "tgt_id": "####"})
                self.logger.exception(
                    f"Failed to create person: {person['tx_type_src_id']} with error: {log}"
                )
        return persons

    def tx_asset_tgt(self, asset):
        return asset

    def tx_asset_tgt_ext(self, new_asset, asset):
        pass

    def insert_update_assets(self, assets):
        for asset in assets:
            tx_type = asset.get("tx_type_src_id").split("-")[0]
            try:
                if tx_type == "product":
                    asset["tgt_id"] = self.hubspot_connector.insert_update_product(
                        asset["data"], id_property=self.setting["id_property"][tx_type]
                    )
                else:
                    raise Exception(f"{tx_type} is not supported.")
                asset["tx_status"] = "S"
            except Exception:
                log = traceback.format_exc()
                asset.update({"tx_status": "F", "tx_note": log, "tgt_id": "####"})
                self.logger.exception(
                    f"Failed to create asset: {asset['tx_type_src_id']} with error: {log}"
                )
        return assets
