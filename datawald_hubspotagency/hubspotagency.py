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
    all_owners = {}

    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.hubspot_connector = HubspotConnector(logger, setting)
        self.datawald = DatawaldConnector(logger, **setting)
        Agency.__init__(self, logger, datawald=self.datawald)
        if setting.get("tx_type"):
            Agency.tx_type = setting.get("tx_type")

    def tx_transaction_tgt(self, transaction):
        if transaction["data"].get("owner_name", None):
            owner_name = transaction["data"].pop("owner_name", None)
            owner = self.get_owner_by_name(owner_name)
            if owner is not None:
                transaction["data"]["hubspot_owner_id"] = owner.id
        return transaction
    
    def tx_transaction_tgt_ext(self, new_transaction, transaction):
        pass

    def insert_update_transactions(self, transactions):
        for transaction in transactions:
            tx_type = transaction.get("tx_type_src_id").split("-")[0]
            try:
                if tx_type== "opportunity":
                    transaction["tgt_id"] = self.insert_update_opportunity(transaction)
                elif tx_type == "order":
                    transaction["tgt_id"] = self.insert_update_order(transaction)
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
    
    def insert_update_order(self, transaction):
        tx_type = transaction.get("tx_type_src_id").split("-")[0]
        items = transaction["data"].pop("items", [])
        deal_number = transaction["data"].get("deal_number")
        order_status = transaction["data"].pop("status", "")
        if order_status != "Billed":
            raise Exception(f"{deal_number}'s status is not Billed, can not be synced to hubspot.")
        if len(items) == 0:
            raise Exception(f"{deal_number} does not have items")

        hs_products = []
        for item in items:
            try:
                hs_product = self.hubspot_connector.get_product(item.get("sku", None), self.setting["id_property"]["product"])
                if hs_product is not None:
                    hs_products.append({
                        "hs_product": hs_product,
                        "quantity": item.get("qty_ordered"),
                        "price": item.get("price")
                    })
            except Exception as e:
                self.logger.info(f"can't find product: field/{self.setting['id_property']['product']} value/{item['sku']}")
                pass
        if len(hs_products) == 0:
            raise Exception(f"{deal_number} does not have avaliable items")

        company_id = transaction["data"].pop("company_id", None)
        deal_id = self.hubspot_connector.insert_update_deal(
            transaction["data"],
            id_property=self.setting["id_property"][tx_type],
        )
        if deal_id is None:
            raise Exception(f"Fail to create deal. deal_number:{deal_number}")

        if company_id is not None and deal_id:
            try:
                company = self.hubspot_connector.get_company(company_id, self.setting["id_property"]["company"])
                company_association = self.hubspot_connector.get_deal_association(deal_id=deal_id, to_object_type="company")
                if len(company_association.results) == 0:
                    self.hubspot_connector.associate_deal_company(deal_id=deal_id, company_id=company.id)
                    
            except Exception as e:
                self.logger.info(str(e))
                pass
        if transaction["data"].get("associated_email_contact"):
            try:
                contact = self.hubspot_connector.get_contact(transaction["data"].get("associated_email_contact"), self.setting["id_property"]["contact"])
                contact_association = self.hubspot_connector.get_deal_association(deal_id=deal_id, to_object_type="contact")
                if len(contact_association.results) == 0:
                    self.hubspot_connector.associate_deal_contact(deal_id=deal_id, contact_id=contact.id)
            except Exception as e:
                self.logger.info(str(e))
                pass
            
        line_items_association = self.hubspot_connector.get_deal_association(deal_id=deal_id, to_object_type="line_items")
        if len(line_items_association.results) == 0 and deal_id:
            for item in hs_products:
                line_item_id = self.hubspot_connector.insert_update_line_item(hs_product=item["hs_product"], quantity=item["quantity"], price=item["price"], associations=["deals"])
                self.hubspot_connector.associate_line_item_deal(line_item_id, deal_id)
        return deal_id

    def insert_update_opportunity(self, transaction):
        tx_type = transaction.get("tx_type_src_id").split("-")[0]
        items = transaction["data"].pop("items", [])
        document_number = transaction["data"].get("document_number")
        hs_products = []
        for item in items:
            try:
                hs_product = self.hubspot_connector.get_product(item.get("sku", None), self.setting["id_property"]["product"])
                if hs_product is not None:
                    hs_products.append({
                        "hs_product": hs_product,
                        "quantity": item.get("qty_ordered"),
                        "price": item.get("price")
                    })
            except Exception as e:
                self.logger.info(f"can't find product: field/{self.setting['id_property']['product']} value/{item['sku']}")
                pass

        deal_id = self.hubspot_connector.insert_update_deal(
            transaction["data"],
            id_property=self.setting["id_property"][tx_type],
        )
        if deal_id is None:
             raise Exception(f"Fail to create deal. document_number:{document_number}")
        
        line_items_association = self.hubspot_connector.get_deal_association(deal_id=deal_id, to_object_type="line_items")
        if len(line_items_association.results) == 0 and deal_id:
            for item in hs_products:
                line_item_id = self.hubspot_connector.insert_update_line_item(hs_product=item["hs_product"], quantity=item["quantity"], price=item["price"], associations=["deals"])
                self.hubspot_connector.associate_line_item_deal(line_item_id, deal_id)
        return deal_id
    
    def get_owner_by_name(self, sales_rep):
        if isinstance(sales_rep, str):
            owners_name_mapping = self.get_owners_name_mapping()
            return owners_name_mapping.get(sales_rep.lower(), None)
        return None
            
    def get_owners_name_mapping(self):
        if len(self.all_owners) == 0:
            owners = self.hubspot_connector.get_all_owners()
            for owner in owners:
                owner_name = "{first_name} {last_name}".format(first_name=owner.first_name, last_name=owner.last_name)
                self.all_owners[owner_name.lower()] = owner
        return self.all_owners

