#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import traceback, pendulum, time
from datawald_agency import Agency
from datawald_connector import DatawaldConnector
from hubspot_connector import HubspotConnector
from datetime import datetime, timedelta
from pytz import timezone
from decimal import Decimal

class IgnoreException(Exception):
    pass

class HubspotAgency(Agency):
    all_owners = {}
    hubspot_users = {}
    hubspot_team_options = None

    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.hubspot_connector = HubspotConnector(logger, setting)
        self.datawald = DatawaldConnector(logger, **setting)
        Agency.__init__(self, logger, datawald=self.datawald)
        if setting.get("tx_type"):
            Agency.tx_type = setting.get("tx_type")

        self.map = setting.get("TXMAP", {})

    def get_records(self, funct, **params):
        try:
            current = datetime.now(tz=timezone(self.setting.get("TIMEZONE", "UTC")))
            hours = params.get("hours", 0.0)
            while True:
                self.logger.info(params)
                records = funct(**params)
                end = datetime.strptime(
                    params.get("cut_date"), "%Y-%m-%dT%H:%M:%S%z"
                ) + timedelta(hours=params["hours"])
                if hours == 0.0:
                    return records
                elif len(records) >= 1 or end >= current:
                    return records
                else:
                    params["hours"] = params["hours"] + hours
                    params.update(
                        {
                            "end_date": end.strftime("%Y-%m-%dT%H:%M:%S%z"),
                        }
                    )
                    time.sleep(5)
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def get_sales_offfline_opportunities(self, **params):
        deal_params = {}
        deal_params["filter_groups"] = [
            {
                "filters": [
                    {
                        "value": self.setting.get("sales_offline_opportunity_pipeline"),
                        "propertyName": "pipeline",
                        "operator": "EQ"
                    },
                    {
                        "value": self.setting.get("sales_offline_opportunity_dealstage"),
                        "propertyName": "dealstage",
                        "operator": "EQ"
                    },
                    {
                        "dateTimeFormat": "EPOCH_MILLISECONDS",
                        "value": int(datetime.strptime(params.get("cut_date", ""), "%Y-%m-%dT%H:%M:%S%z").timestamp() * 1000),
                        "highValue": int(datetime.strptime(params.get("end_date", ""), "%Y-%m-%dT%H:%M:%S%z").timestamp() * 1000),
                        "propertyName": "hs_lastmodifieddate",
                        "operator": "BETWEEN"
                    }
                    
                ]
            }
        ]
        deal_params["limit"]=50
        deal_params["sorts"] = ["hs_lastmodifieddate"]
        # deal_params["properties"] = ["pipeline","class", "customer_po", "delivery_type", "fob_remarks", "freight_terms", "hold_reason", "location", "order_type", "ship_date", "shipping_carrier", "shipping_instructions", "shipping_method", "status", "terms"]
        deal_params["properties"] = self.setting.get("deal_properties", None)
        return self.hubspot_connector.get_deals(**deal_params)

    def tx_transactions_src(self, **kwargs):
        try:
            params = dict(
                kwargs,
                **{
                    "cut_date": kwargs.get("cut_date")
                    .astimezone(timezone(self.setting.get("TIMEZONE", "UTC")))
                    .strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "end_date": datetime.now(
                        tz=timezone(self.setting.get("TIMEZONE", "UTC"))
                    ).strftime("%Y-%m-%dT%H:%M:%S%z"),
                },
            )

            if float(kwargs.get("hours", 0)) > 0:
                params.update(
                    {
                        "end_date": (
                            kwargs.get("cut_date")
                            + timedelta(hours=float(kwargs.get("hours")))
                        ).strftime("%Y-%m-%dT%H:%M:%S%z")
                    }
                )

            if kwargs.get("tx_type") == "order":
                raw_transactions = self.get_records(
                    self.get_sales_offfline_opportunities, **params
                )
            else:
                raise Exception(f"{kwargs.get('tx_type')} is not supported.")
            
            transactions = list(
                map(
                    lambda raw_transaction: self.tx_transaction_src(
                        raw_transaction, **kwargs
                    ),
                    raw_transactions,
                )
            )

            return transactions
        except Exception:
            self.logger.info(kwargs)
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def tx_transaction_src(self, raw_transaction, **kwargs):
        tx_type = kwargs.get("tx_type")
        target = kwargs.get("target")
        transaction = {
            "src_id": raw_transaction[self.setting["src_metadata"][target][tx_type]["src_id"]],
            "created_at": pendulum.parse(raw_transaction[
                self.setting["src_metadata"][target][tx_type]["created_at"]
            ]),
            "updated_at": pendulum.parse(raw_transaction[
                self.setting["src_metadata"][target][tx_type]["updated_at"]
            ])
        }
        raw_transaction = self.tx_transaction_src_ext(raw_transaction, **kwargs)
        try:
            transaction.update(
                {
                    "data": self.transform_data(
                        raw_transaction,
                        self.map[target].get(tx_type),
                    )
                }
            )
        except Exception:
            log = traceback.format_exc()
            transaction.update(
                {"tx_status": "F", "tx_note": log, "data": raw_transaction}
            )
            self.logger.exception(log)

        return transaction

    # def get_transactions_total(self, **kwargs):
    #     if kwargs.get("tx_type") == "opportunity":
    #         params = {
    #             "cut_date": kwargs.get("cut_date").strftime("%Y-%m-%d %H:%M:%S"),
    #             "count_only": True,
    #         }

    #         if float(kwargs.get("hours", 0)) > 0:
    #             params.update(
    #                 {
    #                     "end_day": kwargs.get("cut_date")
    #                     + timedelta(hours=float(kwargs.get("hours", 0)))
    #                 }
    #             )
    #         return self.get_sales_offfline_opportunities(**params)
    #     else:
    #         return 0

    def tx_transaction_src_ext(self, raw_transaction, **kwargs):
        if (raw_transaction.get("pipeline", None) is not None 
        and raw_transaction.get("hs_object_id")
        and raw_transaction.get("pipeline", None) == self.setting.get("sales_offline_opportunity_pipeline")):
            raw_transaction["items"] = []
            try:
                line_items_result = self.hubspot_connector.get_deal_association(deal_id=raw_transaction.get("hs_object_id"), to_object_type="line_items")
                if len(line_items_result.results) > 0:
                    line_items = []
                    for line_item in line_items_result.results:
                        try:
                            line_item_result = self.hubspot_connector.get_line_item(line_item_id=line_item.id, properties=["amount", "hs_sku", "quantity", "price"])
                            line_items.append(line_item_result.properties)
                        except Exception:
                            pass
                    raw_transaction["items"] = line_items
            except Exception as e:
                pass 
            
            contacts_result = self.hubspot_connector.get_deal_association(deal_id=raw_transaction.get("hs_object_id"), to_object_type="contact")
            raw_transaction["contact"] = {}
            if len(contacts_result.results) > 0:
                for contact_result in contacts_result.results:
                    try:
                        contact = self.hubspot_connector.get_contact(contact_id=contact_result.id, properties=["email","firstname", "lastname","gwi_account_no"])
                        raw_transaction["contact"] = contact.properties
                        if not contact.archived:
                            break
                    except Exception:
                        pass
            if not raw_transaction.get("customer_po"):
                raw_transaction["customer_po"] = datetime.now(tz=timezone(self.setting.get("TIMEZONE", "UTC"))).strftime("%Y%m%d%H%M")
            ship_hours = 0
            if datetime.now(tz=timezone("America/Los_Angeles")).hour >= 12:
                ship_hours = 24
            raw_transaction["ship_date"] = ship_hours
        return raw_transaction
    
    def tx_transaction_tgt(self, transaction):

        if transaction["data"].get("owner_name", None):
            owner_name = transaction["data"].pop("owner_name", None)
            owner = self.get_owner_by_name(owner_name)
            if owner is not None:
                transaction["data"]["hubspot_owner_id"] = owner.id

        if transaction["data"].get("seller_sales_rep", None):
            owner_name = transaction["data"]["seller_sales_rep"]
            owner = self.get_owner_by_name(owner_name)
            if owner is not None:
                transaction["data"]["seller_sales_rep"] = owner.id
            else:
                transaction["data"]["seller_sales_rep"] = None

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
                elif tx_type in ["sample_conversion"]:
                    transaction["tgt_id"] = self.update_deal(transaction)
                elif tx_type in ["sample_conversion_item"]:
                    transaction["tgt_id"] = self.update_deal_item(transaction)
                else:
                    raise Exception(f"{tx_type} is not supported.")
                transaction["tx_status"] = "S"
            except IgnoreException:
                log = traceback.format_exc()
                transaction.update({"tx_status": "I", "tx_note": log, "tgt_id": "####"})
                self.logger.info(log)
            except Exception:
                log = traceback.format_exc()
                transaction.update({"tx_status": "F", "tx_note": log, "tgt_id": "####"})
                self.logger.exception(
                    f"Failed to create transaction: {transaction['tx_type_src_id']} with error: {log}"
                )
        return transactions
    
    def tx_persons_src(self, **kwargs):
        try:
            params = dict(
                kwargs,
                **{
                    "cut_date": kwargs.get("cut_date")
                    .astimezone(timezone(self.setting.get("TIMEZONE", "UTC")))
                    .strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "end_date": datetime.now(
                        tz=timezone(self.setting.get("TIMEZONE", "UTC"))
                    ).strftime("%Y-%m-%dT%H:%M:%S%z"),
                },
            )
            if float(kwargs.get("hours", 0)) > 0:
                params.update(
                    {
                        "end_date": (
                            kwargs.get("cut_date")
                            + timedelta(hours=float(kwargs.get("hours")))
                        ).astimezone(timezone(self.setting.get("TIMEZONE", "UTC"))).strftime("%Y-%m-%dT%H:%M:%S%z")
                    }
                )

            if kwargs.get("tx_type") == "company":
                raw_persons = self.get_records(
                    self.get_companies, **params
                )
            elif kwargs.get("tx_type") == "contact":
                raw_persons = self.get_records(
                    self.get_contacts, **params
                )
            else:
                raise Exception(f"{kwargs.get('tx_type')} is not supported.")
            
            persons = list(
                map(
                    lambda raw_person: self.tx_person_src(
                        raw_person, **kwargs
                    ),
                    raw_persons,
                )
            )

            return persons
        except Exception:
            self.logger.info(kwargs)
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def tx_person_src(self, raw_person, **kwargs):
        tx_type = kwargs.get("tx_type")
        target = kwargs.get("target")
        person = {
            "src_id": raw_person[self.setting["src_metadata"][target][tx_type]["src_id"]],
            "created_at": pendulum.parse(raw_person[
                self.setting["src_metadata"][target][tx_type]["created_at"]
            ]),
            "updated_at": pendulum.parse(raw_person[
                self.setting["src_metadata"][target][tx_type]["updated_at"]
            ])
        }
        raw_person = self.tx_person_src_ext(raw_person, **kwargs)
        try:
            person.update(
                {
                    "data": self.transform_data(
                        raw_person,
                        self.map[target].get(tx_type),
                    )
                }
            )
        except Exception:
            log = traceback.format_exc()
            person.update(
                {"tx_status": "F", "tx_note": log, "data": raw_person}
            )
            self.logger.exception(log)

        return person
    
    def tx_person_src_ext(self, raw_person, **kwargs):
        if kwargs.get("tx_type") == "company":
            hubspot_owner_id = raw_person.pop("hubspot_owner_id", None)
            hs_created_by_user_id = raw_person.pop("hs_created_by_user_id", None)
            hubspot_team_id = raw_person.get("hubspot_team_id", None)
            seller_sales_rep_id = raw_person.get("seller_sales_rep2", None)
            seller_sales_rep_assistant_id = raw_person.get("seller_sales_rep_assistant", None)
            sales_rep_assistant_id = raw_person.get("sales_rep_assistant", None)
            hs_parent_company_id = raw_person.pop("hs_parent_company_id", None)
            owner = self.get_hubspot_user_by_id(hubspot_owner_id)
            created_by_user = self.get_hubspot_user_by_id(hs_created_by_user_id)
            seller_sales_rep = self.get_hubspot_user_by_id(seller_sales_rep_id)
            seller_sales_rep_assistant = self.get_hubspot_user_by_id(seller_sales_rep_assistant_id)
            sales_rep_assistant = self.get_hubspot_user_by_id(sales_rep_assistant_id)
            if hs_parent_company_id:
                parent_company = self.hubspot_connector.get_company(hs_parent_company_id)
            else:
                parent_company = None

            raw_person["hubspot_owner"] = "{first_name} {last_name}".format(first_name=owner.first_name, last_name=owner.last_name) if owner is not None else None
            raw_person["created_by_user"] = "{first_name} {last_name}".format(first_name=created_by_user.first_name, last_name=created_by_user.last_name) if created_by_user is not None else None
            raw_person["hubspot_team"] = self.get_hubspot_team_label_by_id(hubspot_team_id)
            raw_person["parent_company"] = parent_company.properties.get("name")  if parent_company is not None else None
            raw_person["seller_sales_rep2"] = "{first_name} {last_name}".format(first_name=seller_sales_rep.first_name, last_name=seller_sales_rep.last_name) if seller_sales_rep is not None else None
            raw_person["seller_sales_rep_assistant"] = "{first_name} {last_name}".format(first_name=seller_sales_rep_assistant.first_name, last_name=seller_sales_rep_assistant.last_name) if seller_sales_rep_assistant is not None else None
            raw_person["sales_rep_assistant"] = "{first_name} {last_name}".format(first_name=sales_rep_assistant.first_name, last_name=sales_rep_assistant.last_name) if sales_rep_assistant is not None else None
            for key,value in raw_person.items():
                if key not in ["hs_object_id"] and isinstance(value, str) and (value.isdigit() or ((value.split(".")[0]).isdigit() and (value.split(".")[-1]).isdigit())):
                    # if Decimal(value) == Decimal(value).to_integral():
                    #     actual_value = int(value)
                    # else:
                    actual_value = float(value)
                    raw_person[key] = actual_value
                else:
                    raw_person[key] = value
        elif kwargs.get("tx_type") == "contact":
            primary_company_id = self.hubspot_connector.get_contact_primary_company_id(raw_person["hs_object_id"])
            primary_company = None
            if primary_company_id:
                try:
                    company = self.hubspot_connector.get_company(company_id=primary_company_id, properties=self.setting.get("company_properties", []))
                    primary_company = company.properties
                except Exception as e:
                    pass
            raw_person["primary_company"] = primary_company
        return raw_person
    
    def get_companies(self, **params):
        company_params = {}
        company_params["filter_groups"] = [
            {
                "filters": [
                    {
                        "dateTimeFormat": "EPOCH_MILLISECONDS",
                        "value": int(datetime.strptime(params.get("cut_date", ""), "%Y-%m-%dT%H:%M:%S%z").timestamp() * 1000),
                        "highValue": int(datetime.strptime(params.get("end_date", ""), "%Y-%m-%dT%H:%M:%S%z").timestamp() * 1000),
                        "propertyName": "hs_lastmodifieddate",
                        "operator": "BETWEEN"
                    }
                    
                ]
            }
        ]
        limit_count = params.get("limit", 100)
        limit = 100
        if int(limit_count) < limit:
            limit = limit_count
        company_params['limit_count'] = limit_count
        company_params["limit"] = limit
        company_params["sorts"] = ["hs_lastmodifieddate"]
        # company_params["properties"] = ["account_manager","account_tags", "account_transfer_date", "annualrevenue", "category", "city", "domain", "name", "hubspot_owner_id", "contract_manufacturer", "country", 
        #                                 "createdate", "hs_created_by_user_id", "customer_group", "customer_territory", "date_of_first_registration", "engagements_last_meeting_booked", "days_to_close", "division", 
        #                                 "event_tags", "first_contact_createdate", "first_conversion_event_name", "first_conversion_date", "first_deal_created_date", "hs_analytics_first_touch_converting_campaign",
        #                                 "hubspot_team_id", "industries", "industry", "is_factory", "notes_last_updated", "notes_last_contacted", "hs_last_sales_activity_timestamp", "hs_lastmodifieddate", "hs_analytics_latest_source",
        #                                 "hs_analytics_latest_source_data_1", "hs_analytics_latest_source_data_2", "hs_analytics_latest_source_timestamp", "lead_qualified_date", "lead_qualified_", "lead_qualifier",
        #                                 "lead_score", "lead_source", "hs_lead_status", "lifecyclestage", "hs_predictivecontactscore_v2", "netsuite_company_id", "num_associated_contacts", "num_associated_deals","hs_num_child_companies",
        #                                 "numberofemployees", "num_conversion_events", "hs_num_open_deals", "hs_analytics_num_page_views", "hs_analytics_num_visits", "num_contacted_notes", "hs_analytics_source_data_1",
        #                                 "hs_analytics_source_data_2", "hs_analytics_source", "hubspot_owner_assigneddate", "hs_parent_company_id", "zip", "product_information", "recent_conversion_event_name", "recent_conversion_date",
        #                                 "recent_deal_amount", "recent_deal_close_date", "reference_id", "sales_rep_assistant", "seller_contract_expiration_date", "seller_first_contract_date", "seller_product_list",
        #                                 "seller_program", "seller_sales_rep2", "seller_rep_assigned_date", "seller_sales_rep_assistant", "seller_status", "stage", "state", "status", "target_ingredients", "hs_analytics_first_timestamp", "hs_analytics_last_timestamp",
        #                                 "hs_analytics_first_visit_timestamp", "hs_analytics_last_visit_timestamp", "timezone", "total_revenue", "type", "test2", "test", "vip_seller", "website"]
        company_params["properties"] = self.setting.get("company_properties", None)
        # company_params["after"] = 10000
        return self.hubspot_connector.get_companies(**company_params)
    
    def get_contacts(self, **params):
        contact_params = {}
        contact_params["filter_groups"] = [
            {
                "filters": [
                    {
                        "dateTimeFormat": "EPOCH_MILLISECONDS",
                        "value": int(datetime.strptime(params.get("cut_date", ""), "%Y-%m-%dT%H:%M:%S%z").timestamp() * 1000),
                        "highValue": int(datetime.strptime(params.get("end_date", ""), "%Y-%m-%dT%H:%M:%S%z").timestamp() * 1000),
                        "propertyName": "lastmodifieddate",
                        "operator": "BETWEEN"
                    },
                    {
                        "value": True,
                        "propertyName": "can_sync",
                        "operator": "EQ"
                    }
                ]
            }
        ]
        limit_count = params.get("limit", 100)
        limit = 100
        if int(limit_count) < limit:
            limit = limit_count
        contact_params['limit_count'] = limit_count
        contact_params["limit"] = limit
        contact_params["sorts"] = ["lastmodifieddate"]
        contact_params["properties"] = self.setting.get("contact_properties", None)
        return self.hubspot_connector.get_contacts(**contact_params)
    
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
        hs_deal_id = transaction["data"].pop("hs_deal_id", None)
        only_update_exists_deal = False
        if hs_deal_id is not None:
            only_update_exists_deal = True

        if order_status != "Billed" and hs_deal_id is None:
            raise IgnoreException(f"{deal_number}'s status is not Billed, can not be synced to hubspot.")
        
        if only_update_exists_deal:

            deal_update_fields = self.setting.get("hubspot_deal_udpate_fields", [])
            if len(deal_update_fields) == 0:
                raise Exception(f"No avaliable update fields in {deal_number}.")
            transaction["data"]["status"] = order_status
            update_data = {
                key: value
                for key, value in transaction["data"].items()
                if key in deal_update_fields
            }
            if len(update_data) == 0:
                raise Exception(f"No avaliable update fields in {deal_number}.")
            update_data["hs_object_id"] = hs_deal_id
            deal_id = self.hubspot_connector.update_deal(update_data)
            return deal_id

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
    
    def update_deal_item(self, transaction):
        deal_number = transaction["data"].pop("deal_number")
        sku = transaction["data"].pop("sku", None)
        if sku is None:
            raise Exception(f"{sku} can not be empty")
        deal = self.hubspot_connector.get_deal(deal_id=deal_number, id_property=self.setting["id_property"]["order"])
        if deal is None:
            raise Exception(f"{deal_number} does not exist in hubspot")
        
        line_items_result = self.hubspot_connector.get_deal_association(deal_id=deal.id, to_object_type="line_items")
        if len(line_items_result.results) > 0:
            line_items = []
            for line_item in line_items_result.results:
                line_item_result = self.hubspot_connector.get_line_item(line_item_id=line_item.id, properties=["amount", "hs_sku", "quantity", "price"])
                if line_item_result is not None and line_item_result.properties.get("hs_sku") == sku:
                    self.hubspot_connector.update_line_item(line_item_id=line_item.id, properties=transaction["data"])
    
    def update_deal(self, transaction):
        items = transaction["data"].pop("items", [])
        deal_number = transaction["data"].get("deal_number")
        deal = self.hubspot_connector.get_deal(deal_id=deal_number, id_property=self.setting["id_property"]["order"])
        if deal is None:
            raise Exception(f"{deal_number} does not exist in hubspot")
        
        deal_update_fields = self.setting.get("hubspot_deal_udpate_fields", [])
        if len(deal_update_fields) == 0:
            raise Exception(f"No avaliable update fields in {deal_number}.")
        
        update_data = {
            key: value
            for key, value in transaction["data"].items()
            if key in deal_update_fields
        }
        if len(update_data) == 0:
            raise Exception(f"No avaliable update fields in {deal_number}.")
        
        update_data["deal_number"] = deal_number
        id_property=self.setting["id_property"]["order"]
        deal_id = self.hubspot_connector.update_deal(properties=update_data, id_property=id_property)
        return deal_id

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
    
    def get_owner_by_name(self, sales_rep):
        if isinstance(sales_rep, str):
            owners_name_mapping = self.get_owners_name_mapping()
            return owners_name_mapping.get(sales_rep.lower(), None)
        return None
    
    def get_hubspot_user_by_id(self, hubspot_user_id):
        hubspot_users = self.get_all_hubspot_users()
        return hubspot_users.get(str(hubspot_user_id), None)
    
    def get_owners_name_mapping(self):
        if len(self.all_owners) == 0:
            owners = self.get_all_hubspot_users()
            for owner_id, owner in owners.items():
                owner_name = "{first_name} {last_name}".format(first_name=owner.first_name, last_name=owner.last_name)
                self.all_owners[owner_name.lower()] = owner
        return self.all_owners
    
    def get_all_hubspot_users(self):
        if len(self.hubspot_users) > 0:
            return self.hubspot_users
        hubspot_users = self.hubspot_connector.get_all_owners()
        for user in hubspot_users:
            self.hubspot_users[str(user.id)] = user
        return self.hubspot_users
    
    def get_hubspot_team_label_by_id(self, hubspot_team_id):
        hubspot_team_options = self.get_hubspot_team_options()
        return hubspot_team_options.get(str(hubspot_team_id), None)
    
    def get_hubspot_team_options(self):
        if self.hubspot_team_options is not None:
            return self.hubspot_team_options
        try:
            hubspot_teams_result = self.hubspot_connector.hubspot.settings.users.teams_api.get_all()
            self.hubspot_team_options = {}
            for team in hubspot_teams_result.results:
                self.hubspot_team_options[str(team.id)] = team.name
        except Exception as e:
            self.logger.info(str(e))
            pass
        if self.hubspot_team_options is None:
            self.hubspot_team_options = {}
        return self.hubspot_team_options
