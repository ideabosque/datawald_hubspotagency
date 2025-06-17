#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import traceback, pendulum, time, copy
from datawald_agency import Agency
from datawald_connector import DatawaldConnector
from hubspot_connector import HubspotConnector
from datetime import datetime, timedelta
from pytz import timezone,utc
from decimal import Decimal

class IgnoreException(Exception):
    pass

class HubspotAgency(Agency):
    all_owners = {}
    hubspot_users = {}
    hubspot_team_options = None
    hubspot_properties = {}
    properties_can_process = {}

    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.hubspot_connector = HubspotConnector(logger, setting)
        self.datawald = DatawaldConnector(logger, **setting)
        Agency.__init__(self, logger, datawald=self.datawald)
        if setting.get("tx_type"):
            Agency.tx_type = setting.get("tx_type")

        self.map = setting.get("TXMAP", {})

    def tx_transactions_src(self, **kwargs):
        return self.tx_entities_src(**kwargs)

    def tx_persons_src(self, **kwargs):
        return self.tx_entities_src(**kwargs)

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

    def tx_transaction_src_ext(self, raw_transaction, **kwargs):
        return raw_transaction
    
    def tx_transaction_tgt(self, transaction):
        return transaction
    
    def tx_transaction_tgt_ext(self, new_transaction, transaction):
        pass

    def insert_update_transactions(self, transactions):
        for transaction in transactions:
            self.insert_update_entity(transaction)
        return transactions
    
    

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
        hs_type = self.get_hs_type(kwargs.get("tx_type"))
        if hs_type == "company":
            raw_person = self.process_hubspot_properties_values(
                object_type="company",
                properties_data=raw_person,
                ignore_properties=[],
                properties=self.setting.get("company_properties")
            )
        elif hs_type == "contact":
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
    
    def get_companies_by_ids(self, **params):
        hs_object_ids = params.get("hs_object_ids", [])
        if len(hs_object_ids) == 0:
            return []
        company_params = {}
        company_params["filter_groups"] = [
            {
                "filters": [
                    {
                    "values": hs_object_ids,
                    "propertyName": "hs_object_id",
                    "operator": "IN"
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
        company_params["properties"] = self.setting.get("company_properties", None)
        return self.hubspot_connector.get_companies(**company_params)
    
    def tx_person_tgt(self, person):
        tx_type = person.get("tx_type_src_id").split("-")[0]
        hs_type = self.get_hs_type(tx_type)
        if hs_type == "contact":
            hubspot_properties = self.get_properties_can_be_processed(object_type=tx_type, properties=self.setting.get("contact_properties", None))
        elif hs_type == "company":
            hubspot_properties = self.get_properties_can_be_processed(object_type=tx_type, properties=self.setting.get("company_properties", None))
        new_person = copy.deepcopy(person)
        for property_name, value in person["data"].items():
            if property_name not in hubspot_properties and property_name != "attachments":
                new_person["data"].pop(property_name)
            elif property_name in hubspot_properties:
                if hubspot_properties[property_name]["field_type"] == "file":
                    try:
                        if isinstance(person["data"][property_name], dict):
                            file_id = self.hubspot_connector.upload_file_by_url(**person["data"][property_name])
                            new_person["data"][property_name] = file_id
                        elif isinstance(person["data"][property_name], list):
                            file_ids = []
                            for file_data in person["data"][property_name]:
                                if isinstance(file_data, dict):
                                    file_id = self.hubspot_connector.upload_file_by_url(**file_data)
                                    file_ids.append(file_id)
                            new_person["data"][property_name] = ";".join(file_ids)
                    except Exception as e:
                        new_person["data"].pop(property_name)
                        self.logger.error(e)
                        pass
        return new_person

    def tx_person_tgt_ext(self, new_person, person):
        pass

    def insert_update_persons(self, persons):
        for person in persons:
            tx_type = person.get("tx_type_src_id").split("-")[0]
            hs_type = self.get_hs_type(tx_type)
            try:
                if hs_type == "contact":
                    person["tgt_id"] = self.hubspot_connector.insert_update_contact(
                        person["data"], id_property=self.setting["id_property"][tx_type]
                    )
                elif hs_type == "company":
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
            hs_type = self.get_hs_type(tx_type)
            try:
                if hs_type == "product":
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
    
    def get_owner_by_name(self, sales_rep):
        if isinstance(sales_rep, str):
            owners_name_mapping = self.get_owners_name_mapping()
            return owners_name_mapping.get(sales_rep.lower(), None)
        return None
    
    def get_hubspot_user_by_id(self, hubspot_user_id):
        hubspot_users = self.get_all_hubspot_users()
        return hubspot_users.get(str(hubspot_user_id), None)
    
    def get_hubspot_user_name_by_id(self, hubspot_user_id):
        hubspot_users = self.get_all_hubspot_users()
        user = hubspot_users.get(str(hubspot_user_id), None)
        if user is None:
            return None
        if user.archived:
            return "{first_name} {last_name} (Deactivated User)".format(first_name=user.first_name, last_name=user.last_name)
        else:
            return "{first_name} {last_name}".format(first_name=user.first_name, last_name=user.last_name)
    
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

    def get_hubspot_properties(self, object_type, properties=None):
        if object_type in self.hubspot_properties:
            return self.hubspot_properties.get(object_type)
        try:
            response = self.hubspot_connector.get_properties_by_object_type(object_type, properties)
            self.hubspot_properties[object_type] = [
                property_model.to_dict()
                for property_model in response.results
            ]
        except Exception as e:
            self.logger.info(str(e))
            self.hubspot_properties[object_type] = None
            pass
        return self.hubspot_properties[object_type]
    
    def get_properties_can_be_processed(self, object_type, properties=None):
        if object_type in self.properties_can_process:
            return self.properties_can_process.get(object_type, {})
        hubspot_properties = self.get_hubspot_properties(object_type, properties)
        process_properties = {
            property_data.get("name"): property_data
            for property_data in hubspot_properties
        }
        for name, data in process_properties.items():
            if len(data.get("options", [])) > 0:
                process_properties[name]["options_mapping"] = {
                    option.get("value"): option.get("label")
                    for option in data.get("options")
                    if option.get("value") and option.get("label")
                }
            else:
                process_properties[name]["options_mapping"] = {}
        self.properties_can_process[object_type] = process_properties

        return self.properties_can_process[object_type]
    
    def format_property_value(self, property_setting, value):
        if value is None:
            return value
        if property_setting.get("field_type") == "checkbox" and len(property_setting.get("options", [])) > 0:
            value_arr = [
                property_setting.get("options_mapping", {}).get(one, one)
                for one in value.split(";")
            ]
            return ";".join(value_arr)
        elif property_setting.get("type") == "enumeration" and len(property_setting.get("options", [])) > 0:
            return property_setting.get("options_mapping", {}).get(value, value)
        elif property_setting.get("type") == "number" and value:
            return float(value)
        elif property_setting.get("type") == "enumeration" and property_setting.get("type") == "OWNER":
            return self.get_hubspot_user_name_by_id(value)
        elif property_setting.get("type") == "number" and property_setting.get("type") == "COMPANY":
            company =  self.hubspot_connector.get_company(value)
            if company is not None:
                return company.properties.get("name")
            else:
                return None
            
        return value
    
    def process_hubspot_properties_values(self, object_type, properties_data, ignore_properties=[], properties=None):
        process_properties = self.get_properties_can_be_processed(object_type, properties)
        convert_timezone = self.setting.get("convert_timezone_settings", {})
        for property_name, property_setting in process_properties.items():
            if property_name in properties_data and property_name not in ignore_properties:
                new_value = self.format_property_value(property_setting=property_setting, value=properties_data.get(property_name))
                if property_name.find("_id") != -1:
                    properties_data[property_name.replace("_id","")] = new_value
                if len(convert_timezone) > 0 and property_setting.get("type") == "datetime" and properties_data.get(property_name):
                    for suffix, timezone_name in convert_timezone.items():
                        field_name_with_suffix = "{proterty_name}_{suffix}".format(proterty_name=property_name, suffix=suffix)
                        if properties_data.get(property_name, "").find(".") != -1:
                            properties_data[field_name_with_suffix] = datetime.strptime(properties_data.get(property_name), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=utc).astimezone(timezone(timezone_name)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        else:
                            properties_data[field_name_with_suffix] = datetime.strptime(properties_data.get(property_name), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=utc).astimezone(timezone(timezone_name)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                            new_value = datetime.strptime(properties_data.get(property_name), "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                properties_data[property_name] = new_value
        return properties_data

    def get_hs_type(self, tx_type):
        hs_types = self.setting.get("hs_types", {})
        return hs_types.get(tx_type, tx_type)