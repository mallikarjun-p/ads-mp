import csv
import re
import ast
import requests
import json
import copy

flag_extra_in_csv_to_be_added_in_database = True

cert_path = './creds/jammer-apigee-client.crt'
key_path = './creds/jammer-apigee-client.key'
ca_cert_path = './creds/root-ca-and-jammer-services-ca-chain.crt'


SUPPLY_LABEL_ID_TO_NAME_DICT = {
    4019: "platform_partner",
    4020: "content_partner",
    40443: "amagi",
    4017: "platform_partner",
    4016: "content_partner",
    4018: "amagi"
}


def parse_demand_tag_priorities(demand_tag_priorities_str):
    # Remove newlines and other formatting issues
    cleaned_str = re.sub(r'\s+', ' ', demand_tag_priorities_str)
    # Ensure proper list format by adding commas between dictionaries if necessary
    cleaned_str = re.sub(r'\}\s*\{', '}, {', cleaned_str)
    # Convert the cleaned string to a list of dictionaries
    demand_tag_priorities_list = ast.literal_eval(cleaned_str)
    # Extract demand_tag_id from each dictionary in the list
    demand_tag_ids = [str(entry['demand_tag_id']) for entry in demand_tag_priorities_list]
    return demand_tag_ids


def parse_supply_router_ratios(supply_router_ratios_str):
    cleaned_str = re.sub(r'\s+', ' ', supply_router_ratios_str)
    cleaned_str = re.sub(r'\}\s*\{', '}, {', cleaned_str)
    supply_router_ratios_list = ast.literal_eval(cleaned_str)
    supply_tag_ids = [str(entry['supply_tag_id']) for entry in supply_router_ratios_list]
    return supply_tag_ids

def read_demand_tag_csv(file_path):
    supply_data_dict = {}
    with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            id_value = row['id']
            supply_lable_ids_str = row['supply_label_ids']
            demand_tag_priorities_str = row['demand_tag_priorities']
            supply_data_dict[id_value] = {
                "demand_tags": parse_demand_tag_priorities(demand_tag_priorities_str),
                "supply_lable_ids": [int(x) for x in supply_lable_ids_str.strip('[]').split()]
            }
    return supply_data_dict


def read_supply_router_csv(file_path):
    router_data_dict = {}
    with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            id_value = row['id']
            supply_router_ratios_str = row['supply_router_ratios']
            supply_tag_ids = parse_supply_router_ratios(supply_router_ratios_str)
            router_data_dict[id_value] = supply_tag_ids
    return router_data_dict

def update_supply_router_data(supply_router_data, demand_data):
    updated_supply_router_data = {}
    for router_id, supply_tag_ids in supply_router_data.items():
        updated_supply_tag_ids = {}
        for supply_tag_id in supply_tag_ids:
            supply_tag_id = str(supply_tag_id)
            if supply_tag_id in demand_data:
                updated_supply_tag_ids[supply_tag_id] = demand_data.pop(supply_tag_id)
        updated_supply_router_data[router_id] = updated_supply_tag_ids
    return updated_supply_router_data

def update_SSAI_router(ssai_dict):
    endpoint = "https://api-master.dev.amaginow.tv/ssai"
    cert = (cert_path, key_path)
    verify = ca_cert_path
    updated_router_ids = list()
    for router_id, supply_tag_dict in ssai_dict.items():
        if router_id!='46424':
            continue

        original_supply_tags = list()
        url = f"{endpoint}?supply_router_id={int(router_id)}"
        response = requests.request("GET", url, cert=cert, verify=verify)

        if response and response.status_code!=200:
            print(f"not able to find any config for router_id: {router_id}")
            continue

        response_data = response.json()
        if response_data and response_data.get('result') and len(response_data['result'])!=0:
            ads_specification = response_data['result'][0]['configuration']['ads_specification']

            if ads_specification.get("deal_type")=="inventory" and ads_specification.get('inventory'):
                supply_router = ads_specification['inventory'].get('supply_router')
                if supply_router and len(supply_router)!=0:

                    indexes_to_be_deleted_from_db_supply_tags = list()
                    original_supply_tags = copy.deepcopy(supply_router[0].get("supply_tags"))

                    for i, db_supply_tag in enumerate(supply_router[0].get("supply_tags")):
                        is_found = False
                        for supply_tag_id, supply_tag_data in supply_tag_dict.items():
                            if db_supply_tag.get('supply_tag_id')==supply_tag_id:
                                is_found = True
                                db_supply_tag['demand_tags'] = supply_tag_data.get("demand_tags")
                                supply_router[0]["supply_tags"][i] = db_supply_tag
                                supply_tag_dict.pop(supply_tag_id)
                                break

                        if not is_found:
                            indexes_to_be_deleted_from_db_supply_tags.append(i)

                    # to delete tags that are in database, but not in csv
                    for i in indexes_to_be_deleted_from_db_supply_tags:
                        del supply_router[0].get("supply_tags")[i]

                    # to add tags that are in csv, but not in db
                    if flag_extra_in_csv_to_be_added_in_database:
                        for supply_tag_id, supply_tag_data in supply_tag_dict.items():
                            supply_tag_obj = {
                                'supply_tag_id': supply_tag_id,
                                'supply_tag_target': get_supply_tag_target(supply_tag_data.get("supply_lable_ids")),
                                'demand_tags': supply_tag_data.get("demand_tags")
                            }
                            match supply_tag_obj['supply_tag_target']:
                                case "platform_partner":
                                    supply_tag_obj['amg_id'] = response_data['result'][0]['platform']['amg_id']
                                case "content_partner":
                                    supply_tag_obj['amg_id'] = response_data['result'][0]['amg_id']
                                case _:
                                    supply_tag_obj['amg_id'] = "amgxxxx"

                            supply_router[0].get("supply_tags").append(supply_tag_obj)

                response_data['result'][0]['configuration']['ads_specification']['inventory']['supply_router'] = supply_router
                response_data['result'][0]['configuration']['ads_specification']['revenue'] = None

        if original_supply_tags==supply_router[0].get("supply_tags"):
            print(f"skipping the updation in configuration for router_id: {router_id}, as there is no change")
            continue

        print(f"updating the configuration for router_id: {router_id}")
        updated_router_ids.append(router_id)

        payload = response_data['result'][0]
        headers = {
            'x-account-id': payload['amg_id'],
            "Content-Type": "application/json"
        }
        response = requests.request("PUT", url, headers=headers, cert=cert, verify=verify, data=json.dumps(payload))
        if response.status_code==200:
            print(f"successfully updated router config for router_id: {router_id}")
        else:
            print(f"failed to update router config for router_id {router_id}")

    print(f"total routers updated : {len(updated_router_ids)}")
    return updated_router_ids

def update_SSAI_supplyTags(ssai_supply_dict):
    endpoint = "https://api-master.dev.amaginow.tv/ssai"
    cert = (cert_path, key_path)
    verify = ca_cert_path

    updated_supply_ids = list()
    for supply_tag_id in list(ssai_supply_dict.keys()):
        demand_tag_data = ssai_supply_dict[supply_tag_id]
        if len(demand_tag_data.get("demand_tags"))==0:
            continue

        if supply_tag_id != '721386':
            continue

        original_supply_tags = list()
        updated_supply_tags = list()
        url = f"{endpoint}?supply_tag_id={int(supply_tag_id)}"
        response = requests.request("GET", url, cert=cert, verify=verify)

        if response and response.status_code != 200:
            print(f"not able to find any config for supply_tag_id: {supply_tag_id}")
            continue

        response_data = response.json()
        if response_data and response_data.get('result') and len(response_data['result']) != 0:
            ads_specification = response_data['result'][0]['configuration']['ads_specification']

            if ads_specification.get("deal_type") == "revenue" and ads_specification.get('revenue'):
                original_supply_tags = ads_specification['revenue'].get('supply_tags')
                updated_supply_tags = copy.deepcopy(original_supply_tags)

                for i, db_supply_tag in enumerate(updated_supply_tags):
                    supply_tag_id = db_supply_tag.get('supply_tag_id')
                    if ssai_supply_dict.get(supply_tag_id):
                        db_supply_tag['demand_tags'] = ssai_supply_dict[supply_tag_id]['demand_tags']


                response_data['result'][0]['configuration']['ads_specification']['inventory'] = None
                response_data['result'][0]['configuration']['ads_specification']['revenue']['supply_tags'] = updated_supply_tags

        if original_supply_tags==updated_supply_tags:
            print(f"skipping the updation in configuration for supply_tag_id: {supply_tag_id}, as there is no change")
            continue

        print(f"updating the configuration for supply_tag_id: {supply_tag_id}")
        updated_supply_tags.append(supply_tag_id)

        payload = response_data['result'][0]
        headers = {
            'x-account-id': payload['amg_id'],
            "Content-Type": "application/json"
        }
        response = requests.request("PUT", url, headers=headers, cert=cert, verify=verify, data=json.dumps(payload))
        if response.status_code == 200:
            print(f"successfully updated router config for supply_tag_id: {supply_tag_id}")
        else:
            print(f"failed to update router config for supply_tag_id {supply_tag_id}")

    print(f"total supply_ids updated : {len(updated_supply_ids)}")
    return updated_supply_ids

def get_supply_tag_target(supply_lable_ids):
    for supply_label_id in supply_lable_ids:
        if SUPPLY_LABEL_ID_TO_NAME_DICT.get(supply_label_id):
            return SUPPLY_LABEL_ID_TO_NAME_DICT[supply_label_id]
    return "Not Found"

if __name__ == "__main__":
    demand_tag_file_path = './fixtures/dmp.config.ss_core_supply_tags-2024-05-28.csv'
    supply_router_file_path = './fixtures/dmp.config.ss_core_router_tags-2024-05-28.csv'


    # Processing demand_tag_priorities CSV
    supplyTag_to_demandTags = read_demand_tag_csv(demand_tag_file_path)
    print("Demand Tag Data length:")
    print(len(supplyTag_to_demandTags))

    # Processing supply_router_ratios CSV
    routerTag_to_supplyTags = read_supply_router_csv(supply_router_file_path)
    print("Supply Router Data length:")
    print(len(routerTag_to_supplyTags))

    # Update supply_router_data with demand_data
    updated_supply_router_data = update_supply_router_data(routerTag_to_supplyTags, supplyTag_to_demandTags)
    print("Updated Supply Router Data:")

    # update_SSAI_router(updated_supply_router_data)
    update_SSAI_supplyTags(supplyTag_to_demandTags)