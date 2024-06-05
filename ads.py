import csv
import re
import ast
import requests
import json

cert_path = './creds/jammer-apigee-client.crt'
key_path = './creds/jammer-apigee-client.key'
ca_cert_path = './creds/root-ca-and-jammer-services-ca-chain.crt'

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
            demand_tag_priorities_str = row['demand_tag_priorities']
            demand_tag_ids = parse_demand_tag_priorities(demand_tag_priorities_str)
            supply_data_dict[id_value] = demand_tag_ids
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

def update_SSAI(ssai_dict):
    endpoint = "https://api-master.dev.amaginow.tv/ssai"
    cert = (cert_path, key_path)
    verify = ca_cert_path

    for router_id, supply_tag_dict in ssai_dict.items():
        if router_id!='46424':
            continue
        
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
                    for supply_tag_id, demand_tags in supply_tag_dict.items():
                        is_found = False
                        for i, supply_tag in enumerate(supply_router[0].get("supply_tags")):
                            if supply_tag.get('supply_tag_id')==supply_tag_id:
                                is_found = True
                                supply_tag['demand_tags'] = demand_tags
                                supply_router[0]['supply_tags'][i]= supply_tag
                                break
                        if not is_found:
                            supply_router[0]['supply_tags'].append({
                                'amg_id': None,
                                'supply_tag_id': supply_tag_id,
                                'supply_tag_target': None,
                                'demand_tags': demand_tags 
                            })

                response_data['result'][0]['configuration']['ads_specification']['inventory']['supply_router'] = supply_router
                response_data['result'][0]['configuration']['ads_specification']['revenue'] = None
            else:
                pass   
        
        print(f"updating the configuration for router_id: {router_id}")

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


if __name__ == "__main__":
    demand_tag_file_path = './fixtures/ss_core_config_supply_tags.csv'
    supply_router_file_path = './fixtures/ss_core_config_router_tags.csv'
    

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
    
    update_SSAI(updated_supply_router_data)