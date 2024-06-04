import csv
import re
import ast

def parse_demand_tag_priorities(demand_tag_priorities_str):
    # Remove newlines and other formatting issues
    cleaned_str = re.sub(r'\s+', ' ', demand_tag_priorities_str)
    # Ensure proper list format by adding commas between dictionaries if necessary
    cleaned_str = re.sub(r'\}\s*\{', '}, {', cleaned_str)
    # Convert the cleaned string to a list of dictionaries
    demand_tag_priorities_list = ast.literal_eval(cleaned_str)
    # Extract demand_tag_id from each dictionary in the list
    demand_tag_ids = [entry['demand_tag_id'] for entry in demand_tag_priorities_list]
    return demand_tag_ids


def parse_supply_router_ratios(supply_router_ratios_str):
    cleaned_str = re.sub(r'\s+', ' ', supply_router_ratios_str)
    cleaned_str = re.sub(r'\}\s*\{', '}, {', cleaned_str)
    supply_router_ratios_list = ast.literal_eval(cleaned_str)
    supply_tag_ids = [entry['supply_tag_id'] for entry in supply_router_ratios_list]
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
    print(updated_supply_router_data)