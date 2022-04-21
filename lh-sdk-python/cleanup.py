import argparse
import logging
import requests


def get_id_from_name_or_id(resource_type, name, url):
    req_url = f'{url}/{resource_type}/{name}'
    response = requests.get(req_url)
    try:
        response.raise_for_status()
        j = response.json()
        assert j['result'] is not None
        logging.info(f"Successfully looked up {resource_type} {name} by ID")
        return j['objectId']

    except Exception:
        logging.info(f"Looking up {resource_type} {name} by name")

    req_url = f'{url}/{resource_type}Alias/name/{name}'
    response = requests.get(req_url)
    response.raise_for_status()
    j = response.json()
    return j['result']['entries'][0]['objectId']


def delete(resource_type, obj_id, url):
    req_url = f"{url}/{resource_type}/{obj_id}"
    requests.delete(req_url).raise_for_status()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "resource_type",
        help="Type of Resource to delete",
        choices=['WFSpec', 'TaskDef', 'ExternalEventDef'],
    )
    parser.add_argument(
        "resource_name",
        help='Name or ID of resource to delete',
    )
    parser.add_argument(
        "--api-url", '-u',
        default="http://localhost:5000",
        help="URL of LittleHorse API"
    )

    ns = parser.parse_args()

    obj_id = get_id_from_name_or_id(
        ns.resource_type, ns.resource_name, ns.api_url
    )
    delete(ns.resource_type, obj_id, ns.api_url)
    print(f"Successfully cleaned up {ns.resource_type} {ns.resource_name} (:")

