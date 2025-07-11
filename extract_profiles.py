import logging
import json
import time
import random
import argparse
from seleniumbase import SB
from bs4 import BeautifulSoup
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_profile_info(sb, url):
    logging.info(f"Extracting profile info from URL: {url}")
    sb.cdp.open(url)
    time.sleep(7)
    profile_info = {}
    try:
        html = sb.cdp.get_page_source()
        soup = BeautifulSoup(html, 'html.parser')
        script_tag = soup.find('script', id='__NEXT_DATA__')
        if script_tag:
            json_data = json.loads(script_tag.string)
            profile_info = extract_profile_info_from_json(json_data)
            logging.info(f"Extracted profile info: {profile_info}")
        else:
            logging.error("Script tag with id '__NEXT_DATA__' not found.")
            return {}
    except Exception as e:
        logging.error(f"Failed to extract profile info: {e}")
        return {}
    return profile_info

def extract_profile_info_from_json(json_data):
    profile_info = {}
    try:
        profile_info["Name"] = json_data["props"]["pageProps"]["displayUser"]["name"]
    except KeyError:
        profile_info["Name"] = None
    try:
        profile_info["Personal Phone"] = json_data["props"]["pageProps"]["displayUser"]["phoneNumbers"]["cell"]
    except KeyError:
        profile_info["Personal Phone"] = None
    try:
        profile_info["Business Phone"] = json_data["props"]["pageProps"]["displayUser"]["phoneNumbers"]["business"]
    except KeyError:
        profile_info["Business Phone"] = None
    try:
        profile_info["Email"] = json_data["props"]["pageProps"]["displayUser"]["email"]
    except KeyError:
        profile_info["Email"] = None
    try:
        business_address = json_data["props"]["pageProps"]["displayUser"]["businessAddress"]
        profile_info["Address"] = f"{business_address['address1']}, {business_address['city']}, {business_address['state']} {business_address['postalCode']}"
    except KeyError:
        profile_info["Address"] = None
    try:
        profile_info["Business Name"] = json_data["props"]["pageProps"]["displayUser"]["businessName"]
    except KeyError:
        profile_info["Business Name"] = None
    try:
        profile_info["Ratings Count"] = json_data["props"]["pageProps"]["displayUser"]["ratings"]["count"]
    except KeyError:
        profile_info["Ratings Count"] = None
    try:
        profile_info["Ratings Average"] = json_data["props"]["pageProps"]["displayUser"]["ratings"]["average"]
    except KeyError:
        profile_info["Ratings Average"] = None
    return profile_info

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--parent_url', required=True)
    parser.add_argument('--batch_number', type=int, required=True)
    parser.add_argument('--batch_links', required=True)
    parser.add_argument('--csv_filename', required=True)
    parser.add_argument('--proxy_username', required=True)
    parser.add_argument('--proxy_password', required=True)
    parser.add_argument('--proxy_dns', required=True)
    args = parser.parse_args()

    parent_url = args.parent_url
    batch_number = args.batch_number
    batch_links = json.loads(args.batch_links)
    csv_filename = args.csv_filename
    proxy_username = args.proxy_username
    proxy_password = args.proxy_password
    proxy_dns = args.proxy_dns
    proxy_selenium = f"{proxy_username}:{proxy_password}@{proxy_dns}"

    batch_results = []
    with SB(uc=True, proxy=proxy_selenium, xvfb=True) as sb:
        sb.activate_cdp_mode("about:blank", tzone="America/Panama")
        for i, link in enumerate(batch_links, 1):
            logging.info(f"Processing profile {i}/{len(batch_links)}: {link}")
            while True:
                profile_info = extract_profile_info(sb, link)
                if not profile_info:
                    sb.driver.quit()
                    sb.get_new_driver(undetectable=True, proxy=proxy_selenium, xvfb=True)
                    sb.activate_cdp_mode("about:blank", tzone="America/Panama")
                    continue
                else:
                    break
            batch_results.append({
                "profile_link": link,
                "profile_data": profile_info
            })
            time.sleep(random.uniform(1, 2))

    # Save batch results as JSON artifact
    json_name = f"{csv_filename.replace('.csv','')}-{batch_number}.json"
    with open(json_name, 'w', encoding='utf-8') as f:
        json.dump(batch_results, f, indent=2, ensure_ascii=False)
    logging.info(f"Batch results saved to {json_name}")

if __name__ == "__main__":
    main() 
