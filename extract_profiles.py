import logging
import json
import time
import random
import argparse
from seleniumbase import SB
from bs4 import BeautifulSoup
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_profile_info(sb, url, batch_number, link_index):
    logging.info(f"Extracting profile info from URL: {url}")
    
    try:
        sb.cdp.open(url)
        logging.info(f"Page opened successfully: {url}")
        time.sleep(7)
        
        # Check if page loaded
        current_url = sb.cdp.get_current_url()
        logging.info(f"Current URL after load: {current_url}")
        
        html = sb.cdp.get_page_source()
        logging.info(f"Page source length: {len(html)} characters")
        
        # Check for common indicators that page loaded
        if "zillow" in html.lower():
            logging.info("Zillow content detected in page")
        else:
            logging.warning("No Zillow content detected in page")
            
        soup = BeautifulSoup(html, 'html.parser')
        script_tag = soup.find('script', id='__NEXT_DATA__')
        
        if script_tag:
            json_data = json.loads(script_tag.string)
            profile_info = extract_profile_info_from_json(json_data)
            logging.info(f"Extracted profile info: {profile_info}")
        else:
            logging.error("Script tag with id '__NEXT_DATA__' not found.")
            # Take screenshot for debugging
            screenshot_name = f"debug_screenshot_batch_{batch_number}_link_{link_index}_{int(time.time())}.png"
            try:
                sb.save_screenshot(screenshot_name)
                logging.info(f"Screenshot saved: {screenshot_name}")
                
                # Also save page source for debugging
                html_name = f"debug_page_source_batch_{batch_number}_link_{link_index}_{int(time.time())}.html"
                with open(html_name, 'w', encoding='utf-8') as f:
                    f.write(html)
                logging.info(f"Page source saved: {html_name}")
                
            except Exception as screenshot_error:
                logging.error(f"Failed to save screenshot: {screenshot_error}")
            
            return {}
    except Exception as e:
        logging.error(f"Failed to extract profile info: {e}")
        # Take screenshot for debugging exceptions too
        screenshot_name = f"error_screenshot_batch_{batch_number}_link_{link_index}_{int(time.time())}.png"
        try:
            sb.save_screenshot(screenshot_name)
            logging.info(f"Error screenshot saved: {screenshot_name}")
        except Exception as screenshot_error:
            logging.error(f"Failed to save error screenshot: {screenshot_error}")
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
    parser.add_argument('--run_uuid', required=True)
    parser.add_argument('--proxy_username', required=True)
    parser.add_argument('--proxy_password', required=True)
    parser.add_argument('--proxy_dns', required=True)
    args = parser.parse_args()

    parent_url = args.parent_url
    batch_number = args.batch_number
    run_uuid = args.run_uuid
    
    # Try to parse JSON from command line argument, with better error handling
    try:
        batch_links = json.loads(args.batch_links)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse batch_links JSON: {e}")
        logging.error(f"Received batch_links: {repr(args.batch_links)}")
        # Try to get from environment variable as fallback
        batch_links_env = os.environ.get('BATCH_LINKS')
        if batch_links_env:
            try:
                batch_links = json.loads(batch_links_env)
                logging.info("Successfully parsed batch_links from environment variable")
            except json.JSONDecodeError as env_e:
                logging.error(f"Failed to parse batch_links from environment: {env_e}")
                logging.error(f"Environment batch_links: {repr(batch_links_env)}")
                raise
        else:
            raise
    
    csv_filename = args.csv_filename
    proxy_username = args.proxy_username
    proxy_password = args.proxy_password
    proxy_dns = args.proxy_dns
    proxy_selenium = f"{proxy_username}:{proxy_password}@{proxy_dns}"

    batch_results = []
    
    logging.info(f"Starting batch processing:")
    logging.info(f"  - Parent URL: {parent_url}")
    logging.info(f"  - Batch number: {batch_number}")
    logging.info(f"  - Run UUID: {run_uuid}")
    logging.info(f"  - Number of links: {len(batch_links)}")
    logging.info(f"  - CSV filename: {csv_filename}")
    logging.info(f"  - Proxy: {proxy_dns}")
    
    # Process all links with single Chrome session (reuse until blocked)
    with SB(uc=True, proxy=proxy_selenium, headless=True) as sb:
        sb.activate_cdp_mode("about:blank", tzone="America/Panama")
        
        for i, link in enumerate(batch_links, 1):
            logging.info(f"Processing profile {i}/{len(batch_links)}: {link}")
            
            while True:
                try:
                    profile_info = extract_profile_info(sb, link, batch_number, i)
                    if not profile_info:
                        # Session might be blocked, get new driver
                        logging.warning(f"Profile extraction failed for {link}, refreshing driver...")
                        sb.driver.quit()
                        sb.get_new_driver(undetectable=True, proxy=proxy_selenium)
                        sb.activate_cdp_mode("about:blank", tzone="America/Panama")
                        continue
                    else:
                        logging.info(f"Successfully extracted profile info for {link}")
                        break
                except Exception as e:
                    logging.error(f"Error processing {link}: {e}")
                    # Try refreshing the driver
                    try:
                        sb.driver.quit()
                        sb.get_new_driver(undetectable=True, proxy=proxy_selenium)
                        sb.activate_cdp_mode("about:blank", tzone="America/Panama")
                    except Exception as refresh_error:
                        logging.error(f"Failed to refresh driver: {refresh_error}")
                        profile_info = {}
                        break
            
            batch_results.append({
                "profile_link": link,
                "profile_data": profile_info
            })
            time.sleep(random.uniform(1, 2))

    # Save batch results as JSON artifact
    json_name = f"{csv_filename.replace('.csv','')}-{batch_number}-{run_uuid}.json"
    with open(json_name, 'w', encoding='utf-8') as f:
        json.dump(batch_results, f, indent=2, ensure_ascii=False)
    logging.info(f"Batch results saved to {json_name}")

if __name__ == "__main__":
    main() 
