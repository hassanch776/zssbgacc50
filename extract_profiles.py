import logging
import json
import time
import random
import argparse
from seleniumbase import SB
from bs4 import BeautifulSoup
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import queue

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration: Number of Chrome instances to run in parallel
NUM_CHROME_INSTANCES = 2  # Reduced to 2 for better stability
MAX_RETRIES_PER_LINK = 3  # Maximum retries per link
THREAD_TIMEOUT_SECONDS = 600  # 10 minutes timeout per thread
OPERATION_TIMEOUT_SECONDS = 60  # 1 minute timeout per operation

class TimeoutException(Exception):
    pass

def extract_profile_info_with_timeout(sb, url, batch_number, link_index, timeout_seconds=OPERATION_TIMEOUT_SECONDS):
    """Extract profile info with operation-level timeout using threading"""
    logging.info(f"Extracting profile info from URL: {url}")
    
    result_queue = queue.Queue()
    exception_queue = queue.Queue()
    
    def extraction_worker():
        try:
            # This is the operation that often hangs
            logging.info(f"Opening page: {url}")
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
                result_queue.put(profile_info)
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
                
                result_queue.put({})
                
        except Exception as e:
            logging.error(f"Failed to extract profile info: {e}")
            exception_queue.put(e)
            result_queue.put({})
    
    # Start the extraction in a separate thread
    worker_thread = threading.Thread(target=extraction_worker)
    worker_thread.daemon = True
    worker_thread.start()
    
    # Wait for result with timeout
    try:
        result = result_queue.get(timeout=timeout_seconds)
        return result
    except queue.Empty:
        logging.error(f"Operation timed out after {timeout_seconds} seconds for URL: {url}")
        # Take screenshot for debugging timeout
        screenshot_name = f"timeout_screenshot_batch_{batch_number}_link_{link_index}_{int(time.time())}.png"
        try:
            sb.save_screenshot(screenshot_name)
            logging.info(f"Timeout screenshot saved: {screenshot_name}")
        except Exception as screenshot_error:
            logging.error(f"Failed to save timeout screenshot: {screenshot_error}")
        return {}

def extract_profile_info(sb, url, batch_number, link_index):
    """Wrapper for backward compatibility"""
    return extract_profile_info_with_timeout(sb, url, batch_number, link_index)

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

def process_batch_chunk(chunk_links, chunk_id, batch_number, run_uuid, proxy_selenium):
    """Process a chunk of links with a dedicated Chrome instance"""
    chunk_results = []
    
    logging.info(f"Chrome instance {chunk_id} starting with {len(chunk_links)} links")
    
    try:
        # Each instance gets its own SB context - no conflicts
        with SB(uc=True, proxy=proxy_selenium, headless=True) as sb:
            sb.activate_cdp_mode("about:blank", tzone="America/Panama")
            
            for i, link in enumerate(chunk_links, 1):
                logging.info(f"Instance {chunk_id} processing link {i}/{len(chunk_links)}: {link}")
                
                # Add retry limit to prevent infinite loops
                retry_count = 0
                profile_info = {}
                
                while retry_count < MAX_RETRIES_PER_LINK:
                    try:
                        # Add heartbeat logging
                        logging.info(f"Instance {chunk_id}: Attempt {retry_count + 1}/{MAX_RETRIES_PER_LINK} for {link}")
                        
                        profile_info = extract_profile_info(sb, link, batch_number, f"{chunk_id}_{i}")
                        if profile_info:
                            logging.info(f"Instance {chunk_id}: Successfully extracted profile info for {link}")
                            break
                        else:
                            # Session might be blocked, get new driver
                            logging.warning(f"Instance {chunk_id}: Profile extraction failed for {link}, attempt {retry_count + 1}/{MAX_RETRIES_PER_LINK}")
                            retry_count += 1
                            if retry_count < MAX_RETRIES_PER_LINK:
                                try:
                                    logging.info(f"Instance {chunk_id}: Refreshing driver...")
                                    sb.driver.quit()
                                    sb.get_new_driver(undetectable=True, proxy=proxy_selenium)
                                    sb.activate_cdp_mode("about:blank", tzone="America/Panama")
                                    time.sleep(2)  # Brief pause before retry
                                    logging.info(f"Instance {chunk_id}: Driver refreshed successfully")
                                except Exception as refresh_error:
                                    logging.error(f"Instance {chunk_id}: Failed to refresh driver: {refresh_error}")
                                    break
                    except Exception as e:
                        logging.error(f"Instance {chunk_id}: Error processing {link}: {e}")
                        retry_count += 1
                        if retry_count < MAX_RETRIES_PER_LINK:
                            # Try refreshing the driver
                            try:
                                logging.info(f"Instance {chunk_id}: Refreshing driver after error...")
                                sb.driver.quit()
                                sb.get_new_driver(undetectable=True, proxy=proxy_selenium)
                                sb.activate_cdp_mode("about:blank", tzone="America/Panama")
                                time.sleep(2)  # Brief pause before retry
                                logging.info(f"Instance {chunk_id}: Driver refreshed after error")
                            except Exception as refresh_error:
                                logging.error(f"Instance {chunk_id}: Failed to refresh driver: {refresh_error}")
                                break
                        else:
                            logging.error(f"Instance {chunk_id}: Max retries reached for {link}")
                            break
                
                # Always append result, even if empty
                chunk_results.append({
                    "profile_link": link,
                    "profile_data": profile_info
                })
                
                # Add heartbeat after each link
                logging.info(f"Instance {chunk_id}: Completed link {i}/{len(chunk_links)}, moving to next...")
                time.sleep(random.uniform(1, 2))
        
        logging.info(f"Chrome instance {chunk_id} completed processing {len(chunk_results)} links")
        return chunk_results
        
    except Exception as e:
        logging.error(f"Chrome instance {chunk_id} failed with fatal error: {e}")
        # Return partial results if any
        return chunk_results

def split_links_into_chunks(batch_links, num_chunks):
    """Split batch_links into roughly equal chunks"""
    chunk_size = len(batch_links) // num_chunks
    chunks = []
    
    for i in range(num_chunks):
        start = i * chunk_size
        if i == num_chunks - 1:  # Last chunk gets any remaining links
            end = len(batch_links)
        else:
            end = (i + 1) * chunk_size
        
        chunks.append(batch_links[start:end])
    
    return chunks

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

    logging.info(f"Starting batch processing:")
    logging.info(f"  - Parent URL: {parent_url}")
    logging.info(f"  - Batch number: {batch_number}")
    logging.info(f"  - Run UUID: {run_uuid}")
    logging.info(f"  - Number of links: {len(batch_links)}")
    logging.info(f"  - CSV filename: {csv_filename}")
    logging.info(f"  - Proxy: {proxy_dns}")
    logging.info(f"  - Chrome instances: {NUM_CHROME_INSTANCES}")
    logging.info(f"  - Max retries per link: {MAX_RETRIES_PER_LINK}")
    logging.info(f"  - Thread timeout: {THREAD_TIMEOUT_SECONDS} seconds")
    
    # Split batch_links into chunks for parallel processing
    link_chunks = split_links_into_chunks(batch_links, NUM_CHROME_INSTANCES)
    
    logging.info(f"Split {len(batch_links)} links into {len(link_chunks)} chunks:")
    for i, chunk in enumerate(link_chunks):
        logging.info(f"  - Chunk {i+1}: {len(chunk)} links")
    
    # Process chunks in parallel using ThreadPoolExecutor with timeout
    all_results = []
    
    with ThreadPoolExecutor(max_workers=NUM_CHROME_INSTANCES) as executor:
        # Submit all chunks to the thread pool
        future_to_chunk = {
            executor.submit(process_batch_chunk, chunk, i+1, batch_number, run_uuid, proxy_selenium): i+1
            for i, chunk in enumerate(link_chunks)
        }
        
        # Collect results as they complete with timeout
        completed_chunks = 0
        total_chunks = len(future_to_chunk)
        
        for future in as_completed(future_to_chunk, timeout=THREAD_TIMEOUT_SECONDS):
            chunk_id = future_to_chunk[future]
            try:
                chunk_results = future.result(timeout=30)  # 30 second timeout for result retrieval
                all_results.extend(chunk_results)
                completed_chunks += 1
                logging.info(f"Chunk {chunk_id} completed successfully with {len(chunk_results)} results ({completed_chunks}/{total_chunks})")
            except TimeoutError:
                logging.error(f"Chunk {chunk_id} timed out")
                completed_chunks += 1
            except Exception as e:
                logging.error(f"Chunk {chunk_id} failed with error: {e}")
                completed_chunks += 1
        
        logging.info(f"All chunks processed. Completed: {completed_chunks}/{total_chunks}")
    
    # Sort results by original order (based on profile_link)
    try:
        all_results.sort(key=lambda x: batch_links.index(x["profile_link"]))
        logging.info("Results sorted by original order")
    except Exception as e:
        logging.error(f"Failed to sort results: {e}")
    
    # Save batch results as JSON artifact
    json_name = f"{csv_filename.replace('.csv','')}-{batch_number}-{run_uuid}.json"
    with open(json_name, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    logging.info(f"Batch results saved to {json_name} with {len(all_results)} total results")

if __name__ == "__main__":
    main() 
