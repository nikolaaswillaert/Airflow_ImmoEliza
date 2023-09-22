import requests
from bs4 import BeautifulSoup
import re
import json
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, as_completed
import time
import pandas as pd
import sys
import threading
import os
from pathlib import Path

# Initialize counter for the counter function
counters = 1

# Build path to file
# Selects current working directory
cwd = Path.cwd()
csv_path = 'data_output/dataframe.csv'
url_path = 'data_output/full_list.txt'
csv_path = (cwd / csv_path).resolve()
url_path = (cwd / url_path).resolve()


# Function to scrape URLs
def scrape_urls(page_num):
    """Scrapes a listing page for all the listing's URLs and writes the output to '.\data_output\full_list.txt'"""
    # Build the URL and make a soup
    base_url = f"https://www.immoweb.be/en/search/house/for-sale?countries=BE&page={page_num}&orderBy=relevance"
    try:
        r = requests.get(base_url)
    except Exception:
        pass
    soup = BeautifulSoup(r.content, "html.parser")
    
    # Look for all links on the page
    urls = []
    for elem in soup.find_all("a", attrs={"class": "card__title-link"}):
        urls.append(elem.get('href'))
        
    # Save URLs to file - full_list.txt (local storage)
    with open(url_path, "a") as f:
        for url in urls:
            f.write(url + '\n')
    return urls

def thread_scraping():
    """Uses threading to get all listing URLs concurrently"""
    full_list_url = []
    num_pages = 5

    # Create a list to store threads
    threads = []
    start_time = time.time()  # Start timer
    print("Scraping search pages...")
    
    # Remove output file if it exists so we have a clean dataset
    if os.path.exists(url_path): 
        os.remove(url_path)

    # Create and start threads
    for i in range(1, num_pages + 1):
        t = threading.Thread(target=lambda: full_list_url.extend(scrape_urls(i)))
        reporting("Search pages scraped:", i)
        threads.append(t)
        t.start()
        
    # Wait for all threads to complete and then join
    for t in threads:
        t.join()

    end_time = time.time()  # Stop timer
    execution_time = end_time - start_time
    print("Scraping completed!              ")
    print("Total URLs scraped:", len(full_list_url))
    print("Total time:", execution_time, "seconds")
    return full_list_url

def reporting(str, i): 
    """Reports on scraping progress"""
    sys.stdout.write(str + ' %d\r' %i)
    sys.stdout.flush()
    return

def counter():
    """Creates a global counter for use in list comprehension"""
    global counters 
    if counters < 1: 
        counters = 1
    else:
        counters +=1
    return

def scrape_house(url):
    """Scrapes all the info from a house listing"""

    # Get the house listing and make a soup
    try:
        house_page = requests.get(url)
        house_page = BeautifulSoup(house_page.text, 'html.parser')
    # Return an enmpty dictionary if we can't parse the URL
    except: 
        return {}

    # Get the hidden info from the java script
    try:
        regex = r"window.classified = (\{.*\})" # Only captures what's between brackets
        script = house_page.find('div',attrs={"id":"main-container"}).script.text
        script = re.findall(regex, script)
        script = json.loads(script[0])
    except:
        return {}

    final_dictionary = {}
    # URL
    try:
        final_dictionary['url'] = url
    except:
        final_dictionary['url'] = 'UNKNOWN'
    # Region
    try:
        final_dictionary['region'] = script['property']['location']['region']
    except:
        final_dictionary['region'] = 'UNKNOWN'
    # Province
    try:
        final_dictionary['province'] = script['property']['location']['province']
    except:
        final_dictionary['province'] = 'UNKNOWN'
    # Locality
    try:
        final_dictionary['locality'] = script['property']['location']['locality']
    except:
        final_dictionary['locality'] = 'UNKNOWN'
    # Type of property
    try:
        final_dictionary['property_type'] = script['property']['type']
    except:
        final_dictionary['property_type'] = 'UNKNOWN'
    # Subtype of property
    try:
        final_dictionary['property_subtype'] = script['property']['subtype']
    except:
        final_dictionary['property_subtype'] = 'UNKNOWN'
    # Price
    try:
        final_dictionary['price'] = script['price']['mainValue']
    except:
        final_dictionary['price'] = 'UNKNOWN'
    # Number of rooms
    try:
        final_dictionary['number_rooms'] = script['property']['bedroomCount']
    except:
        final_dictionary['number_rooms'] = 'UNKNOWN'
    # Living area
    try:
        final_dictionary['living_area'] = script['property']['netHabitableSurface']
    except:
        final_dictionary['living_area'] = 'UNKNOWN'
    # Fully equipped kitchen (Yes/No)
    try:
        final_dictionary['kitchen'] = script['property']['kitchen']['type']
    except:
        final_dictionary['kitchen'] = 0
    # Furnished (Yes/No)
    try:
        final_dictionary['furnished'] = script['transaction']['sale']['isFurnished']
    except:
        final_dictionary['furnished'] = 'UNKNOWN'
    # Open fire (Yes/No)
    try:
        final_dictionary['fireplace'] = script['property']['fireplaceCount']
    except:
        final_dictionary['fireplace'] = 0
    # Terrace (Yes/No)
    try:
        final_dictionary['terrace'] = script['property']['hasTerrace']
    except:
        final_dictionary['terrace'] = 0
    # If yes: Area
    try:
        final_dictionary['terrace_area'] = script['property']['terraceSurface']
    except: 
        final_dictionary['terrace_area'] = 0
    # Garden
    try:
        final_dictionary['garden'] = script['property']['hasGarden']
    except:
        final_dictionary['garden'] = 0
    # If yes: Area
    try:
        final_dictionary['garden_area'] = script['property']['gardenSurface']
    except:
        final_dictionary['garden_area'] = 0
    # Surface of the land
    try: 
        final_dictionary['surface_land'] = script['property']['land']['surface']
    except:
        final_dictionary['surface_land'] = "UNKNOWN"
    # Number of facades
    try:
        final_dictionary['number_facades'] = script['property']['building']['facadeCount']
    except:
        final_dictionary['number_facades'] = "UNKNOWN"
    # Swimming pool (Yes/No)
    try:
        final_dictionary['swimming_pool'] =  script['property']['hasSwimmingPool']
    except:
        final_dictionary['swimming_pool'] = 0
    # State of the building (New, to be renovated, ...)
    try:
        final_dictionary['building_state'] = script['property']['building']['condition']
    except:
        final_dictionary['building_state'] = 'UNKNOWN'

    return final_dictionary

def create_dataframe(ds):
    """Will scrape info from house pages and create a pandas DataFrame from the info we scrape"""
    # Initialize list and fetch all URLs
    houses_links = []
    houses_links = thread_scraping()
    
    print("")
    print("Scraping individual pages...")
    start_time = time.time()  # Start timer

    # Scrape info from house pages concurrently
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [(executor.submit(scrape_house, url), counter(), reporting("Individual pages scraped:", counters), time.sleep(.2)) for url in houses_links]
        results =  [item[0].result() for item in futures]
        df = pd.DataFrame(results)
    
    # Export our dataset to a csv"
    csv_path = f'data_output/history/dataframe_{ds}.csv'
    df.to_csv(csv_path, index = True)

    end_time = time.time()  # Stop timer
    execution_time = end_time - start_time

    print("Scraping completed!                        ")
    print("Total time spent scraping:", execution_time, "seconds")
    # return df


