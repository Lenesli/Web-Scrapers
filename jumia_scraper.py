import requests 
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

baseurl = 'https://www.jumia.ma/'

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
}

categories = {
    "computers": "https://www.jumia.ma/ordinateurs-pc/",
    # Add more categories here as needed
}

# Thread lock for CSV writing
csv_lock = Lock()

def write_header_if_needed(file_path):
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as f:
            if f.readline():
                return  # File not empty, assume header present
    except FileNotFoundError:
        pass
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, 
                              fieldnames=["Title", "Price", "Condition", "Description", "Post Date", "URL", "Scraped at"],
                              quoting=csv.QUOTE_ALL)  # Quote all fields in header too
        writer.writeheader()

def scrape_single_product(link, csv_file, preview=False):
    """Scrape a single product with error handling"""
    try:
        r = requests.get(link, headers=headers, timeout=10)
        soup = BeautifulSoup(r.content, 'html.parser')

        title_elem = soup.find('h1', class_='-fs20 -pts -pbxs')
        title = title_elem.text.strip() if title_elem else "N/A"
        
        price_elem = soup.find('span', class_='-b -ubpt -tal -fs24 -prxs')
        price = price_elem.text.strip() if price_elem else "N/A"
        
        div = soup.find('div', class_='markup -pam')
        if div:
            # Clean description: remove line breaks and extra spaces
            description = div.get_text(separator=' ', strip=True)
            description = ' '.join(description.split())  # Remove all line breaks and extra spaces
            description = description.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
            # Limit description length to avoid extremely long text
            if len(description) > 500:
                description = description[:500] + "..."
        else:
            description = "N/A"
        
        post_date = datetime.now().strftime("%Y-%m-%d")
        scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        product_data = {
            "Title": title.replace('\n', ' ').replace('\r', ' '),  # Clean title too
            "Price": price.replace('\n', ' ').replace('\r', ' '),  # Clean price
            "Condition": "New",
            "Description": description,
            "Post Date": post_date,
            "URL": link,
            "Scraped at": scraped_at
        }

        if preview:
            print(f"\n📝 PREVIEW - First Product:")
            print(f"Title: {product_data['Title']}")
            print(f"Price: {product_data['Price']}")
            print(f"Condition: New")
            print(f"Description: {product_data['Description'][:200]}..." if len(product_data['Description']) > 200 else f"Description: {product_data['Description']}")
            print(f"Post Date: {post_date}")
            print(f"URL: {link}")
            print(f"Scraped at: {scraped_at}")
            print("-" * 80)
            return product_data

        # Thread-safe CSV writing with proper quoting
        with csv_lock:
            with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, 
                                      fieldnames=["Title", "Price", "Condition", "Description", "Post Date", "URL", "Scraped at"],
                                      quoting=csv.QUOTE_ALL)  # Quote all fields to handle commas
                writer.writerow(product_data)
        
        return product_data
        
    except Exception as e:
        print(f"❌ Error scraping {link}: {e}")
        return None

def scrape_category(category_name, category_url):
    csv_file = f'{category_name}_products_from_jumia.csv'

    write_header_if_needed(csv_file)
    
    productlinks = []
    
    # Collect product links (faster with no delays)
    page = 1
    while True:
        print(f"📄 Collecting links from page {page}...")
        r = requests.get(f'{category_url}?page={page}#catalog-listing', headers=headers)
        soup = BeautifulSoup(r.content, 'html.parser')
        productlist = soup.find_all('article', class_='prd _fb col c-prd')

        if not productlist:
            print(f"✅ No more products found on page {page}. Total pages: {page-1}")
            break

        page_links = []
        for item in productlist:
            for link in item.find_all('a', class_='core', href=True):
                page_links.append(baseurl + link['href'])
        
        productlinks.extend(page_links)
        print(f"→ Page {page}: {len(page_links)} products found. Total: {len(productlinks)}")
        page += 1

    print(f"\n🎯 Total links collected: {len(productlinks)}")

    if not productlinks:
        print("❌ No products found!")
        return

    # Preview first product
    print("\n👀 Testing scraping on first product...")
    first_product = scrape_single_product(productlinks[0], csv_file, preview=True)
    
    if not first_product or first_product['Title'] == 'N/A':
        print("⚠️ Preview failed! Check selectors.")
        return

    print("✅ Scraping looks good! Starting full scrape...")
    
    # Fast concurrent scraping
    successful_scrapes = 0
    
    with ThreadPoolExecutor(max_workers=8) as executor:  # 8 concurrent threads
        # Submit all scraping tasks
        future_to_url = {
            executor.submit(scrape_single_product, link, csv_file): link 
            for link in productlinks[1:]  # Skip first one (already previewed)
        }
        
        # Process results as they complete
        for future in as_completed(future_to_url):
            result = future.result()
            if result:
                successful_scrapes += 1
                if successful_scrapes % 25 == 0:
                    print(f"✓ {successful_scrapes + 1}/{len(productlinks)} products scraped...")
    
    print(f"\n🎉 Scraping completed!")
    print(f"📊 Successfully scraped: {successful_scrapes + 1}/{len(productlinks)} products")
    print(f"💾 Data saved to: {csv_file}")

if __name__ == "__main__":
    for category_name, category_url in categories.items():
        print(f"\n{'='*80}")
        print(f"🎯 Starting {category_name.upper()} category - FAST MODE")
        print(f"🚀 Using 8 concurrent threads (no delays)")
        print(f"{'='*80}")
        
        scrape_category(category_name, category_url)
        
        print(f"\n✅ Completed scraping {category_name}")
        if len(categories) > 1:
            print("⏳ Waiting 3 seconds before next category...")
            time.sleep(3)