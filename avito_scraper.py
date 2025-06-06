import requests
from bs4 import BeautifulSoup
from threading import Thread, Lock
from queue import Queue
import time
import random
import csv
import os
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging


class RotatingSessionManager:
    """Manage multiple sessions with different headers to avoid detection"""
    def __init__(self, num_sessions=4):
        self.sessions = []
        self.current_session = 0
        self.lock = Lock()
        
        # Different user agents to rotate
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15'
        ]
        
        for i in range(num_sessions):
            session = requests.Session()
            session.headers.update({
                'User-Agent': user_agents[i % len(user_agents)],
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0'
            })
            self.sessions.append(session)
    
    def get_session(self):
        with self.lock:
            session = self.sessions[self.current_session]
            self.current_session = (self.current_session + 1) % len(self.sessions)
            return session


class SmartRateLimiter:
    """Adaptive rate limiting based on response times and status codes"""
    def __init__(self, base_delay=0.8, max_delay=6.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.current_delay = base_delay
        self.success_count = 0
        self.error_count = 0
        self.lock = Lock()
    
    def wait(self):
        delay = self.current_delay + random.uniform(0, 0.5)
        time.sleep(delay)
    
    def record_success(self):
        with self.lock:
            self.success_count += 1
            self.error_count = 0
            # Gradually decrease delay on continued success
            if self.success_count >= 15:
                self.current_delay = max(self.base_delay, self.current_delay * 0.9)
                self.success_count = 0
    
    def record_error(self, status_code=None):
        with self.lock:
            self.error_count += 1
            self.success_count = 0
            # Increase delay on errors
            if status_code == 429:  # Rate limited
                self.current_delay = min(self.max_delay, self.current_delay * 2)
                print(f"⚠️ Rate limited! Increasing delay to {self.current_delay:.1f}s")
            elif self.error_count >= 5:
                self.current_delay = min(self.max_delay, self.current_delay * 1.2)


class OptimizedAvitoScraper:
    def __init__(self, base_url, category_name, max_workers=7):
        self.base_url = base_url
        self.category_name = category_name
        self.filename = f"{category_name}_products_from_avito.csv"
        self.link_file = f"{self.category_name}_links.txt"
        self.progress_file = f"{self.category_name}_progress.txt"
        
        self.lock = Lock()
        self.session_manager = RotatingSessionManager(num_sessions=max_workers)
        self.rate_limiter = SmartRateLimiter(base_delay=0.8, max_delay=6.0)
        self.max_workers = max_workers
        self.product_count = 0
        self.processed_urls = set()
        
        # Load progress if exists
        self.load_progress()
        
        # Setup CSV
        if not os.path.exists(self.filename):
            with open(self.filename, "w", newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f,
                                        fieldnames=["Title", "Price", "Condition", "Description", "Post Date", "URL", "Scraped at"])
                writer.writeheader()

    def load_progress(self):
        """Load previously processed URLs to resume scraping"""
        if os.path.exists(self.progress_file):
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                self.processed_urls = set(line.strip() for line in f)
            print(f"📋 Loaded {len(self.processed_urls)} previously processed URLs")

    def save_progress(self, url):
        """Save processed URL to progress file"""
        with self.lock:
            if url not in self.processed_urls:
                self.processed_urls.add(url)
                with open(self.progress_file, 'a', encoding='utf-8') as f:
                    f.write(url + '\n')

    def make_request(self, url, retries=3):
        """Make HTTP request with rotation and error handling"""
        session = self.session_manager.get_session()
        
        for attempt in range(retries):
            try:
                self.rate_limiter.wait()
                response = session.get(url, timeout=15)
                
                if response.status_code == 200:
                    self.rate_limiter.record_success()
                    return response
                elif response.status_code == 429:
                    self.rate_limiter.record_error(429)
                    wait_time = (attempt + 1) * 10
                    print(f"⏳ Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    self.rate_limiter.record_error(response.status_code)
                    print(f"⚠️ HTTP {response.status_code} for {url}")
                    
            except requests.exceptions.RequestException as e:
                self.rate_limiter.record_error()
                print(f"⚠️ Request error (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(2, 5))
        
        return None

    def collect_links_optimized(self):
        """Optimized link collection with better performance"""
        print(f"\n🔎 Collecting links from {self.base_url}")
        
        page = 1
        total_links = 0
        collected_links = []

        while True:
            url = f"{self.base_url}?o={page}" if page > 1 else self.base_url
            print(f"📄 Loading page {page}: {url}")

            response = self.make_request(url)
            if not response:
                print(f"🚫 Failed to load page {page}")
                if page == 1:
                    print("❌ Could not load first page. Check internet connection or website availability.")
                break

            soup = BeautifulSoup(response.text, "html.parser")

            # Try multiple selectors for Avito links
            link_selectors = [
                "a.jZXrfL",
                "a[href*='/ordinateurs']",
                ".oan5vy-0 a",
                "a[data-testid='item-link']"
            ]

            items = []
            for selector in link_selectors:
                items = soup.select(selector)
                if items:
                    print(f"✓ Found {len(items)} items using selector: {selector}")
                    break

            if not items:
                print(f"⚠️ No items found on page {page}")
                break

            links = []
            for item in items:
                href = item.get('href')
                if href and ('/ordinateurs' in href or 'desktop' in href or 'bureau' in href):
                    full_url = "https://www.avito.ma" + href if href.startswith("/") else href
                    if full_url not in self.processed_urls:  # Skip already processed
                        links.append(full_url)

            if len(links) < 5 and page > 1:
                print(f"🔚 Too few new links ({len(links)}). Likely end of category. Stopping.")
                break

            collected_links.extend(links)
            total_links += len(links)

            print(f"→ Page {page}: {len(links)} new links collected. Total: {total_links}")
            page += 1

            # Reduced delay between pages
            time.sleep(random.uniform(1, 2))

        # Save links to file
        with open(self.link_file, "w", encoding='utf-8') as f:
            for url in collected_links:
                f.write(url + "\n")

        print(f"✅ Total new links collected: {total_links}")
        return collected_links

    def extract_description_improved(self, soup):
        """Your original description extraction logic - PRESERVED"""
        description = None

        # Strategy 1: Look for specific Avito description selectors first
        desc_selectors = [
            '[data-testid="ad-description"]',
            'div[data-testid="ad-description"]',
            '.sc-1g3sn3w-12',
            '.ad-description',
            'div[class*="description"]',
            'section[data-testid="ad-description"]',
            '[class*="AdDescription"]',
            '.description-content',
            'div.sc-ij98yj-0'
        ]

        for selector in desc_selectors:
            elements = soup.select(selector)
            for element in elements:
                desc_text = element.get_text(strip=True)
                if (len(desc_text) > 50 and
                        (any(emoji in desc_text for emoji in ['🔴', '📍', '⚡', '🔋', '✅', '🇲🇦', '📦', '🚚']) or
                         any(spec in desc_text.upper() for spec in
                             ['PROCESSEUR', 'RAM', 'SSD', 'ECRAN', 'PRIX', 'LIVRAISON', 'INTEL', 'AMD', 'NVIDIA']))):
                    description = desc_text
                    break
            if description:
                break

        # Strategy 2: Content analysis approach
        if not description:
            all_divs = soup.find_all(['div', 'section', 'article'])

            best_candidate = None
            best_score = 0

            for div in all_divs:
                div_text = div.get_text(strip=True)

                if len(div_text) < 50 or len(div_text) > 2000:
                    continue

                score = 0

                # Emoji indicators
                emoji_indicators = ['🔴', '📍', '⚡', '🔋', '✅', '🇲🇦', '📦', '🚚', '☎️']
                score += sum(2 for emoji in emoji_indicators if emoji in div_text)

                # Technical specifications
                tech_specs = ['PROCESSEUR', 'RAM', 'SSD', 'HDD', 'ECRAN', 'CARTE GRAPHIQUE', 'BATTERIE', 'INTEL', 'AMD',
                              'NVIDIA']
                score += sum(3 for spec in tech_specs if spec.upper() in div_text.upper())

                # Common computer terms
                computer_terms = ['RYZEN', 'CORE', 'DDR4', 'DDR5', 'NVME', 'FULL HD', '4K', 'IPS', 'WINDOWS', 'OFFICE']
                score += sum(1 for term in computer_terms if term.upper() in div_text.upper())

                # Price indicators
                if 'PRIX' in div_text.upper() or 'DH' in div_text.upper():
                    score += 2

                # Delivery/contact info
                if 'LIVRAISON' in div_text.upper() or 'CONTACTEZ' in div_text.upper():
                    score += 1

                # Penalize navigation elements
                nav_elements = ['ACCUEIL', 'SE CONNECTER', 'PUBLIER', 'TOUT LE MAROC', 'AVITO MARKET']
                score -= sum(5 for nav in nav_elements if nav.upper() in div_text.upper())

                if score > best_score and score >= 3:
                    best_score = score
                    best_candidate = div_text

            if best_candidate:
                description = best_candidate

        # Clean up description
        if description:
            description = re.sub(r'\s+', ' ', description)

            unwanted_patterns = [
                r'.*?Avito\.ma\s*',
                r'Auto neuf.*?Publier une annonce.*?',
                r'.*?INFORMATIQUE ET MULTIMEDIA.*?Ordinateurs.*?',
                r'.*?Accueil.*?Avito Market.*?'
            ]

            for pattern in unwanted_patterns:
                description = re.sub(pattern, '', description, flags=re.IGNORECASE | re.DOTALL)

            description = description.strip()

            if len(description) > 800:
                lines = description.split('.')
                core_lines = []
                for line in lines[:5]:
                    if len(line.strip()) > 10:
                        core_lines.append(line.strip())
                if core_lines:
                    description = '. '.join(core_lines) + '.'

        return description or "N/A"

    def extract_info(self, url):
        """Your original extraction logic - PRESERVED but optimized"""
        if url in self.processed_urls:
            return None
            
        response = self.make_request(url)
        if not response:
            print(f"⚠️ Failed to get {url}")
            return None

        try:
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract title - Your original logic
            try:
                title_element = soup.find("h1")
                title = title_element.text.strip() if title_element else "N/A"
            except:
                title = "N/A"

            # Extract price - Your original logic
            try:
                price_selectors = [
                    "p.sc-1x0vz2r-0.sc-1veij0r-10",
                    "[data-testid='price']",
                    ".price",
                    "span[class*='price']"
                ]
                price = "N/A"
                for selector in price_selectors:
                    price_tag = soup.select_one(selector)
                    if price_tag:
                        price = price_tag.text.strip().replace("\u202f", " ")
                        break
            except:
                price = "N/A"

            # Extract description using your improved method
            description = self.extract_description_improved(soup)

            # Extract post date - Your original logic
            try:
                post_date_tag = soup.select_one("span.iKguVF time")
                if post_date_tag and post_date_tag.has_attr("datetime"):
                    post_date = post_date_tag["datetime"]
                else:
                    post_date = "N/A"
            except:
                post_date = "N/A"

            # Extract condition - Your original logic
            try:
                condition = soup.select_one("div.kuofIS span.fjZBup")
                condition = condition.text.strip() if condition else "Not specified"
            except:
                condition = "Not specified"

            # Get current timestamp
            scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            self.product_count += 1
            if self.product_count % 25 == 0:
                print(f"✓ {self.product_count} products processed...")

            self.save_progress(url)
            
            return {
                "Title": title,
                "Price": price,
                "Condition": condition,
                "Description": description,
                "Post Date": post_date,
                "URL": url,
                "Scraped at": scraped_at
            }
            
        except Exception as e:
            print(f"❌ Error extracting {url}: {e}")
            return None

    def save_product_batch(self, products):
        """Save multiple products at once"""
        with self.lock:
            with open(self.filename, "a", newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f,
                                        fieldnames=["Title", "Price", "Condition", "Description", "Post Date", "URL", "Scraped at"])
                for product in products:
                    if product:  # Only save valid products
                        writer.writerow(product)

    def worker_batch(self, urls):
        """Process a batch of URLs with ThreadPoolExecutor"""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.extract_info, url): url for url in urls}
            
            for future in as_completed(future_to_url):
                result = future.result()
                if result:
                    results.append(result)
                    
                    # Save in batches of 10 for faster I/O
                    if len(results) >= 10:
                        self.save_product_batch(results)
                        results = []
        
        # Save remaining results
        if results:
            self.save_product_batch(results)

    def scrape_optimized(self):
        """Main optimized scraping method"""
        print(f"\n🚀 Starting OPTIMIZED scrape with {self.max_workers} workers...")
        print(f"📈 Expected to be 3-4x faster than original while preserving data quality")
        
        # Check for existing links
        if os.path.exists(self.link_file):
            print(f"📂 Loading existing links from {self.link_file}")
            with open(self.link_file, 'r', encoding='utf-8') as f:
                all_links = [line.strip() for line in f if line.strip() not in self.processed_urls]
        else:
            # Collect new links
            all_links = self.collect_links_optimized()
            if not all_links:
                print("❌ Failed to collect any links. Exiting.")
                return

        if not all_links:
            print("❌ No new links to process. Exiting.")
            return

        total_original_links = 0
        with open(self.link_file, 'r', encoding='utf-8') as f:
            total_original_links = len(f.readlines())
        
        print(f"📊 Total URLs to process: {len(all_links)}")
        print(f"📈 Already processed: {len(self.processed_urls)}")
        print(f"🎯 Total in file: {total_original_links}")
        print(f"📊 Progress: {len(self.processed_urls)}/{total_original_links} ({len(self.processed_urls)/total_original_links*100:.1f}%)")
        
        # Preview first product
        if all_links:
            print(f"\n👀 Testing extraction on first URL...")
            sample_result = self.extract_info(all_links[0])
            if sample_result and sample_result['Title'] != 'N/A':
                print(f"✅ Extraction working! Sample: {sample_result['Title'][:50]}...")
            else:
                print("⚠️ Extraction might have issues. Check selectors.")

        # Process in batches of 100 (back to faster processing)
        batch_size = 100
        for i in range(0, len(all_links), batch_size):
            batch = all_links[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(all_links) + batch_size - 1) // batch_size
            
            print(f"\n📦 Processing batch {batch_num}/{total_batches}: {len(batch)} URLs")
            start_time = time.time()
            self.worker_batch(batch)
            batch_time = time.time() - start_time
            print(f"⏱️ Batch completed in {batch_time:.1f}s ({len(batch)/batch_time:.1f} URLs/sec)")

        print(f"\n✅ Optimized scraping completed!")
        print(f"📊 Total products scraped: {self.product_count}")
        print(f"💾 Data saved to: {self.filename}")


if __name__ == "__main__":
    categories = {
        "desktop": "https://www.avito.ma/fr/maroc/ordinateurs_bureau-%C3%A0_vendre",
        "laptops": "https://www.avito.ma/fr/maroc/ordinateurs_portables-%C3%A0_vendre",
    }

    for name, url in categories.items():
        print(f"\n{'=' * 80}")
        print(f"🎯 Starting {name.upper()} category - BALANCED OPTIMIZED MODE")
        print(f"{'=' * 80}")

        scraper = OptimizedAvitoScraper(
            url,
            name,
            max_workers=7  # Increased back to 7 for speed
        )
        scraper.scrape_optimized()

        print(f"\n✅ Scraping completed for {name}.")
        print("⏳ Waiting 15 seconds before next category...")
        time.sleep(15)