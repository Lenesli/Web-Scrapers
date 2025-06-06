# Web Scraper Collection 

A collection of optimized web scrapers for e-commerce platforms with multi-threading, rate limiting, and robust error handling.

## 📁 Files Overview

### 1. `avito_scraper.py` - Advanced Avito Scraper
- **Target**: Avito.ma (Morocco's largest marketplace)
- **Features**: Smart rate limiting, session rotation, progress tracking, resume capability
- **Anti-Bot Protection**: Works perfectly against Avito's anti-bot detection system
- **Performance**: 3-4x faster than basic scrapers with 7 concurrent workers
- **Data**: Computers (Desktop & Laptops) with detailed descriptions

### 2. `jumia_scraper.py` - Fast Jumia Scraper  
- **Target**: Jumia.ma (Leading e-commerce platform)
- **Features**: High-speed concurrent scraping, clean data extraction
- **Performance**: 8 concurrent threads for maximum speed
- **Data**: Computer products with specifications

## 🚀 Quick Start

### Prerequisites
```bash
pip install requests beautifulsoup4 lxml
```

### Run the Scrapers
```bash
# Avito Scraper (Balanced speed + reliability)
python avito_scraper.py

# Jumia Scraper (Maximum speed)
python jumia_scraper.py
```

## 📊 Output Files

Both scrapers generate CSV files with the following structure:
- **Title**: Product name
- **Price**: Product price
- **Condition**: New/Used status
- **Description**: Detailed product description
- **Post Date**: When the product was posted
- **URL**: Direct link to the product
- **Scraped at**: Timestamp of when data was collected

## 🔧 Adding New Categories & URLs

### For Avito Scraper

Edit the `categories` dictionary in `avito_scraper.py`:

```python
categories = {
    "desktop": "https://www.avito.ma/fr/maroc/ordinateurs_bureau-à_vendre",
    "laptops": "https://www.avito.ma/fr/maroc/ordinateurs_portables-à_vendre",
    
    # ADD YOUR CATEGORIES HERE:
    "phones": "https://www.avito.ma/fr/maroc/téléphones-à_vendre",
    "tablets": "https://www.avito.ma/fr/maroc/tablettes-à_vendre",
    "cars": "https://www.avito.ma/fr/maroc/voitures-à_vendre",
    "furniture": "https://www.avito.ma/fr/maroc/meubles-à_vendre"
}
```

### For Jumia Scraper

Edit the `categories` dictionary in `jumia_scraper.py`:

```python
categories = {
    "computers": "https://www.jumia.ma/ordinateurs-pc/",
    
    # ADD YOUR CATEGORIES HERE:
    "phones": "https://www.jumia.ma/telephones/",
    "tablets": "https://www.jumia.ma/tablettes/",
    "electronics": "https://www.jumia.ma/electronique/",
    "fashion": "https://www.jumia.ma/mode/"
}
```

## ⚙️ Key Features

### Avito Scraper
- **Smart Rate Limiting**: Automatically adjusts delays based on website response
- **Session Rotation**: Uses multiple browser sessions to avoid detection
- **Anti-Bot Bypass**: Successfully bypasses Avito's anti-bot protection systems
- **Progress Tracking**: Resume scraping from where you left off
- **Error Recovery**: Handles network issues and rate limiting gracefully

### Jumia Scraper
- **High-Speed Processing**: 8 concurrent threads for maximum performance
- **Clean Data Extraction**: Removes formatting issues and handles CSV properly
- **Preview Mode**: Tests extraction before full scrape


---
