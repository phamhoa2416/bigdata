# import time
# import logging
# from datetime import datetime
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from typing import List, Dict, Optional
#
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
#
#
# class TopCVScraper:
#     def __init__(self):
#         self.setup_webdriver()
#
#     def setup_webdriver(self):
#         chrome_options = Options()
#         chrome_options.add_argument('--headless')
#         chrome_options.add_argument('--no-sandbox')
#         chrome_options.add_argument('--disable-dev-shm-usage')
#         chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
#         chrome_options.add_argument('--disable-blink-features=AutomationControlled')
#         chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
#         chrome_options.add_experimental_option('useAutomationExtension', False)
#
#         self.driver = webdriver.Chrome(options=chrome_options)
#         self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
#         self.wait = WebDriverWait(self.driver, 15)
#         logger.info("WebDriver initialized successfully")
#
#     def scrape_job_listings(self, search_query: str = "python developer", max_pages: int = 3) -> List[Dict]:
#         base_url = "https://www.topcv.vn"
#         search_url = f"{base_url}/tim-viec-lam-{search_query.replace(' ', '-')}"
#
#         jobs = []
#         try:
#             logger.info(f"Navigating to: {search_url}")
#             self.driver.get(search_url)
#             time.sleep(3)
#
#             # Check if page loaded successfully
#             if "404" in self.driver.title or "Không tìm thấy" in self.driver.page_source:
#                 logger.warning(f"No results found for query: {search_query}")
#                 return jobs
#
#             for page in range(1, max_pages + 1):
#                 logger.info(f"Scraping page {page} for '{search_query}'")
#
#                 # Wait for job listings to load
#                 try:
#                     self.wait.until(
#                         EC.presence_of_element_located((By.CLASS_NAME, "job-item"))
#                     )
#                 except Exception as e:
#                     logger.warning(f"No job items found on page {page}: {e}")
#                     break
#
#                 # Find all job items
#                 job_elements = self.driver.find_elements(By.CLASS_NAME, "job-item")
#                 logger.info(f"Found {len(job_elements)} job elements on page {page}")
#
#                 for index, job_element in enumerate(job_elements):
#                     try:
#                         job_data = self.extract_job_data(job_element)
#                         if job_data:
#                             jobs.append(job_data)
#                             logger.info(f"Extracted job {index + 1}: {job_data['title']}")
#                     except Exception as e:
#                         logger.error(f"Error extracting job data at index {index}: {e}")
#                         continue
#
#                 # Try to go to next page
#                 if page < max_pages:
#                     next_page_success = self.go_to_next_page()
#                     if not next_page_success:
#                         logger.info("No more pages available")
#                         break
#
#         except Exception as e:
#             logger.error(f"Error during scraping for '{search_query}': {e}")
#
#         return jobs
#
#     def go_to_next_page(self) -> bool:
#         try:
#             next_buttons = self.driver.find_elements(By.CSS_SELECTOR,
#                                                      ".pagination .next a, .pagination li.active + li a")
#
#             for button in next_buttons:
#                 if button.is_displayed() and button.is_enabled():
#                     self.driver.execute_script("arguments[0].click();", button)
#                     time.sleep(3)
#                     return True
#
#             page_links = self.driver.find_elements(By.CSS_SELECTOR, ".pagination a")
#             current_active = self.driver.find_element(By.CSS_SELECTOR, ".pagination li.active")
#             current_page = int(current_active.text) if current_active.text.isdigit() else 0
#
#             for link in page_links:
#                 if link.text.isdigit() and int(link.text) == current_page + 1:
#                     self.driver.execute_script("arguments[0].click();", link)
#                     time.sleep(3)
#                     return True
#
#             return False
#
#         except Exception as e:
#             logger.error(f"Error navigating to next page: {e}")
#             return False
#
#     def extract_job_data(self, job_element) -> Optional[Dict]:
#         try:
#             title_element = job_element.find_element(By.CSS_SELECTOR, ".title a")
#             title = title_element.text.strip()
#             job_url = title_element.get_attribute("href")
#
#             # Extract company
#             company = "Unknown"
#             try:
#                 company_element = job_element.find_element(By.CSS_SELECTOR, ".company")
#                 company = company_element.text.strip()
#             except:
#                 logger.warning("Could not extract company name")
#
#             # Extract location
#             location = "Unknown"
#             try:
#                 location_element = job_element.find_element(By.CSS_SELECTOR, ".location")
#                 location = location_element.text.strip()
#             except:
#                 logger.warning("Could not extract location")
#
#             # Extract salary
#             salary = "Not specified"
#             try:
#                 salary_element = job_element.find_element(By.CSS_SELECTOR, ".salary")
#                 salary = salary_element.text.strip()
#             except:
#                 logger.warning("Could not extract salary")
#
#             # Extract posted time
#             posted_time = "Unknown"
#             try:
#                 time_element = job_element.find_element(By.CSS_SELECTOR, ".time")
#                 posted_time = time_element.text.strip()
#             except:
#                 logger.warning("Could not extract posted time")
#
#             job_data = {
#                 'title': title,
#                 'company': company,
#                 'location': location,
#                 'salary': salary,
#                 'posted_time': posted_time,
#                 'job_url': job_url,
#                 'scraped_at': datetime.now().isoformat(),
#                 'source': 'topcv'
#             }
#
#             return job_data
#
#         except Exception as e:
#             logger.error(f"Error extracting job details: {e}")
#             return None
#
#     def scrape_multiple_queries(self, search_queries: List[str], max_pages: int = 2) -> List[Dict]:
#         all_jobs = []
#
#         for query in search_queries:
#             logger.info(f"Scraping jobs for: {query}")
#             jobs = self.scrape_job_listings(search_query=query, max_pages=max_pages)
#             all_jobs.extend(jobs)
#             logger.info(f"Scraped {len(jobs)} jobs for '{query}'")
#
#             # Be respectful to the website
#             time.sleep(5)
#
#         return all_jobs
#
#     def close(self):
#         """Cleanup resources"""
#         if hasattr(self, 'driver'):
#             self.driver.quit()
#             logger.info("WebDriver closed")
#
#     def __enter__(self):
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.close()

import time
import json
import csv
import random
import logging
from datetime import datetime
from typing import List, Dict, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TopCVScraper")


class TopCVScraper:
    """
    Scraper lấy toàn bộ danh sách việc làm từ TopCV (https://www.topcv.vn/viec-lam)
    """

    def __init__(self, headless: bool = True, delay_range=(3, 6)):
        self.headless = headless
        self.delay_range = delay_range
        self.setup_webdriver()

    # ----------------------------------------------------
    def setup_webdriver(self):
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.wait = WebDriverWait(self.driver, 15)
        logger.info("✅ WebDriver initialized successfully")

    # ----------------------------------------------------
    def scrape_all_jobs(self, max_pages: int = 100) -> List[Dict]:
        """
        Lấy toàn bộ danh sách việc làm trên TopCV.
        Duyệt qua từng trang /viec-lam?page=N
        """
        base_url = "https://www.topcv.vn/viec-lam"
        all_jobs = []

        for page in range(1, max_pages + 1):
            try:
                page_url = f"{base_url}?page={page}"
                logger.info(f"🔎 Scraping page {page}: {page_url}")

                self.driver.get(page_url)
                time.sleep(random.uniform(*self.delay_range))

                # Kiểm tra nếu không còn dữ liệu
                if "404" in self.driver.title or "Không tìm thấy" in self.driver.page_source:
                    logger.warning(f"No more jobs found at page {page}")
                    break

                # Chờ danh sách job tải xong
                self.wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "job-item"))
                )

                job_elements = self.driver.find_elements(By.CLASS_NAME, "job-item")
                logger.info(f"Found {len(job_elements)} jobs on page {page}")

                if not job_elements:
                    logger.info(f"No job items on page {page}, stopping.")
                    break

                for index, job_element in enumerate(job_elements):
                    try:
                        job_data = self.extract_job_data(job_element)
                        if job_data:
                            all_jobs.append(job_data)
                    except Exception as e:
                        logger.error(f"Error extracting job #{index}: {e}")
                        continue

                # Nếu trang có ít job => có thể hết dữ liệu
                if len(job_elements) < 10:
                    logger.info(f"Seems like last page reached ({page}).")
                    break

                # Delay giữa các trang
                time.sleep(random.uniform(*self.delay_range))

            except Exception as e:
                logger.error(f"Error scraping page {page}: {e}")
                continue

        logger.info(f"✅ Completed scraping. Total jobs collected: {len(all_jobs)}")
        return all_jobs

    # ----------------------------------------------------
    def extract_job_data(self, job_element) -> Optional[Dict]:
        """Trích xuất dữ liệu từ một job-item"""
        try:
            title_element = job_element.find_element(By.CSS_SELECTOR, ".title a")
            title = title_element.text.strip()
            job_url = title_element.get_attribute("href")

            # Company
            company = "Unknown"
            try:
                company_element = job_element.find_element(By.CSS_SELECTOR, ".company")
                company = company_element.text.strip()
            except:
                pass

            # Location
            location = "Unknown"
            try:
                location_element = job_element.find_element(By.CSS_SELECTOR, ".location, .address")
                location = location_element.text.strip()
            except:
                pass

            # Salary
            salary = "Not specified"
            try:
                salary_element = job_element.find_element(By.CSS_SELECTOR, ".salary")
                salary = salary_element.text.strip()
            except:
                pass

            # Time posted
            posted_time = "Unknown"
            try:
                time_element = job_element.find_element(By.CSS_SELECTOR, ".time, .job-deadline, .updated-at")
                posted_time = time_element.text.strip()
            except:
                pass

            return {
                "title": title,
                "company": company,
                "location": location,
                "salary": salary,
                "posted_time": posted_time,
                "job_url": job_url,
                "scraped_at": datetime.now().isoformat(),
                "source": "topcv",
            }

        except Exception as e:
            logger.error(f"Error extracting job details: {e}")
            return None

    # ----------------------------------------------------
    def save_to_json(self, jobs: List[Dict], filename: str = "topcv_jobs.json"):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(jobs, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Saved {len(jobs)} jobs to {filename}")

    # ----------------------------------------------------
    def save_to_csv(self, jobs: List[Dict], filename: str = "topcv_jobs.csv"):
        if not jobs:
            logger.warning("No jobs to save.")
            return

        keys = jobs[0].keys()
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(jobs)
        logger.info(f"💾 Saved {len(jobs)} jobs to {filename}")

    # ----------------------------------------------------
    def close(self):
        if hasattr(self, "driver"):
            self.driver.quit()
            logger.info("🧹 WebDriver closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ------------------- Main usage example -------------------
if __name__ == "__main__":
    with TopCVScraper(headless=True) as scraper:
        jobs = scraper.scrape_all_jobs(max_pages=100)
        scraper.save_to_json(jobs)
        scraper.save_to_csv(jobs)
