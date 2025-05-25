import pandas as pd
import concurrent.futures
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    # Add these performance options
    chrome_options.page_load_strategy = 'eager'  # Don't wait for all resources
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-images')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def get_label_value(soup, label_text):
    try:
        # First attempt: direct label match
        label = soup.find("label", string=lambda s: s and label_text in s)
        if label and label.find_next("label"):
            return label.find_next("label").text.strip()
        
        # Second attempt: contains text search
        label = soup.find("label", string=lambda s: s and label_text.lower() in s.lower())
        if label and label.find_next("label"):
            return label.find_next("label").text.strip()
            
        # Third attempt: find by xpath-like approach with BeautifulSoup
        labels = soup.find_all("label")
        for lbl in labels:
            if label_text.lower() in lbl.text.lower():
                next_label = lbl.find_next("label")
                if next_label:
                    return next_label.text.strip()
    except Exception as e:
        print(f"Error extracting {label_text}: {str(e)}")
    return "N/A"

def process_project(url, idx, max_retries=2):
    for attempt in range(max_retries + 1):
        driver = setup_driver()
        try:
            print(f"\n Processing project {idx}: {url}")
            driver.get(url)
            
            # Wait for page to load with timeout
            wait = WebDriverWait(driver, 15)  # Increased timeout
            try:
                wait.until(EC.presence_of_element_located((By.XPATH, 
                    '//label[contains(text(), "RERA Registration No") or contains(text(), "RERA Regd. No")]')))
            except Exception:
                # Fallback to any label if specific one not found
                wait.until(EC.presence_of_element_located((By.TAG_NAME, 'label')))
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            project = {
                "Rera Regd. No": get_label_value(soup, "RERA Registration No"),
                "Project Name": get_label_value(soup, "Project Name")
            }

            # Try to click on Promoter Details tab
            try:
                # Try different possible tab selectors
                selectors = [
                    '//a[contains(text(), "Promoter Details")]',
                    '//a[contains(@href, "promoter")]',
                    '//li/a[contains(text(), "Promoter")]'
                ]
                
                for selector in selectors:
                    try:
                        promoter_tab = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                        driver.execute_script("arguments[0].click();", promoter_tab)
                        break
                    except Exception:
                        continue
                
                # Wait for promoter details to load
                wait.until(EC.presence_of_element_located((By.XPATH, 
                    '//label[contains(text(), "Company Name") or contains(text(), "Promoter Name")]')))
                # Get updated page source after tab click
                soup = BeautifulSoup(driver.page_source, "html.parser")
            except Exception as e:
                print(f" Could not open Promoter Details tab: {str(e)}")

            # Try multiple possible label texts for each field
            project["Promoter Name"] = (
                get_label_value(soup, "Company Name") if get_label_value(soup, "Company Name") != "N/A" 
                else get_label_value(soup, "Promoter Name")
            )
            project["Address of Promoter"] = (
                get_label_value(soup, "Registered Office Address") if get_label_value(soup, "Registered Office Address") != "N/A"
                else get_label_value(soup, "Address")
            )
            project["GST No"] = get_label_value(soup, "GST No.")
            
            return project
        except Exception as e:
            if attempt < max_retries:
                print(f" Attempt {attempt+1} failed for project {idx}. Retrying... Error: {str(e)}")
                driver.quit()
                continue
            else:
                print(f" All attempts failed for project {idx}. Error: {str(e)}")
                return {
                    "Rera Regd. No": "Error",
                    "Project Name": "Error",
                    "Promoter Name": "Error",
                    "Address of Promoter": "Error",
                    "GST No": "Error"
                }
        finally:
            driver.quit()

def scrape_rera_projects(limit=6):
    driver = setup_driver()
    try:
        print("Opening RERA projects list page...")
        driver.get("https://rera.odisha.gov.in/projects/project-list")
        
        # Wait for page to load with timeout
        wait = WebDriverWait(driver, 15)  # Increased timeout
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, '//a[contains(text(), "View Details")]')))
        except Exception as e:
            print(f"Error waiting for View Details links: {str(e)}")
            # Try JavaScript approach to find links
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            wait.until(EC.presence_of_element_located((By.TAG_NAME, 'a')))
        
        # Try multiple approaches to find the view details links
        view_details_buttons = []
        try:
            view_details_buttons = driver.find_elements(By.XPATH, '//a[contains(text(), "View Details")]')
        except Exception:
            try:
                view_details_buttons = driver.find_elements(By.XPATH, '//a[contains(@href, "/project/")]')
            except Exception as e:
                print(f"Failed to find project links: {str(e)}")
        
        if not view_details_buttons:
            print("No project links found. Check if the website structure has changed.")
            return
            
        # Limit the number of projects to scrape
        view_details_buttons = view_details_buttons[:limit]
        urls = []
        for button in view_details_buttons:
            try:
                urls.append(button.get_attribute("href"))
            except Exception as e:
                print(f"Error getting URL from button: {str(e)}")
                
        print(f"Found {len(urls)} project URLs to process")
    except Exception as e:
        print(f"Error during initial page scraping: {str(e)}")
        return
    finally:
        driver.quit()
    
    if not urls:
        print("No URLs found to process.")
        return
    
    # Process projects in parallel
    projects_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_to_url = {executor.submit(process_project, url, idx): (url, idx) 
                         for idx, url in enumerate(urls, start=1)}
        
        for future in concurrent.futures.as_completed(future_to_url):
            url, idx = future_to_url[future]
            try:
                project = future.result()
                if project:  # Only add if we got valid data
                    projects_data.append(project)
                    print(f"Successfully processed project {idx}")
            except Exception as e:
                print(f"\n Error processing project {idx} ({url}): {str(e)}")

    if not projects_data:
        print("No project data was successfully scraped.")
        return
        
    df = pd.DataFrame(projects_data)
    
    try:
        print("\nScraping Complete. Here are the results:\n")
        print(df.to_markdown(index=False))
    except Exception:
        print("\nScraping Complete. Here is the data frame:\n")
        print(df)

    try:
        df.to_csv("odisha_rera_projects.csv", index=False)
        print("\n Data saved to 'odisha_rera_projects.csv'.")
    except Exception as e:
        print(f"Error saving CSV file: {str(e)}")

if __name__ == "__main__":
    try:
        import tabulate  # For to_markdown() function
    except ImportError:
        print("Installing tabulate for better output formatting...")
        import subprocess
        subprocess.check_call(["pip", "install", "tabulate"])
        
    try:
        print("Starting Odisha RERA scraper...")
        scrape_rera_projects()
    except Exception as e:
        print(f"Critical error in main execution: {str(e)}")
        print("Please check your internet connection and try again.")
