"""
Enhanced Selenium scraper for VAHAN PrimeFaces dropdowns
Handles dynamic IDs that change daily for state dropdown and refresh button.
Fixed header parsing for complex table structures.
"""
import time
import re
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException
from bs4 import BeautifulSoup

class VahanScraper:
    """Enhanced scraper for VAHAN dashboard that handles PrimeFaces dropdown interactions with dynamic IDs"""
    
    # Static dropdown IDs (these don't change)
    static_dropdowns = {
        "Y-Axis": "yaxisVar",
        "X-Axis": "xaxisVar", 
        "Year": "selectedYear",
        "Year Type": "selectedYearType",
        "Vehicle Type": "vchgroupTable:selectCatgGrp"
    }
    
    def __init__(self, base_url, wait_time=15):
        self.base_url = base_url
        self.wait_time = wait_time
        self.driver = None
        self.wait = None
        self.dynamic_state_id = None
        self.dynamic_refresh_id = None
        self.scraped_data = []
        
    def setup_driver(self, headless: bool = True) -> None:
        """Initialize the Chrome WebDriver with enhanced options."""
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
        
        # Enhanced options for better compatibility
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.wait = WebDriverWait(self.driver, self.wait_time)
        
    def _detect_dynamic_ids(self) -> None:
        """Detect the dynamic IDs for state dropdown and refresh button."""
        try:
            # Find state dropdown by looking for the select with state options
            state_selects = self.driver.find_elements(
                By.XPATH, 
                "//select[option[contains(@data-escape, 'true') and (contains(text(), 'Karnataka') or contains(text(), 'Delhi') or contains(text(), 'Maharashtra'))]]"
            )
            
            if state_selects:
                state_select = state_selects[0]
                state_select_id = state_select.get_attribute('id')
                if state_select_id and state_select_id.endswith('_input'):
                    self.dynamic_state_id = state_select_id[:-6]
                    print(f"✓ Detected state dropdown ID: {self.dynamic_state_id}")
                else:
                    parent_div = state_select.find_element(By.XPATH, "..")
                    while parent_div and not parent_div.get_attribute('class').startswith('ui-selectonemenu'):
                        parent_div = parent_div.find_element(By.XPATH, "..")
                    if parent_div:
                        self.dynamic_state_id = parent_div.get_attribute('id')
                        print(f"✓ Detected state dropdown ID: {self.dynamic_state_id}")
            
            if not self.dynamic_state_id:
                state_divs = self.driver.find_elements(
                    By.XPATH,
                    "//div[contains(@class, 'ui-selectonemenu')]/label[contains(text(), 'States') or contains(text(), 'Vahan4')]/.."
                )
                if state_divs:
                    self.dynamic_state_id = state_divs[0].get_attribute('id')
                    print(f"✓ Detected state dropdown ID (method 2): {self.dynamic_state_id}")
            
            # Find refresh button
            refresh_buttons = self.driver.find_elements(
                By.XPATH,
                "//button[contains(@class, 'ui-button') and span[contains(text(), 'Refresh')]]"
            )
            
            if refresh_buttons:
                self.dynamic_refresh_id = refresh_buttons[0].get_attribute('id')
                print(f"✓ Detected refresh button ID: {self.dynamic_refresh_id}")
            else:
                refresh_buttons = self.driver.find_elements(
                    By.XPATH,
                    "//button[contains(text(), 'Refresh')] | //input[@type='submit' and contains(@value, 'Refresh')]"
                )
                if refresh_buttons:
                    self.dynamic_refresh_id = refresh_buttons[0].get_attribute('id')
                    print(f"✓ Detected refresh button ID (fallback 1): {self.dynamic_refresh_id}")
                else:
                    potential_buttons = self.driver.find_elements(
                        By.XPATH,
                        "//button[starts-with(@id, 'j_idt')] | //input[@type='submit' and starts-with(@id, 'j_idt')]"
                    )
                    if potential_buttons:
                        for btn in potential_buttons:
                            try:
                                if 'refresh' in btn.get_attribute('onclick').lower():
                                    self.dynamic_refresh_id = btn.get_attribute('id')
                                    print(f"✓ Detected refresh button ID (by onclick): {self.dynamic_refresh_id}")
                                    break
                            except:
                                continue
                        
                        if not self.dynamic_refresh_id and potential_buttons:
                            self.dynamic_refresh_id = potential_buttons[-1].get_attribute('id')
                            print(f"✓ Detected refresh button ID (last j_idt button): {self.dynamic_refresh_id}")
                            
        except Exception as e:
            print(f"Warning: Could not detect dynamic IDs: {e}")

    def open_page(self) -> None:
        """Open the VAHAN dashboard and wait for it to load completely."""
        self.driver.get(self.base_url)
        self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(3)
        self._detect_dynamic_ids()

    def _close_all_open_panels(self) -> None:
        """Close any open dropdown panels to prevent click interception."""
        try:
            open_panels = self.driver.find_elements(
                By.CSS_SELECTOR, 
                "div[id$='_panel'].ui-selectonemenu-panel[style*='display: block'], "
                "div[id$='_panel'].ui-selectonemenu-panel:not([style*='display: none'])"
            )
            
            for panel in open_panels:
                try:
                    body = self.driver.find_element(By.TAG_NAME, "body")
                    ActionChains(self.driver).move_to_element(body).click().perform()
                    time.sleep(0.5)
                except:
                    continue
        except Exception:
            try:
                body = self.driver.find_element(By.TAG_NAME, "body")
                body.click()
                time.sleep(1)
            except:
                pass

    def _parse_complex_table_headers(self) -> list[str]:
        """Parse complex table headers with proper handling of colspan/rowspan."""
        try:
            table_element = self.driver.find_element(By.CSS_SELECTOR, "#combTablePnl table")
            table_html = table_element.get_attribute('outerHTML')
            soup = BeautifulSoup(table_html, 'html.parser')
            
            thead = soup.find('thead', id='vchgroupTable_head')
            if not thead:
                thead = soup.find('thead')
            
            if not thead:
                print("⚠️ No thead found, falling back to simple header parsing")
                return self._get_simple_headers()
            
            header_rows = thead.find_all('tr', role='row')
            print(f"Found {len(header_rows)} header rows")
            
            final_headers = []
            
            if len(header_rows) >= 3:
                first_row = header_rows[0]
                first_row_cells = first_row.find_all('th')
                
                for cell in first_row_cells[:2]:
                    text = cell.get_text(strip=True)
                    text = re.sub(r'\s+', ' ', text)
                    if 'S No' in text:
                        final_headers.append('S No')
                    elif 'Vehicle Class' in text:
                        final_headers.append('Vehicle Class')
                
                last_row = header_rows[-1]
                category_cells = last_row.find_all('th')
                
                for cell in category_cells:
                    text = cell.get_text(strip=True)
                    text = re.sub(r'\s+', ' ', text)
                    
                    if text and text not in ['S No', 'Vehicle Class', '']:
                        if text in ['2WIC', '2WN', '2WT', 'TOTAL', '3WN', '3WT', 'LMV', 'MMV', 'HMV', 'LGV', 'MGV', 'HGV']:
                            final_headers.append(text.strip())
                        elif 'TOTAL' in text.upper():
                            final_headers.append('TOTAL')
                        elif len(text) <= 10:
                            final_headers.append(text)
            else:
                if header_rows:
                    last_header_row = header_rows[-1]
                    header_cells = last_header_row.find_all(['th', 'td'])
                    
                    for cell in header_cells:
                        text = cell.get_text(strip=True)
                        text = re.sub(r'\s+', ' ', text)
                        
                        if text and text not in ['', ' ']:
                            if 'S No' in text:
                                final_headers.append('S No')
                            elif 'Vehicle Class' in text:
                                final_headers.append('Vehicle Class')
                            elif text in ['2WIC', '2WN', '2WT', 'TOTAL', '3WN', '3WT', 'LMV', 'MMV', 'HMV']:
                                final_headers.append(text.strip())
                            else:
                                final_headers.append(text)
            
            print(f"✓ Parsed complex headers ({len(final_headers)} columns): {final_headers}")
            return final_headers
            
        except Exception as e:
            print(f"⚠️ Error parsing complex headers: {e}")
            return self._get_simple_headers()

    def _get_simple_headers(self) -> list[str]:
        """Simple fallback method for header extraction"""
        try:
            header_elements = self.driver.find_elements(By.CSS_SELECTOR, "#combTablePnl thead th")
            headers = []
            for h in header_elements:
                text = h.text.strip()
                if text and text not in headers:
                    headers.append(text)
            return headers
        except:
            return ['S No', 'Vehicle Class', 'Category 1', 'Category 2', 'Category 3', 'Total']

    def _fetch_one_menu_items(self, base_id: str, max_retries: int = 3) -> list[str]:
        """Fetch items from a PrimeFaces selectOneMenu with retry logic."""
        if base_id.endswith("_input"):
            base_id = base_id[:-6]
            
        for attempt in range(max_retries):
            try:
                self._close_all_open_panels()
                
                if attempt > 0:
                    time.sleep(2)
                    
                dropdown_selector = f"{base_id}"
                print(f"Processing dropdown: {dropdown_selector}")
                dropdown = self.wait.until(
                    EC.presence_of_element_located((By.ID, dropdown_selector))
                )
                
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", dropdown)
                time.sleep(1)
                
                click_successful = False
                
                try:
                    self.wait.until(EC.element_to_be_clickable((By.ID, dropdown_selector)))
                    dropdown.click()
                    click_successful = True
                except ElementClickInterceptedException:
                    pass
                    
                if not click_successful:
                    if attempt == max_retries - 1:
                        raise Exception(f"Could not click dropdown after {max_retries} attempts")
                    continue
                
                panel_selector = f"{base_id}_panel"
                panel = self.wait.until(
                    EC.visibility_of_element_located((By.ID, panel_selector))
                )
                
                items = []
                li_elements = panel.find_elements(By.CSS_SELECTOR, "li.ui-selectonemenu-item")
                
                for li in li_elements:
                    text = li.text.strip()
                    if text and text not in items:
                        items.append(text)
                
                try:
                    dropdown.click()
                except:
                    body = self.driver.find_element(By.TAG_NAME, "body")
                    body.click()
                
                return items
                
            except TimeoutException as e:
                if attempt == max_retries - 1:
                    return [f"⚠️ Timeout: Panel not found for {base_id}"]
                continue
                
            except Exception as e:
                if attempt == max_retries - 1:
                    return [f"⚠️ Error (attempt {attempt + 1}): {str(e)[:100]}..."]
                continue
                
        return [f"⚠️ Failed after {max_retries} attempts"]

    @property
    def dropdowns(self):
        """Get dropdown mapping including dynamically detected state ID."""
        dropdown_map = self.static_dropdowns.copy()
        if self.dynamic_state_id:
            dropdown_map["State"] = self.dynamic_state_id
        return dropdown_map

    def fetch_data(self) -> dict:
        """Fetch and parse data from the VAHAN dashboard table."""
        try:
            self._close_all_open_panels()
            self.wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "#combTablePnl table"))
            )
            
            headers = self._parse_complex_table_headers()
            data = []
            row_elements = self.driver.find_elements(By.CSS_SELECTOR, "#combTablePnl tbody tr")
            
            for row in row_elements:
                cells = [cell.text.strip() for cell in row.find_elements(By.TAG_NAME, "td")]
                if any(cells):
                    data.append(cells)
            
            print(f"📊 Table found - Headers: {len(headers)}, Data rows: {len(data)}")
            print(f"Headers: {headers}")
            
            for i, row in enumerate(data[:5]):
                print(f"Row {i+1}: {row}")
            if len(data) > 5:
                print(f"... and {len(data) - 5} more rows")
            
            return {
                "headers": headers,
                "rows": data,
                "status": "success",
                "total_rows": len(data)
            }
            
        except TimeoutException:
            print("⚠️ Timeout waiting for table to load")
            return {"headers": [], "rows": [], "status": "timeout"}
        except Exception as e:
            print(f"⚠️ Error fetching data: {e}")
            return {"headers": [], "rows": [], "status": "error", "error": str(e)}

    def apply_filters(self, filters: dict[str, str]) -> dict:
        """Apply filters to the VAHAN dashboard."""
        dropdown_map = self.dropdowns
        
        for label, value in filters.items():
            if label not in dropdown_map:
                print(f"Unknown filter: {label}")
                continue
                
            widget_id = dropdown_map[label]
            dropdown_css = f"{widget_id}"
            panel_css = f"{widget_id}_panel"
            item_xpath = f".//li[contains(@class,'ui-selectonemenu-item') and normalize-space(text())='{value}']"
            
            try:
                self._close_all_open_panels()
                
                dropdown_div = self.wait.until(
                    EC.element_to_be_clickable((By.ID, dropdown_css))
                )
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});",
                    dropdown_div
                )
                dropdown_div.click()
                
                panel = self.wait.until(
                    EC.visibility_of_element_located((By.ID, panel_css))
                )
                
                option = panel.find_element(By.XPATH, item_xpath)
                option.click()
                
                print(f"Selected {label}: {value}")
                time.sleep(1)
                
            except Exception as e:
                print(f"Error applying filter {label}: {e}")
        
        return self._click_refresh_button()

    def _click_refresh_button(self) -> dict:
        """Click the refresh button using the dynamically detected ID."""
        try:
            if self.dynamic_refresh_id:
                refresh_button = self.wait.until(
                    EC.element_to_be_clickable((By.ID, self.dynamic_refresh_id))
                )
                print(f"✅ Refresh button clicked (ID: {self.dynamic_refresh_id})")
            else:
                refresh_buttons = self.driver.find_elements(
                    By.XPATH,
                    "//input[@type='submit' and contains(@value, 'Refresh')] | //button[contains(text(), 'Refresh')]"
                )
                if refresh_buttons:
                    print("✅ Refresh button clicked (fallback method)")
                else:
                    print("⚠️ Could not find refresh button")
                    return self.fetch_data()
            
            print("⏳ Waiting for data to load...")
            time.sleep(5)
            
            try:
                self.wait.until(
                    EC.presence_of_element_located((By.ID, "combTablePnl"))
                )
                return self.fetch_data()
            except TimeoutException:
                print("⚠️ Table didn't load within timeout period")
                return {"headers": [], "rows": [], "status": "timeout"}
                
        except Exception as e:
            print(f"Error clicking refresh: {e}")
            return {"headers": [], "rows": [], "status": "error", "error": str(e)}

    def scrape_dropdowns(self) -> dict[str, list[str]]:
        """Returns a mapping of dropdown labels to their visible choices."""
        results: dict[str, list[str]] = {}
        dropdown_map = self.dropdowns
        
        for label, widget_id in dropdown_map.items():
            print(f"Processing {label} ({widget_id})...")
            try:
                items = self._fetch_one_menu_items(widget_id)
                if not items:
                    items = [f"⚠️ No items found for {label}"]
                
                if items and items[0].startswith("Select"):
                    items = items[1:]
                    
                results[label] = items
                print(f"  ✓ Found {len(items)} items")
                
            except Exception as exc:
                error_msg = f"⚠️ Unexpected error: {exc}"
                results[label] = [error_msg]
                print(f"  ✗ {error_msg}")
            
            time.sleep(1)
        
        return results

    def scrape_multiple_combinations(self, combinations: list[dict]) -> pd.DataFrame:
        """Scrape data for multiple filter combinations and return as DataFrame."""
        all_data = []
        
        for i, filters in enumerate(combinations):
            print(f"\n--- Scraping combination {i+1}/{len(combinations)} ---")
            print(f"Filters: {filters}")
            
            try:
                result = self.apply_filters(filters)
                
                if result.get("status") == "success" and result.get("rows"):
                    # Add metadata to each row
                    for row_data in result["rows"]:
                        if len(row_data) >= len(result["headers"]):
                            row_dict = dict(zip(result["headers"], row_data))
                            # Add filter information
                            for filter_key, filter_value in filters.items():
                                row_dict[f"Filter_{filter_key}"] = filter_value
                            row_dict["Scraped_Date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            all_data.append(row_dict)
                
                print(f"✅ Successfully scraped {len(result.get('rows', []))} rows")
                
            except Exception as e:
                print(f"❌ Error scraping combination {filters}: {e}")
                continue
            
            # Add delay between combinations
            time.sleep(2)
        
        if all_data:
            df = pd.DataFrame(all_data)
            print(f"\n📊 Total data collected: {len(df)} rows, {len(df.columns)} columns")
            return df
        else:
            print("\n⚠️ No data collected")
            return pd.DataFrame()

    def save_data(self, data: pd.DataFrame, filename: str = None) -> str:
        """Save scraped data to CSV file."""
        if filename is None:
            filename = f"vahan_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        data.to_csv(filename, index=False)
        print(f"💾 Data saved to {filename}")
        return filename

    def close(self) -> None:
        """Close the browser."""
        if self.driver:
            self.driver.quit()


# Usage functions for different scraping scenarios
def scrape_vehicle_categories(scraper: VahanScraper, states: list[str], years: list[str]) -> pd.DataFrame:
    """Scrape data for different vehicle categories across states and years."""
    dropdown_data = scraper.scrape_dropdowns()
    vehicle_types = dropdown_data.get("Vehicle Type", [])
    
    combinations = []
    for state in states:
        for year in years:
            for vehicle_type in vehicle_types[:3]:  # Limit to first 3 vehicle types
                combinations.append({
                    "State": state,
                    "Year": year,
                    "Vehicle Type": vehicle_type
                })
    
    return scraper.scrape_multiple_combinations(combinations)


def scrape_manufacturer_data(scraper: VahanScraper, states: list[str], years: list[str]) -> pd.DataFrame:
    """Scrape manufacturer-wise registration data."""
    dropdown_data = scraper.scrape_dropdowns()
    
    combinations = []
    for state in states:
        for year in years:
            combinations.append({
                "State": state,
                "Year": year,
                "Y-Axis": "Manufacturer" if "Manufacturer" in dropdown_data.get("Y-Axis", []) else dropdown_data.get("Y-Axis", [""])[0]
            })
    
    return scraper.scrape_multiple_combinations(combinations)


# Main execution example
if __name__ == "__main__":
    URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
    scraper = VahanScraper(URL, wait_time=20)
    scraper.setup_driver(headless=True)
    
    try:
        print("🚀 Starting VAHAN data scraping...")
        scraper.open_page()
        
        print(f"Detected State ID: {scraper.dynamic_state_id}")
        print(f"Detected Refresh ID: {scraper.dynamic_refresh_id}")
        
        # Get available options
        dropdown_data = scraper.scrape_dropdowns()
        
        print("\n📋 Available dropdown options:")
        for label, options in dropdown_data.items():
            print(f"\n{label}:")
            for i, option in enumerate(options[:10], 1):
                print(f"  {i:2d}. {option}")
            if len(options) > 10:
                print(f"  ... and {len(options) - 10} more options")
        
        # Example scraping scenarios
        if dropdown_data.get("State") and len(dropdown_data["State"]) > 1:
            states_to_scrape = dropdown_data["State"][:3]  # First 3 states
            years_to_scrape = dropdown_data.get("Year", ["2023", "2024"])[:2]  # Last 2 years
            
            print(f"\n📊 Scraping vehicle category data...")
            vehicle_data = scrape_vehicle_categories(scraper, states_to_scrape, years_to_scrape)
            
            if not vehicle_data.empty:
                vehicle_file = scraper.save_data(vehicle_data, "vahan_vehicle_categories.csv")
                print(f"Vehicle category data saved to: {vehicle_file}")
            
            print(f"\n🏭 Scraping manufacturer data...")
            manufacturer_data = scrape_manufacturer_data(scraper, states_to_scrape, years_to_scrape)
            
            if not manufacturer_data.empty:
                manufacturer_file = scraper.save_data(manufacturer_data, "vahan_manufacturer_data.csv")
                print(f"Manufacturer data saved to: {manufacturer_file}")
        
        print("\n✅ Scraping completed successfully!")
        
    except Exception as e:
        print(f"❌ Script failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print("\n🔄 Closing browser...")
        scraper.close()
        print("Done!")