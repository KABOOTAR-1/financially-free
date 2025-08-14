"""
VAHAN-specific scraper implementation.
Handles PrimeFaces dropdown interactions with dynamic IDs.
"""

import time
import re
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper
from ..core.config import Config
from ..core.exceptions import ScrapingError, DropdownError, DynamicIDError
from ..core.models import FilterCombination, ScrapingResult
from ..utils.logging_utils import get_logger

class VahanScraper(BaseScraper):
    """Enhanced scraper for VAHAN dashboard that handles PrimeFaces dropdown interactions with dynamic IDs."""
    
    def __init__(self, base_url: str = None, wait_time: int = None):
        """Initialize VAHAN scraper.
        
        Args:
            base_url: VAHAN dashboard URL (uses config default if None)
            wait_time: Maximum wait time for elements (uses config default if None)
        """
        super().__init__(
            base_url or Config.VAHAN_BASE_URL,
            wait_time or Config.DEFAULT_WAIT_TIME
        )
        self.dynamic_state_id: Optional[str] = None
        self.dynamic_refresh_id: Optional[str] = None
        self.scraped_data: List[Dict] = []
        
    def open_page(self) -> None:
        """Open the VAHAN dashboard and detect dynamic IDs."""
        super().open_page()
        time.sleep(3)  # Wait for page to fully load
        self._detect_dynamic_ids()
        
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
                    self.logger.info(f"✓ Detected state dropdown ID: {self.dynamic_state_id}")
                else:
                    parent_div = state_select.find_element(By.XPATH, "..")
                    while parent_div and not parent_div.get_attribute('class').startswith('ui-selectonemenu'):
                        parent_div = parent_div.find_element(By.XPATH, "..")
                    if parent_div:
                        self.dynamic_state_id = parent_div.get_attribute('id')
                        self.logger.info(f"✓ Detected state dropdown ID: {self.dynamic_state_id}")
            
            if not self.dynamic_state_id:
                state_divs = self.driver.find_elements(
                    By.XPATH,
                    "//div[contains(@class, 'ui-selectonemenu')]/label[contains(text(), 'States') or contains(text(), 'Vahan4')]/.."
                )
                if state_divs:
                    self.dynamic_state_id = state_divs[0].get_attribute('id')
                    self.logger.info(f"✓ Detected state dropdown ID (fallback): {self.dynamic_state_id}")
            
            # Find refresh button by looking for buttons with refresh-related text or icons
            refresh_buttons = self.driver.find_elements(
                By.XPATH,
                "//button[contains(@class, 'ui-button') and (contains(text(), 'Refresh') or contains(@title, 'Refresh') or .//span[contains(@class, 'ui-icon-refresh')])]"
            )
            
            if refresh_buttons:
                self.dynamic_refresh_id = refresh_buttons[0].get_attribute('id')
                self.logger.info(f"✓ Detected refresh button ID: {self.dynamic_refresh_id}")
            else:
                # Fallback: look for any button with refresh icon
                refresh_icons = self.driver.find_elements(
                    By.XPATH,
                    "//span[contains(@class, 'ui-icon-refresh')]/parent::button"
                )
                if refresh_icons:
                    self.dynamic_refresh_id = refresh_icons[0].get_attribute('id')
                    self.logger.info(f"✓ Detected refresh button ID (fallback): {self.dynamic_refresh_id}")
            
            if not self.dynamic_state_id:
                raise DynamicIDError("Could not detect dynamic state dropdown ID")
            if not self.dynamic_refresh_id:
                self.logger.warning("⚠️ Could not detect refresh button ID - refresh functionality may not work")
                
        except Exception as e:
            raise DynamicIDError(f"Failed to detect dynamic IDs: {e}")
    
    @property
    def dropdowns(self) -> Dict[str, str]:
        """Get dropdown mapping including dynamically detected state ID."""
        dropdown_map = Config.STATIC_DROPDOWNS.copy()
        if self.dynamic_state_id:
            dropdown_map["State"] = self.dynamic_state_id
        return dropdown_map
    
    def fetch_data(self) -> Dict:
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
            
            self.logger.info(f"📊 Table found - Headers: {len(headers)}, Data rows: {len(data)}")
            self.logger.debug(f"Headers: {headers}")
            
            for i, row in enumerate(data[:5]):
                self.logger.debug(f"Row {i+1}: {row}")
            if len(data) > 5:
                self.logger.debug(f"... and {len(data) - 5} more rows")
            
            return {
                "headers": headers,
                "rows": data,
                "status": "success",
                "total_rows": len(data)
            }
            
        except TimeoutException:
            self.logger.warning("⚠️ Timeout waiting for table to load")
            return {"headers": [], "rows": [], "status": "timeout"}
        except Exception as e:
            self.logger.error(f"⚠️ Error fetching data: {e}")
            return {"headers": [], "rows": [], "status": "error", "error": str(e)}
    
    def _parse_complex_table_headers(self) -> List[str]:
        """Parse complex table headers with proper handling of colspan/rowspan."""
        try:
            table_element = self.driver.find_element(By.CSS_SELECTOR, "#combTablePnl table")
            table_html = table_element.get_attribute('outerHTML')
            soup = BeautifulSoup(table_html, 'html.parser')
            
            thead = soup.find('thead', id='vchgroupTable_head')
            if not thead:
                thead = soup.find('thead')
            
            if not thead:
                self.logger.warning("⚠️ No thead found, falling back to simple header parsing")
                return self._get_simple_headers()
            
            header_rows = thead.find_all('tr', role='row')
            self.logger.debug(f"Found {len(header_rows)} header rows")
            
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
            
            self.logger.info(f"✓ Parsed complex headers ({len(final_headers)} columns): {final_headers}")
            return final_headers
            
        except Exception as e:
            self.logger.error(f"⚠️ Error parsing complex headers: {e}")
            return self._get_simple_headers()
    
    def _get_simple_headers(self) -> List[str]:
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
    
    def scrape_dropdowns(self) -> Dict[str, List[str]]:
        """Returns a mapping of dropdown labels to their visible choices."""
        dropdown_data = {}
        
        for label, dropdown_id in self.dropdowns.items():
            try:
                self.logger.info(f"🔍 Scraping {label} dropdown...")
                items = self._fetch_one_menu_items(dropdown_id)
                dropdown_data[label] = items
                
            except Exception as e:
                self.logger.error(f"❌ Failed to scrape {label} dropdown: {e}")
                dropdown_data[label] = []
        
        return dropdown_data
    
    def _fetch_one_menu_items(self, base_id: str, max_retries: int = 3) -> List[str]:
        """Fetch items from a PrimeFaces selectOneMenu with retry logic."""
        # Handle _input suffix like the original
        if base_id.endswith("_input"):
            base_id = base_id[:-6]
            
        for attempt in range(max_retries):
            try:
                self._close_all_open_panels()
                
                if attempt > 0:
                    time.sleep(2)
                    
                dropdown_selector = f"{base_id}"
                self.logger.debug(f"Processing dropdown: {dropdown_selector}")
                dropdown = self.wait.until(
                    EC.presence_of_element_located((By.ID, dropdown_selector))
                )
                
                # Scroll into view
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
                
                # Wait for panel to appear
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
                
                # Close the dropdown
                try:
                    dropdown.click()
                except:
                    body = self.driver.find_element(By.TAG_NAME, "body")
                    body.click()
                
                if items:
                    self.logger.info(f"✅ Found {len(items)} items for {base_id}")
                    return items
                
            except TimeoutException as e:
                self.logger.warning(f"⚠️ Attempt {attempt + 1} failed for {base_id}: Timeout - {e}")
                if attempt == max_retries - 1:
                    return [f"⚠️ Timeout: Panel not found for {base_id}"]
                continue
                
            except Exception as e:
                self.logger.warning(f"⚠️ Attempt {attempt + 1} failed for {base_id}: {e}")
                if attempt == max_retries - 1:
                    return [f"⚠️ Error (attempt {attempt + 1}): {str(e)[:100]}..."]
                continue
                
        return [f"⚠️ Failed after {max_retries} attempts"]
    
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
    
    def _select_dropdown_option(self, dropdown_id: str, option_value: str) -> None:
        """Select an option from a dropdown."""
        try:
            self._close_all_open_panels()
            
            # Click dropdown trigger
            trigger_xpath = f"//div[@id='{dropdown_id}']//div[contains(@class, 'ui-selectonemenu-trigger')]"
            trigger = self.wait.until(EC.element_to_be_clickable((By.XPATH, trigger_xpath)))
            ActionChains(self.driver).move_to_element(trigger).click().perform()
            time.sleep(1)
            
            # Find and click the option
            option_xpath = f"//div[@id='{dropdown_id}_panel']//li[@data-label='{option_value}' or text()='{option_value}']"
            option = self.wait.until(EC.element_to_be_clickable((By.XPATH, option_xpath)))
            ActionChains(self.driver).move_to_element(option).click().perform()
            
            self.logger.info(f"✅ Selected '{option_value}' from {dropdown_id}")
            
        except Exception as e:
            raise DropdownError(f"Failed to select '{option_value}' from {dropdown_id}: {e}")
    
    def _click_refresh_button(self) -> dict:
        """Skip refresh button click and directly fetch data."""
        try:
            print("⏳ Skipping refresh button, fetching data directly...")
            time.sleep(2)  # Brief wait for any filter changes to take effect
            
            try:
                self.wait.until(
                    EC.presence_of_element_located((By.ID, "combTablePnl"))
                )
                return self.fetch_data()
            except TimeoutException:
                print("⚠️ Table didn't load within timeout period")
                return {"headers": [], "rows": [], "status": "timeout"}
                
        except Exception as e:
            print(f"Error fetching data: {e}")
            return {"headers": [], "rows": [], "status": "error", "error": str(e)}
    
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
            filename = Config.get_output_filename("vahan_data")
        
        filepath = Config.OUTPUT_DIR / filename
        Config.ensure_directories()
        
        try:
            data.to_csv(filepath, index=False)
            self.logger.info(f"💾 Data saved to {filepath}")
            return str(filepath)
        except Exception as e:
            raise ScrapingError(f"Failed to save data: {e}")
