"""
Rent contracts downloader with enhanced error handling and retry logic.
"""

import requests
from bs4 import BeautifulSoup
import logging
import time
from typing import Optional

from lib.config import API_CONFIG

logger = logging.getLogger(__name__)


class RentContractsDownloader:
    """
    Downloads rent contract data from Dubai Land Department.
    
    Features:
    - Retry logic with exponential backoff
    - Progress tracking for large downloads
    - Comprehensive error handling
    - Request timeout configuration
    """
    
    def __init__(self, url: str):
        """
        Initialize with the URL to fetch rent contracts from.
        
        Args:
            url: URL to fetch rent contracts data
        """
        self.url = url
        self.timeout = API_CONFIG["request_timeout"]
        self.max_retries = API_CONFIG["max_retries"]
        self.backoff_factor = API_CONFIG["retry_backoff_factor"]

    def fetch_rent_contracts(self) -> bytes:
        """
        Fetch the rent contracts HTML content from the URL with retry logic.
        
        Returns:
            HTML content as bytes
            
        Raises:
            requests.exceptions.RequestException: If all retries fail
        """
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching rent contracts from {self.url} (attempt {attempt + 1}/{self.max_retries})")
                response = requests.get(self.url, timeout=self.timeout)
                response.raise_for_status()
                logger.info(f"Successfully fetched HTML content ({len(response.content):,} bytes)")
                return response.content
                
            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout on attempt {attempt + 1}")
                if attempt < self.max_retries - 1:
                    wait_time = self.backoff_factor ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached for fetching HTML")
                    raise
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self.backoff_factor ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached")
                    raise

    def parse_html(self, html_content: bytes) -> Optional[str]:
        """
        Parse the HTML content to find the download link.
        
        Args:
            html_content: HTML content as bytes
            
        Returns:
            Download URL if found, None otherwise
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            a_tag = soup.find('a', class_='action-icon-anchor')
            
            if a_tag and 'href' in a_tag.attrs:
                href = a_tag['href']
                logger.info(f"Found download link: {href}")
                return href
            else:
                logger.warning("No download link found with class 'action-icon-anchor'")
                return None
                
        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
            return None

    def download_file(self, href: str, filename: str) -> None:
        """
        Download the file from the given href and save it as filename.
        
        Args:
            href: URL to download from
            filename: Local filename to save to
            
        Raises:
            requests.exceptions.RequestException: If download fails
        """
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Downloading file from {href} (attempt {attempt + 1}/{self.max_retries})")
                
                response = requests.get(href, stream=True, timeout=self.timeout)
                response.raise_for_status()
                
                # Get total file size if available
                total_size = int(response.headers.get('content-length', 0))
                
                downloaded = 0
                chunk_size = 8192
                
                with open(filename, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            file.write(chunk)
                            downloaded += len(chunk)
                            
                            # Log progress for large files (> 1MB)
                            if total_size > 1_000_000 and downloaded % (1024 * 1024) == 0:
                                pct = (downloaded / total_size * 100) if total_size else 0
                                logger.debug(f"Downloaded {downloaded:,} / {total_size:,} bytes ({pct:.1f}%)")
                
                logger.info(f"Successfully downloaded {downloaded:,} bytes to {filename}")
                return
                
            except requests.exceptions.Timeout:
                logger.warning(f"Download timeout on attempt {attempt + 1}")
                if attempt < self.max_retries - 1:
                    wait_time = self.backoff_factor ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached for download")
                    raise
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Download failed on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self.backoff_factor ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached")
                    raise
                    
            except IOError as e:
                logger.error(f"Error writing to file {filename}: {e}")
                raise

    def run(self, filename: str) -> bool:
        """
        Run the downloader to fetch, parse, and download the rent contract file.
        
        Args:
            filename: Local filename to save downloaded data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            html_content = self.fetch_rent_contracts()
            href = self.parse_html(html_content)
            
            if href:
                self.download_file(href, filename)
                logger.info(f"Rent contracts successfully downloaded to {filename}")
                return True
            else:
                logger.error("No download link found in HTML")
                return False
                
        except Exception as e:
            logger.error(f"Failed to download rent contracts: {e}")
            return False