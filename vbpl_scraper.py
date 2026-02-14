#
# Created on:   Feb 3, 2026
# Author:       quandm
# Description:  vbpl.vn data extraction script
#

import asyncio
import aiohttp
import os
import json
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import hashlib
from loguru import logger


# CONFIGURATION
CONFIG = {
    # Rate limiting
    'delay_between_requests': 2.0,  # seconds
    'max_concurrent_requests': 2,
    'max_retries': 5,
    'base_retry_delay': 1.0,
    
    # URLs
    'base_url': 'https://vbpl.vn',
    
    # AJAX endpoints for search results
    # Tab 1: Văn bản pháp quy
    # Tab 2: Văn bản hợp nhất
    'ajax_search_urls': {
        'TNCN': {
            'van_ban_phap_quy': 'https://vbpl.vn/VBQPPL_UserControls/Publishing_22/TimKiem/p_KetQuaTimKiemVanBan.aspx?type=0&s=0&SearchIn=Title,Title1&Keyword=Thuế thu nhập cá nhân&IsVietNamese=True&DivID=tabVB_lv1_01',
            'van_ban_hop_nhat': 'https://vbpl.vn/VBQPPL_UserControls/Publishing_22/Timkiem/p_KetQuaTimKiemHopNhat.aspx?type=0&s=0&SearchIn=Title,Title1&Keyword=Thuế thu nhập cá nhân&IsVietNamese=True&DivID=tabVB_lv1_02'
        },
        'TNDN': {
            'van_ban_phap_quy': 'https://vbpl.vn/VBQPPL_UserControls/Publishing_22/TimKiem/p_KetQuaTimKiemVanBan.aspx?type=0&s=0&SearchIn=Title,Title1&Keyword=Thuế thu nhập doanh nghiệp&IsVietNamese=True&DivID=tabVB_lv1_01',
            'van_ban_hop_nhat': 'https://vbpl.vn/VBQPPL_UserControls/Publishing_22/Timkiem/p_KetQuaTimKiemHopNhat.aspx?type=0&s=0&SearchIn=Title,Title1&Keyword=Thuế thu nhập doanh nghiệp&IsVietNamese=True&DivID=tabVB_lv1_02'
        }
    },
    
    # Output directories
    'output_base': 'output',
    'category_dirs': {
        'TNCN': 'thue_thu_nhap_ca_nhan',
        'TNDN': 'thue_thu_nhap_doanh_nghiep'
    },
    
    # Headers
    'headers': {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
}

# SETUP LOGGING
def setup_logging():
    """Configure logging with file and console output"""
    log_dir = Path(CONFIG['output_base']) / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"scraper_{datetime.now():%Y-%m-%d_%H-%M-%S}.log"
    
    # Remove default handler
    logger.remove()
    
    # Add console handler
    logger.add(
        lambda msg: print(msg, end=''),
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        level="INFO"
    )
    
    # Add file handler
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level="DEBUG",
        rotation="10 MB",
        retention="30 days"
    )
    
    logger.info(f"Logging initialized: {log_file}")

# UTILITY FUNCTIONS
def normalize_so_ky_hieu(so_ky_hieu: str) -> str:
    """
    Convert số ký hiệu to valid folder name
    
    Examples:
        '67/2025/QH15' -> '67_2025_QH15'
        'Luật 56/2024/QH15' -> '56_2024_QH15'
        'Chỉ thị 31/CT-UBND (năm 2008)' -> '31_CT-UBND'
    """
    # Extract just the document number pattern
    # Patterns to try (in order of priority):
    patterns = [
        r'(\d+/\d{4}/[\w-]+)',          # 67/2025/QH15
        r'(\d+/[\w-]+)',                 # 31/CT-UBND
        r'([\w-]+/\d{4}/[\w-]+)',       # ABC/2020/XYZ
    ]
    
    normalized = None
    for pattern in patterns:
        match = re.search(pattern, so_ky_hieu)
        if match:
            normalized = match.group(1)
            break
    
    # If no pattern matched, use the whole string
    if not normalized:
        normalized = so_ky_hieu
    
    # Remove common prefixes
    prefixes_to_remove = [
        r'^Luật\s+',
        r'^Nghị định\s+',
        r'^Thông tư\s+',
        r'^Quyết định\s+',
        r'^Chỉ thị\s+',
        r'^Nghị quyết\s+',
        r'^Lệnh\s+',
    ]
    for prefix in prefixes_to_remove:
        normalized = re.sub(prefix, '', normalized, flags=re.IGNORECASE)
    
    # Remove trailing info (Eg: năm 2008)
    normalized = re.sub(r'\s*\([^)]*\)\s*$', '', normalized)
    
    # Replace slashes with underscores
    normalized = normalized.replace('/', '_').replace('\\', '_')
    
    # Remove or replace invalid filename characters
    normalized = re.sub(r'[<>:"|?*]', '_', normalized)
    
    # Remove leading/trailing spaces
    normalized = normalized.strip()
    
    # If empty after all processing, use a fallback
    if not normalized:
        # Use original but sanitized
        normalized = re.sub(r'[<>:"|?*/\\]', '_', so_ky_hieu).strip()
        if not normalized:
            normalized = 'unknown_document'
    
    return normalized

def convert_date(date_str: str) -> Optional[str]:
    """
    Convert DD/MM/YYYY to YYYY-MM-DD (ISO 8601)
    
    Args:
        date_str: Date in DD/MM/YYYY format
    
    Returns:
        Date in YYYY-MM-DD format or None
    """
    if not date_str or date_str == '...' or date_str.strip() == '':
        return None
    
    try:
        # Handle DD/MM/YYYY
        parts = date_str.strip().split('/')
        if len(parts) == 3:
            day, month, year = parts
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        return None
    except Exception as e:
        logger.warning(f"Failed to convert date '{date_str}': {e}")
        return None

def calculate_file_hash(filepath: str) -> Optional[str]:
    """Calculate SHA256 hash of file"""
    try:
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.error(f"Failed to calculate hash for {filepath}: {e}")
        return None

def verify_pdf(filepath: str) -> bool:
    """Verify file is a valid PDF by checking magic bytes"""
    try:
        with open(filepath, 'rb') as f:
            header = f.read(4)
            return header == b'%PDF'
    except:
        return False

def verify_word(filepath: str) -> bool:
    """Verify file is a valid Word document"""
    try:
        with open(filepath, 'rb') as f:
            header = f.read(8)
            
            # Check for .docx (ZIP format: PK\x03\x04)
            if header[:2] == b'PK':
                return True
            
            # Check for .doc (OLE format: D0 CF 11 E0 A1 B1 1A E1)
            if header[:4] == b'\xD0\xCF\x11\xE0':
                return True
            
            # Some .doc files might have slightly different headers
            # Just check if file size > 100 bytes as lenient fallback
            file_size = os.path.getsize(filepath)
            if file_size > 100:
                logger.warning(f"Word file has unknown header but size {file_size} > 100 bytes, accepting")
                return True
            
            return False
    except:
        return False

def verify_zip(filepath: str) -> bool:
    """Verify file is a valid ZIP archive"""
    try:
        with open(filepath, 'rb') as f:
            header = f.read(4)
            # Check for ZIP magic bytes: PK\x03\x04
            return header[:2] == b'PK'
    except:
        return False

def extract_zip(zip_path: str, extract_dir: str) -> bool:
    """
    Extract ZIP file to directory
    
    Args:
        zip_path: Path to ZIP file
        extract_dir: Directory to extract to
    
    Returns:
        True if successful, False otherwise
    """
    try:
        import zipfile
        
        # Verify it's a valid ZIP
        if not verify_zip(zip_path):
            logger.error(f"Invalid ZIP file: {zip_path}")
            return False
        
        # Extract
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Count extracted files
        extracted_count = len(os.listdir(extract_dir)) - 1  # -1 for metadata.json
        logger.info(f"Extracted {extracted_count} files from ZIP")
        
        return True
    except Exception as e:
        logger.error(f"Failed to extract ZIP: {e}")
        return False

def save_metadata(metadata: Dict, filepath: str):
    """Save metadata to JSON file"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        logger.debug(f"Metadata saved: {filepath}")
    except Exception as e:
        logger.error(f"Failed to save metadata to {filepath}: {e}")
        raise

def merge_metadata(existing: Dict, new: Dict) -> Dict:
    """
    Merge two metadata dicts (for handling duplicates)
    Keep both source URLs and merge file info
    """
    merged = existing.copy()
    
    # Add new source URL to list
    if 'source_urls' not in merged:
        merged['source_urls'] = [existing.get('source', {})]
    merged['source_urls'].append(new.get('source', {}))
    
    # Update scraped_at
    merged['scraped_at'] = new.get('scraped_at', merged.get('scraped_at'))
    
    # Merge document info (prefer non-None values)
    for key, value in new.get('document', {}).items():
        if value and not merged.get('document', {}).get(key):
            if 'document' not in merged:
                merged['document'] = {}
            merged['document'][key] = value
    
    # Update file info if new one is better
    if new.get('file', {}).get('pdf_downloaded') and not merged.get('file', {}).get('pdf_downloaded'):
        merged['file'] = new.get('file', {})
    
    return merged

def should_skip_document(folder_path: str, metadata_path: str) -> bool:
    """
    Check if document is already fully processed
    
    Returns True if:
    - Folder exists
    - metadata.json exists and is valid
    - Status is 'success'
    - PDF/Word is downloaded
    - File actually exists and is valid
    """
    if not os.path.exists(folder_path):
        return False
    
    if not os.path.exists(metadata_path):
        return False
    
    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        # Check status
        if metadata.get('status') != 'success':
            return False
        
        # Check file downloaded flag
        file_info = metadata.get('file', {})
        if not file_info.get('pdf_downloaded') and not file_info.get('word_downloaded'):
            return False
        
        # Check file exists
        filename = file_info.get('pdf_filename') or file_info.get('word_filename')
        if not filename:
            return False
        
        filepath = os.path.join(folder_path, filename)
        if not os.path.exists(filepath):
            return False
        
        # Verify file is valid
        if file_info.get('pdf_downloaded') and not verify_pdf(filepath):
            logger.warning(f"Invalid PDF detected: {filepath}")
            return False
        
        if file_info.get('word_downloaded') and not verify_word(filepath):
            logger.warning(f"Invalid Word document detected: {filepath}")
            return False
        
        return True
        
    except Exception as e:
        logger.warning(f"Error checking document status at {metadata_path}: {e}")
        return False

# HTTP CLIENT
class HTTPClient:
    """Async HTTP client with retry logic and rate limiting"""
    
    def __init__(self, session: aiohttp.ClientSession, config: Dict):
        self.session = session
        self.config = config
        self.semaphore = asyncio.Semaphore(config['max_concurrent_requests'])
    
    async def fetch(self, url: str, description: str = "") -> Optional[str]:
        """
        Fetch URL with retry logic and exponential backoff
        
        Args:
            url: URL to fetch
            description: Description for logging
        
        Returns:
            HTML content or None if failed
        """
        async with self.semaphore:
            for attempt in range(self.config['max_retries'] + 1):
                try:
                    # Rate limiting delay
                    await asyncio.sleep(self.config['delay_between_requests'])
                    
                    logger.debug(f"Fetching [{attempt+1}/{self.config['max_retries']+1}]: {description or url}")
                    
                    async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        if response.status == 200:
                            html = await response.text()
                            logger.debug(f"Success: {description or url} ({len(html)} bytes)")
                            return html
                        
                        # Handle 404
                        if response.status == 404:
                            logger.warning(f"404 Not Found: {url}")
                            return None
                        
                        # Handle rate limiting
                        if response.status in [429, 503]:
                            wait_time = self.config['base_retry_delay'] * (2 ** attempt)
                            logger.warning(f"Rate limited (HTTP {response.status}), waiting {wait_time:.1f}s...")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        # Other HTTP errors
                        if attempt < self.config['max_retries']:
                            wait_time = self.config['base_retry_delay'] * (attempt + 1)
                            logger.warning(f"HTTP {response.status}, retrying in {wait_time:.1f}s (attempt {attempt+1}/{self.config['max_retries']})")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"Failed after {self.config['max_retries']} retries: HTTP {response.status}")
                            return None
                
                except asyncio.TimeoutError:
                    if attempt < self.config['max_retries']:
                        wait_time = self.config['base_retry_delay'] * (attempt + 1)
                        logger.warning(f"Timeout, retrying in {wait_time:.1f}s (attempt {attempt+1}/{self.config['max_retries']})")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Timeout after {self.config['max_retries']} retries: {url}")
                        return None
                
                except aiohttp.ClientError as e:
                    if attempt < self.config['max_retries']:
                        wait_time = self.config['base_retry_delay'] * (attempt + 1)
                        logger.warning(f"Network error: {e}, retrying in {wait_time:.1f}s")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Network error after {self.config['max_retries']} retries: {e}")
                        return None
            
            return None
    
    async def download_file(self, url: str, filepath: str, description: str = "") -> bool:
        """
        Download file with retry logic
        
        Args:
            url: URL to download
            filepath: Destination path
            description: Description for logging
        
        Returns:
            True if successful, False otherwise
        """
        async with self.semaphore:
            temp_filepath = filepath + '.tmp'
            
            for attempt in range(self.config['max_retries'] + 1):
                try:
                    await asyncio.sleep(self.config['delay_between_requests'])
                    
                    logger.debug(f"Downloading [{attempt+1}/{self.config['max_retries']+1}]: {description or url}")
                    
                    async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
                        if response.status == 200:
                            # Stream to temp file
                            with open(temp_filepath, 'wb') as f:
                                async for chunk in response.content.iter_chunked(8192):
                                    f.write(chunk)
                            
                            # Atomic rename
                            os.replace(temp_filepath, filepath)
                            size = os.path.getsize(filepath)
                            logger.debug(f"Downloaded: {description or url} ({size} bytes)")
                            return True
                        
                        if response.status == 404:
                            logger.warning(f"404 Not Found: {url}")
                            return False
                        
                        if attempt < self.config['max_retries']:
                            wait_time = self.config['base_retry_delay'] * (attempt + 1)
                            logger.warning(f"HTTP {response.status}, retrying in {wait_time:.1f}s")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.error(f"Failed to download after {self.config['max_retries']} retries")
                            return False
                
                except Exception as e:
                    if attempt < self.config['max_retries']:
                        wait_time = self.config['base_retry_delay'] * (attempt + 1)
                        logger.warning(f"Download error: {e}, retrying in {wait_time:.1f}s")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Download failed after {self.config['max_retries']} retries: {e}")
                        if os.path.exists(temp_filepath):
                            os.remove(temp_filepath)
                        return False
            
            return False

# HTML PARSERS
def parse_list_page(html: str, category: str, doc_type: str) -> Dict:
    """
    Parse search results list page
    
    Args:
        html: HTML content
        category: Category code (TNCN, TNDN)
        doc_type: Document type (van_ban_phap_quy, van_ban_hop_nhat)
    
    Returns:
        {
            'total_documents': int,
            'total_pages': int,
            'documents': [...]
        }
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    # Get total document count
    # Try: <div class="message"><span>Tìm thấy <strong>71</strong> văn bản.</span></div>
    total_docs = 0
    message_div = soup.select_one('div.message')
    if message_div:
        strong_tag = message_div.find('strong')
        if strong_tag:
            try:
                total_docs = int(strong_tag.text.strip())
            except:
                pass
    
    if total_docs == 0:
        # Fallback: count documents on current page and estimate
        doc_items = soup.select('ul.listLaw > li')
        if doc_items:
            logger.warning(f"Could not find total count, estimating from page: {len(doc_items)} docs found")
            total_docs = len(doc_items) * 10  # Rough estimate
        else:
            logger.warning("No documents found on page")
            return {
                'total_documents': 0,
                'total_pages': 0,
                'documents': []
            }
    
    # Calculate total pages (30 docs per page based on logs)
    docs_per_page = 30
    total_pages = (total_docs + docs_per_page - 1) // docs_per_page
    
    # Parse document items
    documents = []
    doc_items = soup.select('ul.listLaw > li')
    
    logger.debug(f"Found {len(doc_items)} documents on page")
    
    for idx, item in enumerate(doc_items, 1):
        try:
            doc = parse_list_item(item, category, doc_type)
            documents.append(doc)
        except Exception as e:
            logger.error(f"Failed to parse document {idx}: {e}")
    
    return {
        'total_documents': total_docs,
        'total_pages': total_pages,
        'documents': documents
    }

def parse_list_item(item, category: str, doc_type: str) -> Dict:
    """Parse a single <li> item from search results"""
    # Extract link and ItemID
    title_link = item.select_one('p.title a')
    if not title_link:
        raise Exception("No title link found")
    
    href = title_link.get('href', '')
    match = re.search(r'ItemID=(\d+)', href)
    if not match:
        raise Exception(f"Could not extract ItemID from {href}")
    
    item_id = match.group(1)
    so_ky_hieu = title_link.text.strip()
    
    # Extract description
    des_p = item.select_one('div.des p')
    trich_yeu = des_p.text.strip() if des_p else ""
    
    # Extract dates from right column
    right_div = item.select_one('div.right')
    ngay_ban_hanh_raw = None
    ngay_hieu_luc_raw = None
    trang_thai_raw = None
    
    if right_div:
        green_ps = right_div.select('p.green')
        if len(green_ps) >= 1:
            ngay_ban_hanh_raw = green_ps[0].text.split(':', 1)[1].strip() if ':' in green_ps[0].text else None
        if len(green_ps) >= 2:
            ngay_hieu_luc_raw = green_ps[1].text.split(':', 1)[1].strip() if ':' in green_ps[1].text else None
        
        red_p = right_div.select_one('p.red')
        if red_p:
            trang_thai_raw = red_p.text.split(':', 1)[1].strip() if ':' in red_p.text else None
    
    return {
        'item_id': item_id,
        'category': category,
        'doc_type': doc_type,  # van_ban_phap_quy or van_ban_hop_nhat
        'so_ky_hieu': so_ky_hieu,
        'trich_yeu': trich_yeu,
        'ngay_ban_hanh_raw': ngay_ban_hanh_raw,
        'ngay_hieu_luc_raw': ngay_hieu_luc_raw,
        'trang_thai_raw': trang_thai_raw,
        'thuoc_tinh_url': f"https://vbpl.vn/TW/Pages/vbpq-thuoctinh.aspx?ItemID={item_id}",
        'pdf_page_url': f"https://vbpl.vn/TW/Pages/vbpq-van-ban-goc.aspx?ItemID={item_id}"
    }

def parse_thuoc_tinh_page(html: str, basic_info: Dict) -> Dict:
    """
    Parse Thuộc tính (attributes) page
    
    Returns:
        Complete metadata dict
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    table = soup.select_one('div.vbProperties table')
    if not table:
        raise Exception("Could not find vbProperties table")
    
    def get_field_value(label_text: str) -> Optional[str]:
        """Find cell with label, return next cell's text"""
        label_cell = table.find('td', class_='label', string=lambda x: x and label_text in x)
        if label_cell:
            value_cell = label_cell.find_next_sibling('td')
            if value_cell:
                # Handle both plain text and <ul><li> format
                ul = value_cell.find('ul')
                if ul:
                    items = [li.text.strip() for li in ul.find_all('li')]
                    return ', '.join(items) if items else None
                return value_cell.get_text(strip=True) or None
        return None
    
    # Extract title
    title_cell = table.select_one('tr td.title[colspan="4"]')
    ten_van_ban = title_cell.text.strip() if title_cell else basic_info['so_ky_hieu']
    
    # For Thông tư liên tịch, there can be multiple rows or a single row with multiple agencies
    co_quan_list = []
    chuc_danh_list = []
    nguoi_ky_list = []
    
    # Find all rows with "Cơ quan ban hành" label
    co_quan_labels = table.find_all('td', class_='label', string=lambda x: x and 'Cơ quan ban hành' in x)
    
    for co_quan_label in co_quan_labels:
        co_quan_row = co_quan_label.find_parent('tr')
        cells = co_quan_row.find_all('td')
        
        if len(cells) >= 4:
            # Extract text from <a> tag if present
            co_quan_cell = cells[1]
            co_quan_a = co_quan_cell.find('a')
            co_quan = co_quan_a.text.strip() if co_quan_a else co_quan_cell.get_text(strip=True)
            
            chuc_danh = cells[2].get_text(strip=True) or None
            nguoi_ky = cells[3].get_text(strip=True) or None
            
            if co_quan:
                co_quan_list.append(co_quan)
            if chuc_danh:
                chuc_danh_list.append(chuc_danh)
            if nguoi_ky:
                nguoi_ky_list.append(nguoi_ky)
    
    # Join multiple agencies with comma
    co_quan = ', '.join(co_quan_list) if co_quan_list else None
    chuc_danh = ', '.join(chuc_danh_list) if chuc_danh_list else None
    nguoi_ky = ', '.join(nguoi_ky_list) if nguoi_ky_list else None
    
    # Get status from vbInfo section
    tinh_trang = None
    vb_info = soup.select_one('div.vbInfo')
    if vb_info:
        status_li = vb_info.select_one('li.red')
        if not status_li:
            status_li = vb_info.select_one('li.green')
        if status_li:
            text = status_li.get_text()
            if ':' in text:
                tinh_trang = text.split(':', 1)[1].strip()
    
    # Build metadata
    metadata = {
        'status': 'success',
        'scraped_at': datetime.now(timezone.utc).isoformat(),
        'source': {
            'item_id': basic_info['item_id'],
            'category': basic_info['category'],
            'doc_type': basic_info.get('doc_type', 'van_ban_phap_quy'),
            'detail_url': basic_info['thuoc_tinh_url'],
            'pdf_page_url': basic_info['pdf_page_url']
        },
        'document': {
            'so_ky_hieu': get_field_value('Số ký hiệu') or basic_info['so_ky_hieu'],
            'ten_van_ban': ten_van_ban,
            'loai_van_ban': get_field_value('Loại văn bản'),
            'co_quan_ban_hanh': co_quan,
            'chuc_danh': chuc_danh,
            'nguoi_ky': nguoi_ky,
            'ngay_ban_hanh': convert_date(get_field_value('Ngày ban hành')),
            'ngay_hieu_luc': convert_date(get_field_value('Ngày có hiệu lực')),
            'ngay_cong_bao': convert_date(get_field_value('Ngày đăng công báo')),
            'so_cong_bao': None,  # Not present in current format
            'nganh': get_field_value('Ngành'),
            'linh_vuc': get_field_value('Lĩnh vực'),
            'pham_vi': get_field_value('Phạm vi'),
            'tinh_trang_hieu_luc': tinh_trang or basic_info.get('trang_thai_raw'),
            'trich_yeu': basic_info['trich_yeu'],
            'nguon_thu_thap': get_field_value('Nguồn thu thập')
        },
        'file': {
            'pdf_downloaded': False,
            'word_downloaded': False,
            'pdf_filename': f"{normalize_so_ky_hieu(basic_info['so_ky_hieu'])}.pdf",
            'word_filename': f"{normalize_so_ky_hieu(basic_info['so_ky_hieu'])}.doc",
            'pdf_url': None,
            'word_url': None,
            'pdf_size_bytes': None,
            'word_size_bytes': None,
            'pdf_hash_sha256': None,
            'word_hash_sha256': None
        }
    }
    
    return metadata

def extract_download_links(html: str, item_id: str) -> List[Tuple[str, str]]:
    """
    The dialog is embedded in the detail page (vbpq-thuoctinh.aspx) with ID divShowDialogDownload_{ItemID}
    
    Returns:
        List of (filename, url) tuples, prioritized as PDF > Word > Others
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find download dialog - it's embedded in the detail page with ItemID
    download_div = soup.select_one(f'#divShowDialogDownload_{item_id}')
    if not download_div:
        # Try without ItemID suffix
        download_div = soup.select_one('#divShowDialogDownload')
    
    if not download_div:
        logger.warning(f"No download dialog found for ItemID {item_id}")
        return []
    
    # Find all download links
    links = download_div.select('ul.fileAttack li a')
    
    files = []
    for link in links:
        # Extract filename and URL from javascript:downloadfile('filename', '/path/to/file')
        href = link.get('href', '')
        match = re.search(r"downloadfile\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]", href)
        if match:
            filename = match.group(1)
            url = match.group(2)
            files.append((filename, url))
        else:
            # Fallback: try to get from link text
            filename = link.get_text(strip=True)
            # Try simpler regex
            match2 = re.search(r"downloadfile\([^,]+,\s*['\"]([^'\"]+)['\"]", href)
            if match2 and filename:
                url = match2.group(1)
                files.append((filename, url))
    
    if not files:
        logger.warning("No downloadable files found")
        return []
    
    # Prioritize: PDF > Word > ZIP > Others
    pdf_files = [(f, u) for f, u in files if f.lower().endswith('.pdf')]
    word_files = [(f, u) for f, u in files if f.lower().endswith(('.doc', '.docx'))]
    zip_files = [(f, u) for f, u in files if f.lower().endswith('.zip')]
    other_files = [(f, u) for f, u in files if not f.lower().endswith(('.pdf', '.doc', '.docx', '.zip'))]
    
    # Return prioritized list
    prioritized = pdf_files + word_files + zip_files + other_files
    
    logger.debug(f"Found {len(files)} files: {len(pdf_files)} PDF, {len(word_files)} Word, {len(zip_files)} ZIP, {len(other_files)} Others")
    
    return prioritized

# SCRAPER PHASES
async def phase1_discovery(client: HTTPClient, category: str, search_urls: Dict[str, str]) -> List[Dict]:
    """
    Crawl both "Văn bản pháp quy" AND "Văn bản hợp nhất"
    
    Phase 1: Crawl all search result pages to discover documents
    
    Returns:
        List of basic document info dicts
    """
    logger.info(f"[{category}] Phase 1: Starting discovery...")
    
    all_documents = []
    
    # Crawl BOTH document types
    for doc_type, search_url in search_urls.items():
        logger.info(f"[{category}] Crawling {doc_type}...")
        
        # Fetch page 1 to get total pages
        logger.info(f"[{category}] [{doc_type}] Fetching page 1 to determine total pages...")
        html = await client.fetch(search_url, f"{category} - {doc_type} - Page 1")
        
        if not html:
            logger.error(f"[{category}] [{doc_type}] Failed to fetch page 1")
            continue
        
        # Parse page 1
        result = parse_list_page(html, category, doc_type)
        total_pages = result['total_pages']
        total_docs = result['total_documents']
        all_documents.extend(result['documents'])
        
        logger.info(f"[{category}] [{doc_type}] Found {total_docs} total documents across {total_pages} pages")
        logger.info(f"[{category}] [{doc_type}] Page 1: {len(result['documents'])} documents")
        
        # Crawl remaining pages
        for page in range(2, total_pages + 1):
            # AJAX pagination: add &Page=N parameter
            page_url = f"{search_url}&Page={page}"
            
            logger.info(f"[{category}] [{doc_type}] Fetching page {page}/{total_pages}...")
            html = await client.fetch(page_url, f"{category} - {doc_type} - Page {page}")
            
            if not html:
                logger.error(f"[{category}] [{doc_type}] Failed to fetch page {page}")
                continue
            
            # Parse page
            result = parse_list_page(html, category, doc_type)
            all_documents.extend(result['documents'])
            logger.info(f"[{category}] [{doc_type}] Page {page}: {len(result['documents'])} documents")
    
    logger.info(f"[{category}] Phase 1 complete: Discovered {len(all_documents)} documents")
    return all_documents

async def phase2_extract_metadata(client: HTTPClient, doc: Dict, output_dir: str) -> Optional[Dict]:
    """
    Phase 2: Extract full metadata from Thuộc tính page
    
    Returns:
        Complete metadata dict or None if failed
    """
    item_id = doc['item_id']
    so_ky_hieu = doc['so_ky_hieu']
    
    # Better normalization (handles "Chỉ thị 31/CT-UBND (năm 2008)")
    folder_name = normalize_so_ky_hieu(so_ky_hieu)
    folder_path = os.path.join(output_dir, folder_name)
    metadata_path = os.path.join(folder_path, 'metadata.json')
    
    # Check for duplicate - if exists, merge instead of skip
    is_duplicate = False
    existing_metadata = None
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                existing_metadata = json.load(f)
            is_duplicate = True
            logger.info(f"[{item_id}] Duplicate detected: {so_ky_hieu} - will merge metadata")
        except:
            pass
    
    # If already fully processed and not a duplicate, skip
    if not is_duplicate and should_skip_document(folder_path, metadata_path):
        logger.info(f"[{item_id}] Skipping - already processed: {so_ky_hieu}")
        return None
    
    # Create folder
    os.makedirs(folder_path, exist_ok=True)
    
    # Fetch Thuộc tính page
    logger.info(f"[{item_id}] Fetching metadata: {so_ky_hieu}")
    html = await client.fetch(doc['thuoc_tinh_url'], f"{item_id} - Thuộc tính")
    
    if not html:
        logger.error(f"[{item_id}] Failed to fetch Thuộc tính page")
        # Save error status (only if not duplicate)
        if not is_duplicate:
            error_metadata = {
                'status': 'failure',
                'error': 'Failed to fetch Thuộc tính page',
                'scraped_at': datetime.now(timezone.utc).isoformat(),
                'source': doc
            }
            save_metadata(error_metadata, metadata_path)
        return None
    
    # Parse metadata
    try:
        new_metadata = parse_thuoc_tinh_page(html, doc)
        
        # Merge if duplicate
        if is_duplicate:
            metadata = merge_metadata(existing_metadata, new_metadata)
            logger.info(f"[{item_id}] Merged duplicate metadata: {so_ky_hieu}")
        else:
            metadata = new_metadata
        
        # Save metadata (PDF not downloaded yet)
        save_metadata(metadata, metadata_path)
        logger.info(f"[{item_id}] Metadata saved: {so_ky_hieu}")
        
        return metadata
        
    except Exception as e:
        logger.error(f"[{item_id}] Failed to parse metadata: {e}")
        # Save error status (only if not duplicate)
        if not is_duplicate:
            error_metadata = {
                'status': 'failure',
                'error': f'Parse error: {str(e)}',
                'scraped_at': datetime.now(timezone.utc).isoformat(),
                'source': doc
            }
            save_metadata(error_metadata, metadata_path)
        return None

async def phase3_download_file(client: HTTPClient, metadata: Dict, output_dir: str) -> bool:
    """
    Phase 3: Download file using "Tải về" tab
    Prioritize: PDF > Word > Others
    
    Returns:
        True if successful, False otherwise
    """
    item_id = metadata['source']['item_id']
    so_ky_hieu = metadata['document']['so_ky_hieu']
    
    folder_name = normalize_so_ky_hieu(so_ky_hieu)
    folder_path = os.path.join(output_dir, folder_name)
    metadata_path = os.path.join(folder_path, 'metadata.json')
    
    # Check if already downloaded
    file_info = metadata.get('file', {})
    pdf_path = os.path.join(folder_path, file_info.get('pdf_filename', 'document.pdf'))
    word_path = os.path.join(folder_path, file_info.get('word_filename', 'document.doc'))
    
    if file_info.get('pdf_downloaded') and os.path.exists(pdf_path) and verify_pdf(pdf_path):
        logger.info(f"[{item_id}] PDF already exists: {so_ky_hieu}")
        return True
    
    if file_info.get('word_downloaded') and os.path.exists(word_path) and verify_word(word_path):
        logger.info(f"[{item_id}] Word document already exists: {so_ky_hieu}")
        return True
    
    # Fetch "Bản PDF" page to access "Tải về" dialog
    logger.info(f"[{item_id}] Fetching download page: {so_ky_hieu}")
    pdf_page_url = metadata['source']['pdf_page_url']
    html = await client.fetch(pdf_page_url, f"{item_id} - Download page")
    
    if not html:
        logger.error(f"[{item_id}] Failed to fetch download page")
        metadata['file']['error'] = 'Failed to fetch download page'
        save_metadata(metadata, metadata_path)
        return False
    
    # Extract download links (prioritized)
    download_links = extract_download_links(html, item_id)
    if not download_links:
        logger.warning(f"[{item_id}] No download links found: {so_ky_hieu}")
        metadata['file']['pdf_downloaded'] = False
        metadata['file']['word_downloaded'] = False
        metadata['file']['error'] = 'No download links found'
        save_metadata(metadata, metadata_path)
        return False
    
    # Try downloading files in priority order
    success = False
    extract_success = False  # Track ZIP extraction
    for filename, url in download_links:
        # Determine file type
        is_pdf = filename.lower().endswith('.pdf')
        is_word = filename.lower().endswith(('.doc', '.docx'))
        is_zip = filename.lower().endswith('.zip')
        
        if not is_pdf and not is_word and not is_zip:
            # Skip other file types
            logger.debug(f"[{item_id}] Skipping unsupported file: {filename}")
            continue
        
        # Construct full URL
        if not url.startswith('http'):
            file_url = f"https://vbpl.vn{url}"
        else:
            file_url = url
        
        # Set destination path and file type
        if is_pdf:
            dest_path = pdf_path
            file_type = 'PDF'
        elif is_word:
            dest_path = word_path
            file_type = 'Word'
        else:  # is_zip
            dest_path = os.path.join(folder_path, filename)  # Keep original ZIP filename
            file_type = 'ZIP'
        
        # Download file
        logger.info(f"[{item_id}] Downloading {file_type}: {filename}")
        download_success = await client.download_file(file_url, dest_path, f"{item_id} - {filename}")
        
        if download_success:
            # Verify file
            if is_pdf and not verify_pdf(dest_path):
                logger.error(f"[{item_id}] Downloaded file is not a valid PDF")
                os.remove(dest_path)
                continue
            
            if is_word and not verify_word(dest_path):
                logger.error(f"[{item_id}] Downloaded file is not a valid Word document")
                os.remove(dest_path)
                continue
            
            if is_zip:
                if not verify_zip(dest_path):
                    logger.error(f"[{item_id}] Downloaded file is not a valid ZIP")
                    os.remove(dest_path)
                    continue
                
                # Extract ZIP
                logger.info(f"[{item_id}] Extracting ZIP to {folder_path}")
                extract_success = extract_zip(dest_path, folder_path)
                if not extract_success:
                    logger.error(f"[{item_id}] Failed to extract ZIP")
                    # Keep ZIP file even if extraction fails
            
            # Update metadata
            file_size = os.path.getsize(dest_path)
            file_hash = calculate_file_hash(dest_path)
            
            if is_pdf:
                metadata['file']['pdf_downloaded'] = True
                metadata['file']['pdf_url'] = file_url
                metadata['file']['pdf_size_bytes'] = file_size
                metadata['file']['pdf_hash_sha256'] = file_hash
                metadata['file']['pdf_filename'] = filename  # Use actual filename
            elif is_word:
                metadata['file']['word_downloaded'] = True
                metadata['file']['word_url'] = file_url
                metadata['file']['word_size_bytes'] = file_size
                metadata['file']['word_hash_sha256'] = file_hash
                metadata['file']['word_filename'] = filename  # Use actual filename
            else:  # is_zip
                metadata['file']['zip_downloaded'] = True
                metadata['file']['zip_url'] = file_url
                metadata['file']['zip_size_bytes'] = file_size
                metadata['file']['zip_hash_sha256'] = file_hash
                metadata['file']['zip_filename'] = filename
                metadata['file']['zip_extracted'] = extract_success if is_zip else False
            
            logger.info(f"[{item_id}] {file_type} downloaded successfully: {file_size} bytes")
            success = True
            break  # Stop after first successful download
    
    if not success:
        logger.error(f"[{item_id}] Failed to download any file")
        metadata['file']['error'] = 'All downloads failed or invalid'
    
    # Save updated metadata
    save_metadata(metadata, metadata_path)
    return success

# MAIN SCRAPER
async def scrape_category(category: str, search_urls: Dict[str, str], output_dir: str):
    """Scrape all documents for a category"""
    logger.info(f"Starting scrape for category: {category}")
    logger.info(f"Output directory: {output_dir}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Create HTTP client
    async with aiohttp.ClientSession(headers=CONFIG['headers']) as session:
        client = HTTPClient(session, CONFIG)
        
        # Phase 1: Discovery (Both văn bản pháp quy AND văn bản hợp nhất)
        logger.info(f"[{category}] PHASE 1: DISCOVERY")
        documents = await phase1_discovery(client, category, search_urls)
        
        if not documents:
            logger.error(f"[{category}] No documents discovered, aborting")
            return
        
        logger.info(f"[{category}] Discovery complete: {len(documents)} documents\n")
        
        # Phase 2: Extract Metadata
        logger.info(f"[{category}] PHASE 2: EXTRACT METADATA")
        
        metadata_list = []
        for idx, doc in enumerate(documents, 1):
            logger.info(f"[{category}] Processing {idx}/{len(documents)}: {doc['so_ky_hieu']}")
            metadata = await phase2_extract_metadata(client, doc, output_dir)
            if metadata:
                metadata_list.append(metadata)
        
        logger.info(f"[{category}] Metadata extraction complete: {len(metadata_list)}/{len(documents)} successful\n")
        
        # Phase 3: Download Files 
        logger.info(f"[{category}] PHASE 3: DOWNLOAD FILES")
        
        file_success = 0
        for idx, metadata in enumerate(metadata_list, 1):
            logger.info(f"[{category}] Downloading file {idx}/{len(metadata_list)}: {metadata['document']['so_ky_hieu']}")
            success = await phase3_download_file(client, metadata, output_dir)
            if success:
                file_success += 1
        
        logger.info(f"[{category}] File download complete: {file_success}/{len(metadata_list)} successful\n")
    
    # Final summary
    logger.info(f"[{category}] SCRAPING COMPLETE")
    logger.info(f"Total documents discovered: {len(documents)}")
    logger.info(f"Metadata extracted: {len(metadata_list)}")
    logger.info(f"Files downloaded: {file_success}")

async def main():
    """Main entry point"""
    setup_logging()
    
    logger.info("LEGAL DOCUMENT SCRAPER vbpl.vn")    
    logger.info("Configuration:")
    logger.info(f"  - Delay between requests: {CONFIG['delay_between_requests']}s")
    logger.info(f"  - Max concurrent requests: {CONFIG['max_concurrent_requests']}")
    logger.info(f"  - Max retries: {CONFIG['max_retries']}")
    logger.info(f"  - Output directory: {CONFIG['output_base']}")
    logger.info("")
    
    # Create base output directory
    Path(CONFIG['output_base']).mkdir(parents=True, exist_ok=True)
    
    # Scrape both categories
    for category_code, category_dir in CONFIG['category_dirs'].items():
        search_urls = CONFIG['ajax_search_urls'][category_code]
        output_dir = os.path.join(CONFIG['output_base'], category_dir)
        
        try:
            await scrape_category(category_code, search_urls, output_dir)
        except Exception as e:
            logger.error(f"Fatal error in category {category_code}: {e}", exc_info=True)
    
    logger.info("\n SCRAPING COMPLETE!")
    logger.info(f"Output directory: {CONFIG['output_base']}")

if __name__ == "__main__":
    asyncio.run(main())
