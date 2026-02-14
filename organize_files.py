"""
Copies all PDF and Word files from output directory to organized raw_data directory

Structure:
- PDFs -> raw_data/pdfs_docs/
- Word -> raw_data/word_docs/

Author: quandm
Date: 2026-02-05
"""

import os
import shutil
from pathlib import Path
from datetime import datetime

# CONFIGURATION

# Directories
SOURCE_DIR = 'output'
TARGET_BASE = 'raw_data'
PDF_DIR = os.path.join(TARGET_BASE, 'pdfs_docs')
WORD_DIR = os.path.join(TARGET_BASE, 'word_docs')

# File extensions
PDF_EXTENSIONS = ['.pdf']
WORD_EXTENSIONS = ['.doc', '.docx']

# MAIN SCRIPT

def ensure_directories():
    """Create target directories if they don't exist"""
    os.makedirs(PDF_DIR, exist_ok=True)
    os.makedirs(WORD_DIR, exist_ok=True)
    print(f"✓ Ensured directories exist:")
    print(f"  - {PDF_DIR}")
    print(f"  - {WORD_DIR}")

def get_all_files(source_dir, extensions):
    """
    Recursively find all files with given extensions
    
    Args:
        source_dir: Root directory to search
        extensions: List of file extensions (e.g., ['.pdf', '.doc'])
    
    Returns:
        List of file paths
    """
    files = []
    
    for root, dirs, filenames in os.walk(source_dir):
        for filename in filenames:
            # Check if file has target extension
            if any(filename.lower().endswith(ext) for ext in extensions):
                filepath = os.path.join(root, filename)
                files.append(filepath)
    
    return files

def copy_files(source_files, target_dir, file_type):
    """
    Copy files to target directory
    
    Args:
        source_files: List of source file paths
        target_dir: Destination directory
        file_type: Description for logging (e.g., 'PDF', 'Word')
    
    Returns:
        Tuple of (success_count, error_count)
    """
    success_count = 0
    error_count = 0
    
    print(f"COPYING {file_type} FILES")
    print(f"Found {len(source_files)} {file_type} files")
    print(f"Target: {target_dir}\n")
    
    for idx, source_path in enumerate(source_files, 1):
        filename = os.path.basename(source_path)
        target_path = os.path.join(target_dir, filename)
        
        # Handle duplicate filenames
        if os.path.exists(target_path):
            # Check if files are identical
            if os.path.getsize(source_path) == os.path.getsize(target_path):
                print(f"[{idx}/{len(source_files)}] SKIP (duplicate): {filename}")
                success_count += 1
                continue
            else:
                # Add number suffix for different file
                base, ext = os.path.splitext(filename)
                counter = 1
                while os.path.exists(target_path):
                    new_filename = f"{base}_{counter}{ext}"
                    target_path = os.path.join(target_dir, new_filename)
                    counter += 1
                print(f"[{idx}/{len(source_files)}] RENAME: {filename} -> {os.path.basename(target_path)}")
        
        try:
            # Copy file
            shutil.copy2(source_path, target_path)
            
            # Verify copy
            if os.path.exists(target_path):
                size_mb = os.path.getsize(target_path) / (1024 * 1024)
                print(f"[{idx}/{len(source_files)}] COPIED: {filename} ({size_mb:.2f} MB)")
                success_count += 1
            else:
                print(f"[{idx}/{len(source_files)}] FAILED: {filename} (verification failed)")
                error_count += 1
        
        except Exception as e:
            print(f"[{idx}/{len(source_files)}] ERROR: {filename} - {e}")
            error_count += 1
    
    return success_count, error_count

def main():
    """Main execution"""
    print("FILE ORGANIZER - PDF & Word to raw_data")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Check if source directory exists
    if not os.path.exists(SOURCE_DIR):
        print(f"ERROR: Source directory '{SOURCE_DIR}' does not exist!")
        return
    
    # Ensure target directories exist
    ensure_directories()
    
    # Find all PDF files
    print(f"\nScanning for PDF files in '{SOURCE_DIR}'...")
    pdf_files = get_all_files(SOURCE_DIR, PDF_EXTENSIONS)
    
    # Find all Word files
    print(f"Scanning for Word files in '{SOURCE_DIR}'...")
    word_files = get_all_files(SOURCE_DIR, WORD_EXTENSIONS)
    
    # Copy PDF files
    pdf_success, pdf_errors = copy_files(pdf_files, PDF_DIR, 'PDF')
    
    # Copy Word files
    word_success, word_errors = copy_files(word_files, WORD_DIR, 'Word')
    
    # Summary
    print("SUMMARY")
    print(f"PDF Files:")
    print(f"  Found:    {len(pdf_files)}")
    print(f"  Copied:   {pdf_success}")
    print(f"  Errors:   {pdf_errors}")
    print(f"\nWord Files:")
    print(f"  Found:    {len(word_files)}")
    print(f"  Copied:   {word_success}")
    print(f"  Errors:   {word_errors}")
    print(f"\nTotal:")
    print(f"  Found:    {len(pdf_files) + len(word_files)}")
    print(f"  Copied:   {pdf_success + word_success}")
    print(f"  Errors:   {pdf_errors + word_errors}")
    print(f"\nTarget locations:")
    print(f"  PDFs:  {os.path.abspath(PDF_DIR)}")
    print(f"  Word:  {os.path.abspath(WORD_DIR)}")
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if pdf_errors > 0 or word_errors > 0:
        print(f"\n  WARNING: {pdf_errors + word_errors} file(s) had errors!")
    else:
        print(f"\n SUCCESS: All files copied successfully!")

if __name__ == '__main__':
    main()
