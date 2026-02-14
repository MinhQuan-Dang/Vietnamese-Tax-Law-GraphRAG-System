"""
Converts old .doc files (Word 97-2003) to Markdown using 2-step process:
1. Convert .doc → .docx using python-docx or win32com
2. Convert .docx → .md using markitdown

Author: quandm
Date: 2026-02-05
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from markitdown import MarkItDown

# Try to import win32com for .doc conversion
try:
    import win32com.client
    HAS_WIN32COM = True
except ImportError:
    HAS_WIN32COM = False
    print("Warning: win32com not available. Will only process .docx files.")
    print("Install: pip install pywin32")

# CONFIGURATION
SOURCE_DIR = 'raw_data/word_docs'
OUTPUT_DIR = '../markdown_docs'
TEMP_DIR = './temp_docx'  # Temporary folder for converted .docx files

# UTILITY FUNCTIONS
def ensure_directories():
    """
    Create output directories
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR,exist_ok=True)
    print(f"Output: {os.path.abspath(OUTPUT_DIR)}")
    print(f"Temp: {os.path.abspath(TEMP_DIR)}\n")


def convert_doc_to_docx(doc_path, docx_path):
    """
    Convert .doc to .docx using Word COM automation (Windows only)
    
    Args:
        doc_path: Path to .doc file
        docx_path: Path to output .docx file
    
    Returns:
        True if successful, False otherwise
    """
    if not HAS_WIN32COM:
        return False
    
    try:
        # Absolute paths required for COM
        doc_path_abs = os.path.abspath(doc_path)
        docx_path_abs = os.path.abspath(docx_path)
        
        # Start Word application
        word = win32com.client.Dispatch('Word.Application')
        word.Visible = False
        
        try:
            # Open .doc file
            doc = word.Documents.Open(doc_path_abs)
            
            # Save as .docx (format code 16 = docx)
            doc.SaveAs2(docx_path_abs, FileFormat=16)
            
            # Close document
            doc.Close()
            
            return True
        
        finally:
            # Always quit Word
            word.Quit()
    
    except Exception as e:
        print(f"      Error converting to .docx: {e}")
        return False


def convert_to_markdown(input_path, output_path, md_converter):
    """
    Convert Word file to Markdown using markitdown
    
    Args:
        input_path: Path to .doc/.docx file
        output_path: Path to output .md file
        md_converter: MarkItDown instance
    
    Returns:
        Tuple of (success: bool, error_message: str or None)
    """
    try:
        result = md_converter.convert(input_path)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result.text_content)

        if os.path.exists(output_path):
            return True, None
        else:
            return False, "Output file was not created"
    except Exception as e:
        return False, str(e)


def process_file(input_path, md_converter):
    """
    Process a single Word file (.doc or .docx)
    
    Strategy:
    - If .docx: Convert directly to markdown
    - If .doc: Convert to .docx first, then to markdown
    
    Returns:
        Tuple of (success: bool, error_message: str or None)
    """
    filename = os.path.basename(input_path)
    base_name = os.path.splitext(filename)[0]
    extension = os.path.splitext(filename)[1].lower()

    # Output markdown file
    output_md = os.path.join(OUTPUT_DIR, f"{base_name}.md")

    # Strategy based on file type
    if extension == '.docx':
        # Convert to markdown directly
        return convert_to_markdown(input_path, output_md, md_converter)
    
    elif extension == '.doc':
        if not HAS_WIN32COM:
            return False, "Cannot covert .doc files (win32com not available)"




        
        # Two-step conversion: .doc → .docx → .md
        temp_docx = os.path.join(TEMP_DIR, f"{base_name}.docx")
        
        # Step 1: Convert to .docx
        print(f"Converting .doc → .docx...")
        if not convert_doc_to_docx(input_path, temp_docx):
            return False, "Failed to convert .doc to .docx"
        
        # Step 2: Convert .docx to markdown
        print(f"Converting .docx → .md...")
        success, error = convert_to_markdown(temp_docx, output_md, md_converter)
        
        # Cleanup temp file
        try:
            if os.path.exists(temp_docx):
                os.remove(temp_docx)
        except:
            pass
        
        return success, error
    
    else:
        return False, f"Unsupported file type: {extension}"

def get_word_files(source_dir):
    """
    Get all .doc and .docx files
    """
    files = []
    for filename in os.listdir(source_dir):
        if filename.lower().endswith(('.doc', '.docx')):
            filepath = os.path.join(source_dir, filename)
            if os.path.isfile(filepath):
                files.append(filepath)
    files.sort()
    return files

# MAIN
def main():
    """Main conversion process"""
    print("WORD TO MARKDOWN CONVERTER (with Legacy .doc Support)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Check prerequisites
    if not HAS_WIN32COM:
        print("IMPORTANT: win32com not available!")
        print("This means .doc files (Word 97-2003) cannot be converted.")
        print("Only .docx files will be processed.")
        print("To enable .doc support: pip install pywin32\n")
        
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            print("Aborted.")
            return
        print()
    
    # Setup
    ensure_directories()
    
    # Get files
    print(f"Scanning: {os.path.abspath(SOURCE_DIR)}")
    word_files = get_word_files(SOURCE_DIR)
    
    if not word_files:
        print("No Word files found!")
        return
    
    # Count by type
    doc_files = [f for f in word_files if f.lower().endswith('.doc')]
    docx_files = [f for f in word_files if f.lower().endswith('.docx')]
    
    print(f"Found {len(word_files)} files:")
    print(f"    .doc:  {len(doc_files)}")
    print(f"    .docx: {len(docx_files)}\n")
    
    # Initialize converter
    print("Initializing markitdown...")
    md = MarkItDown()
    print("Ready\n")
    
    # Process files
    print("CONVERTING FILES")   
    success_count = 0
    error_count = 0
    errors = []
    
    for idx, input_path in enumerate(word_files, 1):
        filename = os.path.basename(input_path)
        file_size_mb = os.path.getsize(input_path) / (1024 * 1024)
        
        print(f"[{idx}/{len(word_files)}] {filename} ({file_size_mb:.2f} MB)")
        
        # Process
        success, error_msg = process_file(input_path, md)
        
        if success:
            base_name = os.path.splitext(filename)[0]
            output_file = f"{base_name}.md"
            output_path = os.path.join(OUTPUT_DIR, output_file)
            output_size_kb = os.path.getsize(output_path) / 1024
            
            print(f"SUCCESS → {output_file} ({output_size_kb:.1f} KB)\n")
            success_count += 1
        else:
            print(f"FAILED: {error_msg}\n")
            error_count += 1
            errors.append((filename, error_msg))
    
    # Cleanup temp directory
    try:
        if os.path.exists(TEMP_DIR) and not os.listdir(TEMP_DIR):
            os.rmdir(TEMP_DIR)
    except:
        pass
    
    # Summary
    print("SUMMARY")
    print(f"Total files:    {len(word_files)}")
    print(f"    .doc files:   {len(doc_files)}")
    print(f"    .docx files:  {len(docx_files)}")
    print(f"\nConverted:      {success_count}")
    print(f"Failed:         {error_count}")
    print(f"Success rate:   {success_count/len(word_files)*100:.1f}%")
    
    if errors:
        print(f"\nFAILED FILES ({len(errors)}):")
        
        # Group by error type
        error_groups = {}
        for filename, error_msg in errors:
            if error_msg not in error_groups:
                error_groups[error_msg] = []
            error_groups[error_msg].append(filename)
        
        for error_msg, filenames in error_groups.items():
            print(f"\n  {error_msg} ({len(filenames)} files)")
            for f in filenames[:5]:  # Show first 5
                print(f"    - {f}")
            if len(filenames) > 5:
                print(f"    ... and {len(filenames) - 5} more")
    
    print(f"\nOutput: {os.path.abspath(OUTPUT_DIR)}")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if error_count == 0:
        print("\nSUCCESS: All files converted!")
    elif success_count > 0:
        print(f"\nPARTIAL SUCCESS: {success_count}/{len(word_files)} files converted")
    else:
        print("\nFAILED: No files converted")

if __name__ == '__main__':
    main()
