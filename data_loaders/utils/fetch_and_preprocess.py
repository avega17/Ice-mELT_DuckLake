"""
Simplified utilities for fetching datasets from GitHub repositories.

This module contains the essential fetch_from_github function needed by the
Hamilton DOI pipeline for downloading GitHub-hosted datasets.
"""

import os
import re
import tempfile
import shutil
import subprocess
import urllib.parse
from pathlib import Path

import requests
from tqdm import tqdm


def fetch_from_github(doi, dst, max_mb=100, force=False):
    """
    Fetch dataset files from GitHub repositories.
    
    Args:
        doi (str): GitHub URL of the dataset
        dst (str): Path to save the dataset
        max_mb (int): Maximum file size in MB
        force (bool): Force download even if files exist
        
    Returns:
        dict: A dictionary with dataset information
    """
    max_dl = max_mb * 1024 * 1024
    dst_p = Path(dst)
    geofile_regex = r'^(.*\.(geojson|json|shp|zip|csv|gpkg))$'
    
    # Create destination directory if it doesn't exist
    os.makedirs(dst, exist_ok=True)
    
    # Check if local path exists and contains expected files
    if os.path.exists(dst) and any(os.path.splitext(fname)[1] in ['.geojson', '.json', '.shp', '.zip'] 
                                  for fname in os.listdir(dst)) and not force:
        print(f"Destination path already exists and contains expected files.")
        print(f"\033[1mSkipping Download!\033[0m")
        # Get dataset dir info
        ds_files = [os.path.join(root, fname) for root, dirs, files in os.walk(dst_p) 
                   for fname in files if re.match(geofile_regex, fname)]
        return {
            'output_folder': dst,
            'files': ds_files,
            'fs_tree': f"Directory: {dst} ({len(ds_files)} files)"
        }
    
    # Parse the GitHub URL
    parts = doi.replace('https://github.com/', '').split('/')
    repo_path = f"{parts[0]}/{parts[1]}"
    
    # Extract branch and path
    branch = 'main'  # Default branch
    path = ''
    
    # Check if it's a folder/repository or a single file
    if '/blob/' not in doi and 'raw.githubusercontent.com' not in doi:
        try:
            if 'tree' in parts:
                tree_index = parts.index('tree')
                branch = parts[tree_index + 1]
                path = '/'.join(parts[tree_index + 2:]) if len(parts) > tree_index + 2 else ''
            
            # Create a temporary directory for the sparse checkout
            with tempfile.TemporaryDirectory() as temp_dir:
                # Initialize git repository with sparse checkout
                cmd = f"git clone --filter=blob:limit={max_mb}m --depth 1 https://github.com/{repo_path}.git {os.path.basename(dst)}"
                
                process = subprocess.run(cmd, shell=True, cwd=temp_dir, 
                                        capture_output=True, text=True)
                if process.returncode != 0:
                    raise Exception(f"Git command failed: {cmd}\n{process.stderr}")
                
                # Copy only the files in the dir specified in DOI/URL
                repo_ds_dir = os.path.join(temp_dir, os.path.basename(dst), path) if path else os.path.join(temp_dir, os.path.basename(dst))
                files_list = []
                
                for root, _, files in os.walk(repo_ds_dir):
                    for file in files:
                        if file.startswith('.git'):
                            continue
                        src_file = os.path.join(root, file)
                        # Create relative path
                        rel_path = os.path.relpath(src_file, repo_ds_dir)
                        dst_file = os.path.join(dst, rel_path)
                        
                        # Create destination directory if needed
                        os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                        
                        # Copy the file
                        shutil.copy2(src_file, dst_file)
                        files_list.append(dst_file)
                        print(f"Copied {rel_path} to {os.path.relpath(dst_file)}")
            
            ds_files = [f for f in files_list if re.match(geofile_regex, f)]
            return {
                'output_folder': dst,
                'files': ds_files,
                'fs_tree': f"Directory: {dst} ({len(ds_files)} files)"
            }
                
        except Exception as e:
            print(f"Error performing git clone: {e}")
            return None
    else:
        # It's a single file (raw URL or blob URL)
        try:
            # Convert blob URL to raw URL if needed
            if '/blob/' in doi:
                raw_url = doi.replace('github.com', 'raw.githubusercontent.com').replace('/blob/', '/')
            else:
                raw_url = doi
            
            # Extract filename from URL
            filename = os.path.basename(urllib.parse.urlparse(raw_url).path)
            local_file_path = os.path.join(dst, filename)
            
            # Download the file
            response = requests.get(raw_url, stream=True)
            response.raise_for_status()
            
            # Check file size
            file_size = int(response.headers.get('content-length', 0))
            if file_size > max_dl:
                print(f"File size ({file_size} bytes) exceeds maximum allowed size ({max_dl} bytes)")
                return None
            
            with open(local_file_path, 'wb') as f:
                for chunk in tqdm(response.iter_content(chunk_size=8192), desc=f"Downloading {filename}", unit='KB'):
                    f.write(chunk)
            print(f"Downloaded {filename} to {os.path.relpath(local_file_path)}")
            
            return {
                'output_folder': dst,
                'files': [local_file_path],
                'fs_tree': f"Directory: {dst} (1 file)"
            }
            
        except Exception as e:
            print(f"Error downloading GitHub file: {e}")
            return None
