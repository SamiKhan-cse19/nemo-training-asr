from huggingface_hub import snapshot_download
import os, glob, zipfile, concurrent.futures
import ipywidgets as widgets
from IPython.display import display

repo_to_download = "kabir5297/OldDumpsDatasets"
zips_dir = "DATA/zips"
zip_extraction_dir = "./"

snapshot_download(repo_id=repo_to_download, repo_type="dataset", local_dir=zips_dir, max_workers=os.cpu_count(), resume_download=True)

zip_file_names = glob.glob(f"{zips_dir}/*.zip")

extraction_tasks = {}
for zip_file in zip_file_names:
    extraction_tasks[zip_file] = zip_extraction_dir

def extract_zip(zip_path, extract_dir, progress_widget):
    """Extract a zip file to specified directory with ipywidgets progress bar."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get list of all files to extract
            all_files = zip_ref.namelist()
            total_size = sum(zip_ref.getinfo(file).file_size for file in all_files)
            
            # Create extraction directory if it doesn't exist
            os.makedirs(extract_dir, exist_ok=True)
            
            # Set up progress bar maximum value
            progress_widget.max = total_size
            
            # Extract each file and update progress
            extracted_size = 0
            for file in all_files:
                zip_ref.extract(file, extract_dir)
                extracted_size += zip_ref.getinfo(file).file_size
                progress_widget.value = extracted_size
                progress_widget.description = f"{int(100 * extracted_size / total_size)}%"
                
        return f"Successfully extracted {zip_path}"
    except Exception as e:
        return f"Failed to extract {zip_path}: {e}"

def extract_multiple_zips(zip_files):
    """Extract multiple zip files with individual progress bars."""
    # Create progress widgets for each ZIP file
    progress_widgets = {}
    for zip_path, extract_dir in zip_files.items():  # <-- FIXED
        file_name = os.path.basename(zip_path)
        label = widgets.HTML(value=f"<b>{file_name}</b>")
        progress = widgets.FloatProgress(
            value=0, 
            min=0, 
            description="0%",
            bar_style='info',
            style={'bar_color': '#0078D7'},
            layout=widgets.Layout(width='90%')
        )
        
        # Create container for the label and progress bar
        container = widgets.VBox([label, progress])
        display(container)
        
        progress_widgets[zip_path] = progress
    
    # Using ThreadPoolExecutor for I/O-bound operations
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Submit all extraction tasks and collect futures
        futures = [
            executor.submit(extract_zip, zip_path, extract_dir, progress_widgets[zip_path]) 
            for zip_path, extract_dir in zip_files.items()  # <-- FIXED
        ]
        
        # Wait for all futures to complete and get results
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    # Display completion status
    summary = widgets.HTML(value="<b>All extractions completed</b>")
    display(summary)
    
    return results


results = extract_multiple_zips(extraction_tasks)

os.remove(zips_dir)