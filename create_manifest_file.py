import csv
import json
import os
import torchaudio
import multiprocessing
from tqdm import tqdm
import sys

# Increase CSV field size limit
csv.field_size_limit(sys.maxsize)
# Define input and output file paths
csv_file = "DATA/train_valid_without_errs.csv"  # Replace with your actual CSV file path
manifest_file = "DATA/nemo_manifest.json"

def get_audio_duration(file_path):
    try:
        waveform, sample_rate = torchaudio.load(file_path)
        return waveform.shape[1] / sample_rate
    except Exception as e:
        print(f"Warning: Could not get duration for {file_path}: {e}")
        return 0.0

def process_row(row):
    file_path = row["file_name"].strip()
    transcript = row["transcripts"].strip()
    file_path = os.abspath(os.path.join("DATA", file_path))
    # Skip rows with missing or erroneous data
    if not file_path or not transcript:
        return None
    
    # Get audio duration
    duration = get_audio_duration(file_path)
    
    return {
        "audio_filepath": file_path,
        "duration": duration,
        "text": transcript,
        "target_lang": "bn"
    }

def convert_csv_to_nemo_manifest(csv_path, manifest_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
    
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = list(tqdm(pool.imap(process_row, reader), total=len(reader), desc="Processing"))
    
    # Remove None values (skipped rows)
    manifest_data = [entry for entry in results if entry is not None]
    
    # Write to manifest file
    with open(manifest_path, "w", encoding="utf-8") as f:
        for entry in manifest_data:
            json.dump(entry, f, ensure_ascii=False)
            f.write("\n")

    print(f"Manifest file saved as {manifest_path}")

# Run conversion
convert_csv_to_nemo_manifest(csv_file, manifest_file)