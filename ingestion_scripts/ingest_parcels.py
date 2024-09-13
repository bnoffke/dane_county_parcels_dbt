#%%
import requests
import zipfile
import io
import os
import shutil
from dbfread import DBF
import pandas as pd
import duckdb
from pathlib import Path
import re
from datetime import datetime

#Thoughts
# Do a schema compare task for each dataframe after dropping name columns, compared to the destination table
#%%
# Define ROOT constant
ROOT = Path(os.getenv("DATA_PATH"))
ARCHIVE_DIR = ROOT / "archive"
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_SCRIPTS_PATH = os.path.join(CURRENT_DIR, '..', 'sql_scripts')
CREATE_TABLE_SCRIPT = Path(SQL_SCRIPTS_PATH) / 'create_table_parcels_raw.sql'
DEST_TABLE = 'raw.parcels_raw'
EXCLUDE_FIELDS = ['Owner','CoOwner','ConctOwner']
#%%

def ensure_dir(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)

def find_parcel_zip_files():
    zip_files = []
    for file in os.listdir(ROOT):
        if file.startswith("Parcels") and file.endswith(".zip"):
            year_match = re.search(r'Parcels(\d{4})\.zip', file)
            if year_match:
                year = int(year_match.group(1))
            else:
                year = datetime.now().year
            zip_files.append({'file':file, 'year_number':year})
    return zip_files

def extract_zip(zip_file, year):
    extract_dir = ROOT / f"parcels_{year}"
    try:
        ensure_dir(extract_dir)
        with zipfile.ZipFile(ROOT / zip_file) as zip_ref:
            zip_ref.extractall(extract_dir)
        print(f"Successfully extracted zip file for year {year}")
        
        print(f"Contents of {extract_dir}:")
        for file in os.listdir(extract_dir):
            print(f"  {file}")
    except zipfile.BadZipFile:
        print(f"Error: The file for year {year} is not a valid zip file")
    except Exception as e:
        print(f"Error extracting zip file for year {year}: {e}")

def find_dbf_file(year):
    extract_dir = ROOT / f"parcels_{year}"
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            if file.lower().endswith('.dbf'):
                if file.startswith("GISdw_DCL_TaxParcelPoly"):
                    return Path(root) / file
                elif file.lower().startswith('parcels'):
                    return Path(root) / file
    return None

def dbf_to_dataframe(dbf_path):
    encodings = ['utf-8', 'latin-1', 'ascii', 'iso-8859-1']
    skipped_records = 0
    for encoding in encodings:
        try:
            dbf = DBF(dbf_path, encoding=encoding)
            result = []
            for record in dbf:
                # Extract the PARCELNO value (handle different column names)
                parcel_no = record.get('PARCELNO') or record.get('PARCEL_NUM') or record.get('PARCEL_NO')
                
                if not parcel_no or parcel_no == '':
                    skipped_records += 1
                    continue
                
                # Create the attributes dictionary (excluding PARCELNO and any in the exclude_fields list)
                attributes = {
                    key: value for key, value in record.items() 
                    if key not in ['PARCELNO', 'PARCEL_NUM', 'PARCEL_NO'] + EXCLUDE_FIELDS
                }
                
                # Create the final dictionary structure
                result.append({
                    'PARCELNO': parcel_no,
                    'ATTRIBUTES': attributes
                })
            print(f"Successfully read DBF file using {encoding} encoding, skipped {skipped_records} due to missing parcel no.")
            return pd.DataFrame(result)
        except UnicodeDecodeError:
            print(f"Failed to read DBF file with {encoding} encoding, trying next...")
        except Exception as e:
            print(f"Error reading DBF file {dbf_path} with {encoding} encoding: {e}")
    
    print(f"Failed to read DBF file {dbf_path} with all attempted encodings")
    return None

def process_year(zip_file, year):
    extract_zip(zip_file, year)
    
    dbf_file = find_dbf_file(year)
    if dbf_file is None:
        print(f"DBF file not found for year {year}")
        return None
    
    print(f"Found DBF file: {dbf_file}")
    df = dbf_to_dataframe(dbf_file)
    if df is not None:
        df['year_number'] = year
        df['load_dttm'] = datetime.now()
    return df

def archive_zip_file(zip_file, year):
    ensure_dir(ARCHIVE_DIR)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    new_filename = f"Parcels{year}_{timestamp}.zip"
    shutil.move(ROOT / zip_file, ARCHIVE_DIR / new_filename)
    print(f"Moved {zip_file} to archive as {new_filename}")

def clean_up_data_path():
    for item in os.listdir(ROOT):
        item_path = ROOT / item
        if item_path.is_dir() and item.startswith("parcels_"):
            shutil.rmtree(item_path)
            print(f"Removed directory: {item_path}")
        elif item_path.is_file() and item.startswith("Parcel") and item.endswith(".zip"):
            os.remove(item_path)
            print(f"Removed file: {item_path}")
#%%

#%%
def main():
    db_path = ROOT / 'munincipal_data.db'
    conn = duckdb.connect(str(db_path))
    
    # Read SQL file and execute
    with open(CREATE_TABLE_SCRIPT, 'r') as file:
        parcel_data_sql = file.read()
    conn.execute(parcel_data_sql)
    
    zip_files = find_parcel_zip_files()
    
    for zip_file_info in zip_files:
        year = zip_file_info['year_number']
        zip_file = zip_file_info['file']
        df = process_year(zip_file, year)
        if df is not None and not df.empty:
            conn.execute(f"INSERT INTO {DEST_TABLE} SELECT * FROM df")
            print(f'Loaded {len(df)} parcel records for {year}.')
            archive_zip_file(zip_file, year)
        
        # Clean up extracted files
        shutil.rmtree(ROOT / f"parcels_{year}", ignore_errors=True)
    
    conn.close()

if __name__ == "__main__":
    main()
#%%
