import requests
import pandas as pd
import io
import os
from datetime import datetime

# --- 1. Date Logic ---
now = datetime.now()
go_back = 2  # Adjust this to 1 if you want the immediate previous month
last_month_date = datetime(now.year, now.month, 1) - pd.DateOffset(months=go_back)

L_YEAR = last_month_date.year
L_MONTH_NAME = last_month_date.strftime("%B")
L_MONTH_STR = last_month_date.strftime("%m")
DATE_KEY = f"{L_YEAR}-{L_MONTH_STR}"

# --- 2. Configuration ---
# MASTER_FILE = "C:\Users\Admin\OneDrive\4. Data\4.9. Raw_Customs\Data_Srcaping_2026\Export-Import Customs_31072025.xlsx"
MASTER_FILE = "C:/Users/Admin/OneDrive/4. Data/4.9. Raw_Customs/Data_Srcaping_2026/Export-Import Customs_31072025.xlsx"
API_TOKEN = "fHs4RbfpSE57qfYM"
BASE_URL = "https://stats.customs.gov.kh/api/file/download"

# Mapping target categories to their specific files and sheet names
FILE_MAP = {
    "Export": {
        "files": [f"{DATE_KEY}-TSC-EN-EX.xlsx", f"{DATE_KEY}-TSCC-EN-EX.xlsx"], 
        "sheet": "Data_Matrix_Export "
    },
    "Import": {
        "files": [f"{DATE_KEY}-TSC-EN-IM.xlsx", f"{DATE_KEY}-TSCC-EN-IM.xlsx"], 
        "sheet": "DataMatrix_Import "
    }
}

print(f"📅 Targeting Data: {L_MONTH_NAME} {L_YEAR} (Key: {DATE_KEY})")

def clean_raw_data(content):
    """Extracts columns 1 and 4 from the source Excel and removes headers/footers."""
    df = pd.read_excel(io.BytesIO(content), header=None)
    # iloc[6:-3] skips the top 6 rows and bottom 3 rows of the source file
    df_clean = df.iloc[6:-2].reset_index(drop=True)
    return df_clean[[1, 4]].dropna()

def transform_to_standard(df, title, filename):
    """Converts raw scrapped data into the Master File's standardized format."""
    standard_rows = []
    is_tsc = "TSC-" in filename
    indicator_type = "Main market" if is_tsc else "Main product"

    for _, row in df.iterrows():
        desc = str(row.iloc[0]).strip()
        val = row.iloc[1]

        # Filter out sub-totals or headers found within the data body
        if desc.lower() in ["country", "description", "total", "grand total"] or desc.upper().startswith("TOTAL"):
            continue

        standard_rows.append({
            "No.": 0, "Tittle ": title, "Country": "Cambodia", "Update frequency ": "Monthly",
            "Status": "Real", "Yearly": L_YEAR, "Monthly": L_MONTH_NAME,
            "Indecator": indicator_type, "Sub1": desc if is_tsc else ".", "Sub2": desc if not is_tsc else ".",
            "Sub3": ".", "Sub4": ".", "Sub5": ".", "Sub6": ".", "Unit": "(Value in Thousand US $)",
            "Value": val, "Accesss Date": datetime.now().strftime("%m/%d/%Y"), "Pubilsh Date": "",
            "Link(if avilable)": "https://stats.customs.gov.kh/en/publication", "Note": ""
        })
    return pd.DataFrame(standard_rows)

def main():
    headers = {"User-Agent": "Mozilla/5.0", "x-api-token": API_TOKEN}

    if not os.path.exists(MASTER_FILE):
        print(f"❌ Error: {MASTER_FILE} not found.")
        return

    # 1. Load EVERY sheet from the workbook into a dictionary
    try:
        all_sheets = pd.read_excel(MASTER_FILE, sheet_name=None)
        print(f"📂 Loaded {len(all_sheets)} sheets from master file.")
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return

    # Create a copy to hold our updates while keeping other sheets intact
    sheets_to_write = all_sheets.copy()

    for category, config in FILE_MAP.items():
        target_name = config["sheet"].strip()
        
        # Find actual sheet name (matching case and spaces)
        actual_sheet_name = next((s for s in all_sheets.keys() if s.strip() == target_name), None)

        if not actual_sheet_name:
            print(f"⚠️ Sheet '{target_name}' not found in Excel. Skipping {category}.")
            continue

        existing_df = all_sheets[actual_sheet_name]

        # --- Duplicate Check ---
        if not existing_df.empty and 'Yearly' in existing_df.columns:
            # Check if Year and Month combination already exists
            duplicate_rows = existing_df[
                (existing_df['Yearly'].astype(str).str.strip() == str(L_YEAR)) &
                (existing_df['Monthly'].astype(str).str.strip().str.lower() == L_MONTH_NAME.lower())
            ]

            if not duplicate_rows.empty:
                print(f"🛑 SKIP: Data for {L_MONTH_NAME} {L_YEAR} already exists in '{actual_sheet_name}'.")
                continue

            # Calculate next ID number
            last_no = pd.to_numeric(existing_df.iloc[:, 0], errors='coerce').max()
            last_no = 0 if pd.isna(last_no) else last_no
        else:
            last_no = 0

        # --- Scrapping Logic ---
        print(f"🚀 Processing {category}s...")
        new_data_list = []
        for fname in config["files"]:
            params = {"filePath": f"/ePay-SAN/statistics/trade/{L_YEAR}/excel/{fname}", "filename": fname}
            try:
                res = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
                if res.status_code == 200:
                    new_data_list.append(transform_to_standard(clean_raw_data(res.content), category, fname))
                    print(f"   ✅ Fetched {fname}")
                else:
                    print(f"   ❌ Failed to fetch {fname} (Status: {res.status_code})")
            except Exception as e:
                print(f"   ❌ Connection error for {fname}: {e}")

        if new_data_list:
            new_df = pd.concat(new_data_list, ignore_index=True)
            # Generate sequential "No." column
            new_df['No.'] = range(int(last_no) + 1, int(last_no) + 1 + len(new_df))

            # Append new data to the existing sheet data
            updated_df = pd.concat([existing_df, new_df], ignore_index=True)
            sheets_to_write[actual_sheet_name] = updated_df
            print(f"🎉 Successfully added {len(new_df)} rows to {actual_sheet_name}")

    # 3. Save everything back to the Master File
    try:
        with pd.ExcelWriter(MASTER_FILE, engine='openpyxl', mode='w') as writer:
            for sheet_name, df in sheets_to_write.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"\n✅ SUCCESS: All data and original sheets written to {MASTER_FILE}")
    except PermissionError:
        print(f"\n❌ ERROR: Could not save file. Please close '{MASTER_FILE}' and try again.")
    except Exception as e:
        print(f"\n❌ ERROR saving file: {e}")

if __name__ == "__main__":
    main()
