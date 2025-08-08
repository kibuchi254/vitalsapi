import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
import io
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_excel_file(file_content: bytes) -> List[Dict[str, Any]]:
    """
    Parse Excel file content with multiple sheets and extract birth records.
    Handles problematic Excel structures, missing headers, and data inconsistencies.
    """
    try:
        # Read all sheets from Excel file
        xls = pd.ExcelFile(io.BytesIO(file_content))
        all_records = []

        # Define expected column order and mapping
        EXPECTED_COLUMNS = [
            'record_date',
            'ip_number', 
            'mother_name',
            'admission_date',
            'discharge_date',
            'date_of_birth',
            'gender',
            'mode_of_delivery',
            'child_name',
            'father_name',
            'birth_notification_no'
        ]

        for sheet_name in xls.sheet_names:
            logger.info(f"Processing sheet: {sheet_name}")
            try:
                # Try multiple approaches to read the Excel sheet
                df = None
                parsing_method = "unknown"
                
                # Method 1: Try reading with first row as header
                try:
                    df_test = pd.read_excel(xls, sheet_name=sheet_name, header=0)
                    if not df_test.empty and has_meaningful_headers(df_test.columns):
                        df = df_test
                        parsing_method = "header_row_0"
                        logger.info(f"Successfully parsed with header at row 0")
                except Exception as e:
                    logger.debug(f"Method 1 failed: {e}")

                # Method 2: Try reading without header and detect data start
                if df is None:
                    try:
                        df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                        df, parsing_method = detect_and_parse_data(df_raw)
                        if df is not None:
                            logger.info(f"Successfully parsed using method: {parsing_method}")
                    except Exception as e:
                        logger.debug(f"Method 2 failed: {e}")

                # Method 3: Try reading from different header rows
                if df is None:
                    for header_row in [1, 2, 3]:
                        try:
                            df_test = pd.read_excel(xls, sheet_name=sheet_name, header=header_row)
                            if not df_test.empty and has_meaningful_headers(df_test.columns):
                                df = df_test
                                parsing_method = f"header_row_{header_row}"
                                logger.info(f"Successfully parsed with header at row {header_row}")
                                break
                        except Exception as e:
                            logger.debug(f"Header row {header_row} failed: {e}")

                if df is None or df.empty:
                    logger.warning(f"Could not parse sheet {sheet_name}, skipping")
                    continue

                logger.info(f"Parsing method used: {parsing_method}")
                logger.info(f"Original columns found: {list(df.columns)}")

                # Clean and standardize the dataframe
                df = clean_and_standardize_dataframe(df, parsing_method)
                
                if df is None or df.empty:
                    logger.warning(f"No valid data found in sheet {sheet_name} after cleaning")
                    continue

                logger.info(f"Final columns after cleaning: {list(df.columns)}")

                # Convert to list of dictionaries and validate
                sheet_records = []
                for idx, row in df.iterrows():
                    try:
                        record = row.to_dict()
                        validated_record = validate_and_clean_record(record)
                        if validated_record and is_record_complete(validated_record):
                            sheet_records.append(validated_record)
                        else:
                            logger.debug(f"Skipping incomplete record at row {idx}: {record}")
                    except Exception as e:
                        logger.error(f"Error processing row {idx}: {e}")
                        continue

                all_records.extend(sheet_records)
                logger.info(f"Extracted {len(sheet_records)} valid records from sheet {sheet_name}")

            except Exception as e:
                logger.error(f"Error processing sheet {sheet_name}: {str(e)}")
                continue

        if not all_records:
            raise ValueError("No valid records found in any sheet of the Excel file")

        logger.info(f"Total valid records parsed: {len(all_records)}")
        return all_records

    except Exception as e:
        logger.error(f"Error parsing Excel file: {str(e)}")
        raise ValueError(f"Error parsing Excel file: {str(e)}")


def has_meaningful_headers(columns) -> bool:
    """Check if column names look like meaningful headers"""
    meaningful_count = 0
    for col in columns:
        if isinstance(col, str) and len(col.strip()) > 2 and 'unnamed' not in col.lower():
            meaningful_count += 1
    return meaningful_count >= 3  # At least 3 meaningful headers


def detect_and_parse_data(df_raw: pd.DataFrame) -> tuple[Optional[pd.DataFrame], str]:
    """Detect where the actual data starts and parse accordingly"""
    
    # Method A: Look for a row that could be headers
    for row_idx in range(min(5, len(df_raw))):
        row_values = df_raw.iloc[row_idx].values
        text_count = sum(1 for val in row_values if isinstance(val, str) and len(str(val).strip()) > 2)
        
        if text_count >= 4:  # At least 4 text values that could be headers
            # Try using this row as headers
            df_with_headers = df_raw.iloc[row_idx+1:].copy()
            df_with_headers.columns = df_raw.iloc[row_idx].values
            df_with_headers = df_with_headers.reset_index(drop=True)
            
            if not df_with_headers.empty:
                logger.info(f"Found potential headers at row {row_idx}: {list(df_raw.iloc[row_idx].values)}")
                return df_with_headers, f"detected_headers_row_{row_idx}"
    
    # Method B: Assume standard column order and assign headers
    if len(df_raw.columns) >= 8:  # Minimum expected columns
        EXPECTED_COLUMNS = [
            'record_date', 'ip_number', 'mother_name', 'admission_date',
            'discharge_date', 'date_of_birth', 'gender', 'mode_of_delivery',
            'child_name', 'father_name', 'birth_notification_no'
        ]
        
        # Find first row with substantial data
        data_start_row = 0
        for row_idx in range(min(5, len(df_raw))):
            non_null_count = df_raw.iloc[row_idx].count()
            if non_null_count >= 5:  # At least 5 non-null values
                data_start_row = row_idx
                break
        
        df_with_assumed_headers = df_raw.iloc[data_start_row:].copy()
        # Assign expected headers up to the number of columns we have
        new_columns = EXPECTED_COLUMNS[:len(df_with_assumed_headers.columns)]
        df_with_assumed_headers.columns = new_columns
        df_with_assumed_headers = df_with_assumed_headers.reset_index(drop=True)
        
        logger.info(f"Assigned standard headers starting from row {data_start_row}")
        return df_with_assumed_headers, f"assumed_headers_from_row_{data_start_row}"
    
    return None, "failed"


def clean_and_standardize_dataframe(df: pd.DataFrame, parsing_method: str) -> Optional[pd.DataFrame]:
    """Clean and standardize the dataframe columns and data"""
    
    # Clean column names
    df.columns = df.columns.astype(str)
    df.columns = [col.strip().lower().replace(' ', '_').replace("'", "").replace('"', '') 
                  for col in df.columns]
    
    # Column mapping for flexible matching
    COLUMN_MAPPING = {
        # Date variations
        'date': 'record_date',
        'record_date': 'record_date',
        'date_recorded': 'record_date',
        
        # IP Number variations
        'ip_no': 'ip_number',
        'ip._no': 'ip_number',
        'ip_number': 'ip_number',
        'ip_no.': 'ip_number',
        'ipno': 'ip_number',
        
        # Mother name variations
        'mother_name': 'mother_name',
        'mothers_name': 'mother_name',
        'mother': 'mother_name',
        
        # Date fields
        'admission_date': 'admission_date',
        'date_of_admission': 'admission_date',
        'admitted_date': 'admission_date',
        'discharge_date': 'discharge_date',
        'date_of_discharge': 'discharge_date',
        'discharged_date': 'discharge_date',
        
        # Birth date
        'date_of_birth': 'date_of_birth',
        'birth_date': 'date_of_birth',
        'dob': 'date_of_birth',
        
        # Gender
        'gender': 'gender',
        'sex': 'gender',
        
        # Mode of delivery
        'mode_of_delivery': 'mode_of_delivery',
        'delivery_mode': 'mode_of_delivery',
        'delivery_method': 'mode_of_delivery',
        
        # Child name
        'child_name': 'child_name',
        'childs_name': 'child_name',
        'baby_name': 'child_name',
        'child': 'child_name',
        
        # Father name
        'father_name': 'father_name',
        'fathers_name': 'father_name',
        'father': 'father_name',
        
        # Birth notification
        'birth_notification_no': 'birth_notification_no',
        'notification_no': 'birth_notification_no',
        'birth_notification': 'birth_notification_no',
        'notification_number': 'birth_notification_no',
        'birth_notification_number': 'birth_notification_no'
    }
    
    # Apply column mapping
    df = df.rename(columns=COLUMN_MAPPING)
    
    # If we still don't have the required columns and used assumed headers, 
    # try to map by position for common Excel layouts
    required_fields = ['record_date', 'ip_number', 'date_of_birth', 'child_name', 'birth_notification_no']
    missing_required = [field for field in required_fields if field not in df.columns]
    
    if missing_required and 'assumed_headers' in parsing_method:
        logger.info(f"Missing required fields after mapping: {missing_required}")
        logger.info("Attempting position-based mapping for common Excel layouts...")
        
        # Try common layouts based on the data you showed in logs
        if len(df.columns) >= 11:
            # Based on your log: [nan, nan, 'patricia_kalekye_mwaniki', nan, nan, nan, 'female', 'caeserian_section', 'talia_mwikali', 'unnamed:_9', nan]
            # This looks like: [?, ?, mother_name, ?, ?, date_of_birth?, gender, mode_of_delivery, child_name, father_name?, ?]
            position_mapping = {}
            
            # Find columns with meaningful data to help identify structure
            for idx, col in enumerate(df.columns):
                sample_values = df.iloc[:3, idx].dropna().astype(str).str.lower()
                
                # Look for gender indicators
                if any(val in ['male', 'female', 'm', 'f'] for val in sample_values):
                    position_mapping[col] = 'gender'
                    
                # Look for delivery mode indicators
                elif any(word in val for val in sample_values for word in ['caeser', 'normal', 'section', 'delivery']):
                    position_mapping[col] = 'mode_of_delivery'
                    
            # Apply position mapping
            df = df.rename(columns=position_mapping)
            
            # For remaining unmapped required fields, try to infer from position and content
            if 'child_name' not in df.columns:
                # Look for columns with name-like content
                for col in df.columns:
                    if col not in ['gender', 'mode_of_delivery']:
                        sample_vals = df[col].dropna().head(3).astype(str)
                        # Simple heuristic: names are usually 2+ words or have specific patterns
                        if len(sample_vals) > 0:
                            avg_words = sample_vals.str.split().str.len().mean()
                            if avg_words >= 1.5:  # Likely names
                                df = df.rename(columns={col: 'child_name'})
                                break
    
    # Remove rows that are completely empty or have only NaN values
    df = df.dropna(how='all')
    
    # Log final column mapping
    logger.info(f"Columns after standardization: {list(df.columns)}")
    
    return df if not df.empty else None


def validate_and_clean_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Validate and clean individual record data"""
    try:
        # Create clean record
        clean_record = {}
        
        for key, value in record.items():
            # Handle various null representations
            if pd.isna(value) or value == 'nan' or value == '' or str(value).strip() == '':
                clean_record[key] = None
            else:
                clean_record[key] = value
        
        # Process date fields
        date_fields = ['record_date', 'admission_date', 'discharge_date', 'date_of_birth']
        for field in date_fields:
            if field in clean_record and clean_record[field] is not None:
                try:
                    if isinstance(clean_record[field], str):
                        clean_record[field] = pd.to_datetime(clean_record[field], dayfirst=True).date()
                    elif hasattr(clean_record[field], 'date'):
                        clean_record[field] = clean_record[field].date()
                except:
                    clean_record[field] = None
        
        # Process text fields
        text_fields = ['gender', 'mode_of_delivery', 'child_name', 'father_name', 'mother_name']
        for field in text_fields:
            if field in clean_record and clean_record[field] is not None:
                clean_record[field] = str(clean_record[field]).strip().title()
        
        # Process ID fields
        if 'ip_number' in clean_record and clean_record['ip_number'] is not None:
            clean_record['ip_number'] = str(clean_record['ip_number']).strip()
            
        if 'birth_notification_no' in clean_record and clean_record['birth_notification_no'] is not None:
            clean_record['birth_notification_no'] = str(clean_record['birth_notification_no']).strip()
        
        # Validate gender
        if clean_record.get('gender'):
            gender = clean_record['gender'].upper()
            if gender in ['M', 'MALE']:
                clean_record['gender'] = 'Male'
            elif gender in ['F', 'FEMALE']:
                clean_record['gender'] = 'Female'
        
        return clean_record
        
    except Exception as e:
        logger.error(f"Error validating record: {e}")
        return None


def is_record_complete(record: Dict[str, Any]) -> bool:
    """Check if record has minimum required fields"""
    required_fields = ['child_name', 'birth_notification_no']
    
    for field in required_fields:
        value = record.get(field)
        if not value or (isinstance(value, str) and value.strip() == ''):
            return False
    
    return True