import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
import io
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def parse_excel_file(file_content: bytes) -> List[Dict[str, Any]]:
    try:
        xls = pd.ExcelFile(io.BytesIO(file_content))
        all_records = []

        EXPECTED_COLUMNS = [
            'record_date', 'ip_number', 'mother_name', 'admission_date',
            'discharge_date', 'date_of_birth', 'gender', 'mode_of_delivery',
            'child_name', 'father_name', 'birth_notification_no'
        ]

        for sheet_name in xls.sheet_names:
            logger.info(f"Processing sheet: {sheet_name}")
            try:
                df = None
                parsing_method = "unknown"
                
                try:
                    df_test = pd.read_excel(xls, sheet_name=sheet_name, header=0)
                    if not df_test.empty and has_meaningful_headers(df_test.columns):
                        df = df_test
                        parsing_method = "header_row_0"
                        logger.info(f"Successfully parsed with header at row 0")
                except Exception as e:
                    logger.debug(f"Method 1 failed: {e}")

                if df is None:
                    try:
                        df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                        df, parsing_method = detect_and_parse_data(df_raw)
                        if df is not None:
                            logger.info(f"Successfully parsed using method: {parsing_method}")
                    except Exception as e:
                        logger.debug(f"Method 2 failed: {e}")

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

                df = clean_and_standardize_dataframe(df, parsing_method)
                
                if df is None or df.empty:
                    logger.warning(f"No valid data found in sheet {sheet_name} after cleaning")
                    continue

                logger.info(f"Final columns after cleaning: {list(df.columns)}")

                sheet_records = []
                for idx, row in df.iterrows():
                    try:
                        record = row.to_dict()
                        logger.debug(f"Raw record at row {idx + 2}: {record}")
                        validated_record = validate_and_clean_record(record)
                        if validated_record:
                            logger.debug(f"Validated record at row {idx + 2}: {validated_record}")
                            if is_record_complete(validated_record):
                                sheet_records.append(validated_record)
                            else:
                                logger.error(f"Skipping incomplete record at row {idx + 2}: {validated_record}")
                        else:
                            logger.error(f"Skipping invalid record at row {idx + 2}: {record}")
                    except Exception as e:
                        logger.error(f"Error processing row {idx + 2}: {e}")
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
    meaningful_count = 0
    for col in columns:
        if isinstance(col, str) and len(col.strip()) > 2 and 'unnamed' not in col.lower():
            meaningful_count += 1
    return meaningful_count >= 3

def detect_and_parse_data(df_raw: pd.DataFrame) -> tuple[Optional[pd.DataFrame], str]:
    first_row = df_raw.iloc[0] if len(df_raw) > 0 else None
    if first_row is not None:
        date_count = sum(1 for val in first_row if isinstance(val, (pd.Timestamp, datetime)))
        number_count = sum(1 for val in first_row if isinstance(val, (int, float)) and not pd.isna(val))
        
        if date_count >= 2 and number_count >= 1:
            logger.info("First row appears to contain actual data, not headers")
            
            EXPECTED_COLUMNS = [
                'record_date', 'ip_number', 'mother_name', 'admission_date',
                'discharge_date', 'date_of_birth', 'gender', 'mode_of_delivery',
                'child_name', 'father_name', 'birth_notification_no'
            ]
            
            df_with_headers = df_raw.copy()
            new_columns = EXPECTED_COLUMNS[:len(df_with_headers.columns)]
            for i in range(len(new_columns), len(df_with_headers.columns)):
                new_columns.append(f'extra_column_{i}')
                
            df_with_headers.columns = new_columns
            df_with_headers = df_with_headers.reset_index(drop=True)
            
            logger.info(f"Treating all rows as data with assigned headers: {new_columns}")
            return df_with_headers, "data_as_first_row"
    
    for row_idx in range(min(5, len(df_raw))):
        row_values = df_raw.iloc[row_idx].values
        text_count = sum(1 for val in row_values if isinstance(val, str) and len(str(val).strip()) > 2)
        date_count = sum(1 for val in row_values if isinstance(val, (pd.Timestamp, datetime)))
        number_count = sum(1 for val in row_values if isinstance(val, (int, float)) and not pd.isna(val))
        
        if text_count >= 4 and date_count <= 1 and number_count <= 2:
            df_with_headers = df_raw.iloc[row_idx+1:].copy()
            df_with_headers.columns = df_raw.iloc[row_idx].values
            df_with_headers = df_with_headers.reset_index(drop=True)
            
            if not df_with_headers.empty:
                logger.info(f"Found potential headers at row {row_idx}: {list(df_raw.iloc[row_idx].values)}")
                return df_with_headers, f"detected_headers_row_{row_idx}"
    
    if len(df_raw.columns) >= 8:
        EXPECTED_COLUMNS = [
            'record_date', 'ip_number', 'mother_name', 'admission_date',
            'discharge_date', 'date_of_birth', 'gender', 'mode_of_delivery',
            'child_name', 'father_name', 'birth_notification_no'
        ]
        
        data_start_row = 0
        for row_idx in range(min(5, len(df_raw))):
            non_null_count = df_raw.iloc[row_idx].count()
            if non_null_count >= 5:
                data_start_row = row_idx
                break
        
        df_with_assumed_headers = df_raw.iloc[data_start_row:].copy()
        new_columns = EXPECTED_COLUMNS[:len(df_with_assumed_headers.columns)]
        df_with_assumed_headers.columns = new_columns
        df_with_assumed_headers = df_with_assumed_headers.reset_index(drop=True)
        
        logger.info(f"Assigned standard headers starting from row {data_start_row}")
        return df_with_assumed_headers, f"assumed_headers_from_row_{data_start_row}"
    
    return None, "failed"

def clean_and_standardize_dataframe(df: pd.DataFrame, parsing_method: str) -> Optional[pd.DataFrame]:
    EXPECTED_COLUMNS = [
        'record_date', 'ip_number', 'mother_name', 'admission_date',
        'discharge_date', 'date_of_birth', 'gender', 'mode_of_delivery',
        'child_name', 'father_name', 'birth_notification_no'
    ]
    
    df.columns = df.columns.astype(str)
    df.columns = [col.strip().lower().replace(' ', '_').replace("'", "").replace('"', '') 
                  for col in df.columns]
    
    COLUMN_MAPPING = {
        'date': 'record_date',
        'record_date': 'record_date',
        'date_recorded': 'record_date',
        'record_date.1': 'ip_number',
        'ip_no': 'ip_number',
        'ip_number': 'ip_number',
        'mother_name': 'mother_name',
        'mothers_name': 'mother_name',
        'admission_date': 'admission_date',
        'date_of_admission': 'admission_date',
        'discharge_date': 'discharge_date',
        'date_of_discharge': 'discharge_date',
        'date_of_birth': 'date_of_birth',
        'birth_date': 'date_of_birth',
        'dob': 'date_of_birth',
        'gender': 'gender',
        'sex': 'gender',
        'mode_of_delivery': 'mode_of_delivery',
        'delivery_mode': 'mode_of_delivery',
        'child_name': 'child_name',
        'childs_name': 'child_name',
        'father_name': 'father_name',
        'fathers_name': 'father_name',
        'birth_notification_no': 'birth_notification_no',
        'notification_no': 'birth_notification_no'
    }
    
    df = df.rename(columns=COLUMN_MAPPING)
    
    required_fields = ['ip_number', 'mother_name', 'birth_notification_no']
    missing_required = [field for field in required_fields if field not in df.columns]
    non_meaningful_headers = any(isinstance(col, (pd.Timestamp, datetime)) or str(col).startswith('Unnamed') 
                                or str(col).isdigit() for col in df.columns)
    
    if missing_required or non_meaningful_headers:
        logger.info(f"Missing required fields: {missing_required} or non-meaningful headers detected")
        logger.info("Attempting position-based mapping based on expected column order...")
        
        if len(df.columns) >= 11:
            position_mapping = {
                df.columns[0]: 'record_date',
                df.columns[1]: 'ip_number',
                df.columns[2]: 'mother_name',
                df.columns[3]: 'admission_date',
                df.columns[4]: 'discharge_date',
                df.columns[5]: 'date_of_birth',
                df.columns[6]: 'gender',
                df.columns[7]: 'mode_of_delivery',
                df.columns[8]: 'child_name',
                df.columns[9]: 'father_name',
                df.columns[10]: 'birth_notification_no'
            }
            
            df = df.rename(columns=position_mapping)
            logger.info(f"Applied position-based mapping: {list(df.columns)}")
            
            missing_required = [field for field in required_fields if field not in df.columns]
            if missing_required:
                logger.warning(f"Still missing required fields after positional mapping: {missing_required}")
                return None
    
    for idx, col in enumerate(df.columns):
        if col not in EXPECTED_COLUMNS:
            sample_values = df.iloc[:3, idx].dropna().astype(str).str.lower()
            if any(val in ['male', 'female', 'm', 'f', 'other'] for val in sample_values):
                df = df.rename(columns={col: 'gender'})
            elif any(word in val for val in sample_values for word in ['caeser', 'normal', 'section', 'delivery', 'vacuum', 'forceps', 'breech']):
                df = df.rename(columns={col: 'mode_of_delivery'})
            elif any(len(str(val).split()) >= 2 for val in sample_values):
                if col != 'mother_name' and col != 'child_name' and col != 'father_name':
                    if 'mother_name' not in df.columns:
                        df = df.rename(columns={col: 'mother_name'})
                    elif 'child_name' not in df.columns:
                        df = df.rename(columns={col: 'child_name'})
                    elif 'father_name' not in df.columns:
                        df = df.rename(columns={col: 'father_name'})
            elif any(len(str(val)) >= 4 and str(val).isdigit() for val in sample_values):
                if 'birth_notification_no' not in df.columns:
                    df = df.rename(columns={col: 'birth_notification_no'})
    
    df = df.dropna(how='all')
    logger.info(f"Columns after standardization: {list(df.columns)}")
    return df if not df.empty else None

def validate_and_clean_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        clean_record = {}
        for key, value in record.items():
            if pd.isna(value) or value == 'nan' or value == '' or str(value).strip() == '':
                clean_record[key] = None
            else:
                clean_record[key] = value
        
        date_fields = ['record_date', 'admission_date', 'discharge_date', 'date_of_birth']
        for field in date_fields:
            if field in clean_record and clean_record[field] is not None:
                try:
                    if isinstance(clean_record[field], str):
                        try:
                            # Handle DD-MMM format (e.g., '12-Jul') with default year 2025
                            clean_record[field] = pd.to_datetime(clean_record[field], format='%d-%b', errors='raise', yearfirst=False).replace(year=2025).date()
                        except:
                            # Fallback to general parsing with default year
                            parsed_date = pd.to_datetime(clean_record[field], dayfirst=True, errors='coerce')
                            if pd.isna(parsed_date):
                                logger.error(f"Invalid date for field {field} in record: {clean_record[field]}")
                                clean_record[field] = None
                            else:
                                clean_record[field] = parsed_date.date()
                    elif isinstance(clean_record[field], (pd.Timestamp, datetime)):
                        clean_record[field] = clean_record[field].date()
                except Exception as e:
                    logger.error(f"Failed to parse date for field {field}: {clean_record[field]}, error: {e}")
                    clean_record[field] = None
        
        text_fields = ['gender', 'mode_of_delivery', 'child_name', 'father_name', 'mother_name', 'ip_number', 'birth_notification_no']
        for field in text_fields:
            if field in clean_record and clean_record[field] is not None:
                clean_record[field] = str(clean_record[field]).strip().title()
                if field == 'father_name' and (clean_record[field].lower().startswith('unnamed') or len(clean_record[field]) < 2):
                    clean_record[field] = None
                    logger.debug(f"Set father_name to None for invalid value: {clean_record[field]}")
                if field in ['mother_name', 'ip_number', 'birth_notification_no'] and len(clean_record[field]) < 2:
                    logger.error(f"Invalid {field} value: {clean_record[field]}")
                    return None
                # For mode_of_delivery, ensure it's a non-empty string
                if field == 'mode_of_delivery' and len(clean_record[field]) < 1:
                    logger.error(f"Invalid mode_of_delivery value: {clean_record[field]}")
                    clean_record[field] = None
        
        return clean_record
        
    except Exception as e:
        logger.error(f"Error validating record: {e}")
        return None

def is_record_complete(record: Dict[str, Any]) -> bool:
    required_fields = ['ip_number', 'mother_name', 'birth_notification_no']
    
    for field in required_fields:
        value = record.get(field)
        if not value or (isinstance(value, str) and value.strip() == ''):
            logger.error(f"Record missing or invalid required field '{field}': {value}")
            return False
    
    birth_no = record.get('birth_notification_no')
    if birth_no:
        try:
            birth_no_str = str(birth_no).strip()
            if len(birth_no_str) < 4:
                logger.error(f"Birth notification number too short: {birth_no_str}")
                return False
        except:
            logger.error(f"Invalid birth notification number: {birth_no}")
            return False
    
    mother_name = record.get('mother_name')
    if mother_name:
        try:
            mother_name_str = str(mother_name).strip()
            if len(mother_name_str) < 2:
                logger.error(f"Mother name too short: {mother_name_str}")
                return False
        except:
            logger.error(f"Invalid mother name: {mother_name}")
            return False
    
    ip_number = record.get('ip_number')
    if ip_number:
        try:
            ip_number_str = str(ip_number).strip()
            if len(ip_number_str) < 2:
                logger.error(f"IP number too short: {ip_number_str}")
                return False
        except:
            logger.error(f"Invalid IP number: {ip_number}")
            return False
    
    logger.debug(f"Record is complete: ip_number={record.get('ip_number')}, mother_name={record.get('mother_name')}, birth_notification_no={record.get('birth_notification_no')}")
    return True