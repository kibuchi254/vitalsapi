import pandas as pd
from typing import List, Dict, Any
from datetime import datetime
import io
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_excel_file(file_content: bytes) -> List[Dict[str, Any]]:
    """
    Parse Excel file content with multiple sheets and extract birth records.
    Handles inconsistencies, missing headers, and standardizes data.
    """
    try:
        # Read all sheets from Excel file
        xls = pd.ExcelFile(io.BytesIO(file_content))
        all_records = []

        # Expected headers mapping for flexibility
        column_mapping = {
            # Date fields
            'date': 'record_date',
            'record_date': 'record_date',
            'date_recorded': 'record_date',
            
            # IP Number variations
            'ip._no': 'ip_number',
            'ip_no': 'ip_number',
            'ip_number': 'ip_number',
            'ip no.': 'ip_number',
            'ip no': 'ip_number',
            'ip_no.': 'ip_number',
            
            # Mother name
            'mother_name': 'mother_name',
            "mother's_name": 'mother_name',
            'mother name': 'mother_name',
            
            # Date fields
            'date_of_admission': 'admission_date',
            'admission_date': 'admission_date',
            'admitted_date': 'admission_date',
            'date_of_discharge': 'discharge_date',
            'discharge_date': 'discharge_date',
            'discharged_date': 'discharge_date',
            
            # Birth date
            'date_of_birth': 'date_of_birth',
            'birth_date': 'date_of_birth',
            'dob': 'date_of_birth',
            
            # Gender
            'gender': 'gender',
            'sex': 'gender',
            
            # Delivery mode
            'mode_of_delivery': 'mode_of_delivery',
            'delivery_mode': 'mode_of_delivery',
            'delivery_method': 'mode_of_delivery',
            
            # Child name
            "child's_name": 'child_name',
            'child_name': 'child_name',
            'child name': 'child_name',
            'baby_name': 'child_name',
            
            # Father name
            "father's_name": 'father_name',
            'father_name': 'father_name',
            'father name': 'father_name',
            
            # Birth notification
            'birth_notification_no': 'birth_notification_no',
            'notification_no': 'birth_notification_no',
            'birth_notification': 'birth_notification_no',
            'notification_number': 'birth_notification_no',
            'birth_notification_number': 'birth_notification_no'
        }

        for sheet_name in xls.sheet_names:
            logger.info(f"Processing sheet: {sheet_name}")
            try:
                # Try reading with first row as header first
                df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
                
                # Skip empty sheets
                if df.empty:
                    logger.warning(f"Sheet {sheet_name} is empty, skipping.")
                    continue

                # Clean column names (remove spaces, convert to lowercase)
                df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
                
                # Apply column mapping
                df = df.rename(columns={k.lower().replace(' ', '_'): v for k, v in column_mapping.items()})
                
                # Log which columns were found and mapped
                logger.info(f"Columns found in sheet {sheet_name}: {list(df.columns)}")
                
                # Check if we have the essential required fields
                required_fields = ['record_date', 'ip_number', 'date_of_birth', 'child_name', 'birth_notification_no']
                missing_fields = [field for field in required_fields if field not in df.columns]
                
                if missing_fields:
                    logger.error(f"Missing required fields in sheet {sheet_name}: {missing_fields}")
                    continue

                # Drop rows where ALL essential fields are NaN
                df = df.dropna(subset=required_fields, how='all')
                
                # Convert dates with better error handling
                date_fields = ['record_date', 'admission_date', 'discharge_date', 'date_of_birth']
                for field in date_fields:
                    if field in df.columns:
                        df[field] = pd.to_datetime(df[field], errors='coerce', dayfirst=True).dt.date

                # Clean text fields
                text_fields = ['gender', 'mode_of_delivery', 'child_name', 'father_name', 'mother_name']
                for field in text_fields:
                    if field in df.columns:
                        df[field] = df[field].astype(str).str.strip()
                        # Only title case if not 'nan'
                        df.loc[df[field] != 'nan', field] = df.loc[df[field] != 'nan', field].str.title()
                        # Replace 'nan' strings with None
                        df.loc[df[field] == 'nan', field] = None

                # Convert numeric IDs to string, handling NaN
                if 'ip_number' in df.columns:
                    df['ip_number'] = df['ip_number'].fillna('').astype(str).str.strip()
                    df.loc[df['ip_number'] == '', 'ip_number'] = None
                    
                if 'birth_notification_no' in df.columns:
                    df['birth_notification_no'] = df['birth_notification_no'].fillna('').astype(str).str.strip()
                    df.loc[df['birth_notification_no'] == '', 'birth_notification_no'] = None

                # Validate each record
                validated_records = []
                for _, row in df.iterrows():
                    try:
                        record = validate_birth_record_data(row.to_dict())
                        if record and is_valid_record(record):
                            validated_records.append(record)
                        else:
                            logger.warning(f"Skipping invalid record: {record}")
                    except Exception as e:
                        logger.error(f"Error validating record: {e}")
                        continue

                all_records.extend(validated_records)
                logger.info(f"Processed {len(validated_records)} valid records from sheet {sheet_name}")

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


def validate_birth_record_data(record: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and clean individual birth record data."""
    # Handle None and NaN values
    clean_record = {}
    for key, value in record.items():
        if pd.isna(value) or value == 'nan' or value == '':
            clean_record[key] = None
        else:
            clean_record[key] = value
    
    # Validate gender
    valid_genders = ['Male', 'Female', 'M', 'F']
    gender = clean_record.get('gender')
    if gender:
        if gender.upper() in ['M', 'MALE']:
            clean_record['gender'] = 'Male'
        elif gender.upper() in ['F', 'FEMALE']:
            clean_record['gender'] = 'Female'
        elif gender not in valid_genders:
            clean_record['gender'] = 'Other'

    # Validate mode of delivery
    valid_modes = [
        'Normal', 'C-Section', 'Caeserian Section', 'Spontaneous Vertex Delivery',
        'Vacuum', 'Forceps', 'Breech', 'Born Before Arrival', 'Caesarean Section'
    ]
    mode = clean_record.get('mode_of_delivery')
    if mode and mode not in valid_modes:
        # Try to map common variations
        mode_mapping = {
            'cs': 'C-Section',
            'c-sec': 'C-Section',
            'caesarean': 'Caesarean Section',
            'normal delivery': 'Normal',
            'svd': 'Spontaneous Vertex Delivery'
        }
        clean_record['mode_of_delivery'] = mode_mapping.get(mode.lower(), 'Normal')

    # Validate dates
    if clean_record.get('record_date') and clean_record.get('date_of_birth'):
        if clean_record['record_date'] < clean_record['date_of_birth']:
            logger.warning("Record date is before birth date, setting birth date to None")
            clean_record['date_of_birth'] = None

    return clean_record


def is_valid_record(record: Dict[str, Any]) -> bool:
    """Check if a record has all required fields with valid values."""
    required_fields = ['record_date', 'ip_number', 'date_of_birth', 'child_name', 'birth_notification_no']
    
    for field in required_fields:
        value = record.get(field)
        if not value or (isinstance(value, str) and value.strip() == ''):
            logger.warning(f"Record missing required field '{field}': {record}")
            return False
    
    return True