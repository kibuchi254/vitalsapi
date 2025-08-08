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
    Handles inconsistencies and standardizes data.
    """
    try:
        # Read all sheets from Excel file
        xls = pd.ExcelFile(io.BytesIO(file_content))
        all_records = []

        # Process each sheet (assuming each sheet represents a year)
        for sheet_name in xls.sheet_names:
            logger.info(f"Processing sheet: {sheet_name}")
            try:
                df = pd.read_excel(xls, sheet_name=sheet_name)

                # Skip empty sheets
                if df.empty:
                    logger.warning(f"Sheet {sheet_name} is empty, skipping.")
                    continue

                # Clean column names - remove extra spaces, convert to lowercase, and standardize
                df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

                # Flexible column mapping to handle variations in column names
                column_mapping = {
                    'date': 'record_date',
                    'ip._no': 'ip_number',
                    'ip_no': 'ip_number',
                    'ip_number': 'ip_number',
                    'date_of_admission': 'admission_date',
                    'admission_date': 'admission_date',
                    'date_of_discharge': 'discharge_date',
                    'discharge_date': 'discharge_date',
                    'date_of_birth': 'date_of_birth',
                    'birth_date': 'date_of_birth',
                    'gender': 'gender',
                    'sex': 'gender',
                    'mode_of_delivery': 'mode_of_delivery',
                    'delivery_mode': 'mode_of_delivery',
                    "child's_name": 'child_name',
                    'child_name': 'child_name',
                    "father's_name": 'father_name',
                    'father_name': 'father_name',
                    'birth_notification_no': 'birth_notification_no',
                    'notification_no': 'birth_notification_no',
                    'birth_notification': 'birth_notification_no'
                }

                # Rename columns using the mapping
                df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

                # Define essential fields
                essential_fields = ['record_date', 'ip_number', 'date_of_birth', 'child_name', 'birth_notification_no']

                # Check if essential fields exist
                missing_essential = [field for field in essential_fields if field not in df.columns]
                if missing_essential:
                    logger.error(f"Sheet {sheet_name} missing essential columns: {missing_essential}")
                    continue

                # Remove rows where essential fields are null
                df = df.dropna(subset=essential_fields)

                # Convert dates to datetime.date
                date_fields = ['record_date', 'admission_date', 'discharge_date', 'date_of_birth']
                for field in date_fields:
                    if field in df.columns:
                        try:
                            df[field] = pd.to_datetime(df[field], errors='coerce').dt.date
                        except Exception as e:
                            logger.warning(f"Error converting dates in {field} for sheet {sheet_name}: {str(e)}")

                # Clean and standardize text fields
                text_fields = ['gender', 'mode_of_delivery', 'child_name', 'father_name']
                for field in text_fields:
                    if field in df.columns:
                        df[field] = df[field].astype(str).str.strip().str.title()

                # Convert IP number and birth notification number to string
                df['ip_number'] = df['ip_number'].astype(str)
                df['birth_notification_no'] = df['birth_notification_no'].astype(str)

                # Validate data
                df = df.apply(validate_birth_record_data, axis=1)

                # Convert to list of dictionaries
                if isinstance(df, pd.DataFrame):
                    records = df.to_dict(orient='records')
                else:
                    logger.error(f"Expected a pandas DataFrame, got {type(df)} in sheet {sheet_name}")
                    continue

                # Filter out records with invalid essential data
                valid_records = []
                for record in records:
                    if all(record.get(field) and not pd.isna(record.get(field)) for field in essential_fields):
                        # Handle None values for optional fields
                        for field in ['admission_date', 'discharge_date', 'father_name', 'mode_of_delivery']:
                            if pd.isna(record.get(field)):
                                record[field] = None
                        valid_records.append(record)
                    else:
                        logger.warning(f"Skipping invalid record in sheet {sheet_name}: {record}")

                all_records.extend(valid_records)
                logger.info(f"Processed {len(valid_records)} valid records from sheet {sheet_name}")

            except Exception as e:
                logger.error(f"Error processing sheet {sheet_name}: {str(e)}")
                continue

        if not all_records:
            raise ValueError("No valid records found in any sheet of the Excel file")

        return all_records

    except Exception as e:
        logger.error(f"Error parsing Excel file: {str(e)}")
        raise ValueError(f"Error parsing Excel file: {str(e)}")

def validate_birth_record_data(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and clean individual birth record data.
    """
    # Validate gender
    valid_genders = ['Male', 'Female', 'Other']
    if record.get('gender') not in valid_genders:
        logger.warning(f"Invalid gender '{record.get('gender')}' in record {record.get('ip_number')}, setting to 'Other'")
        record['gender'] = 'Other'

    # Validate mode of delivery
    valid_modes = ['Normal', 'C-Section', 'Caeserian Section', 'Spontaneous Vertex Delivery', 'Vacuum', 'Forceps', 'Breech', 'Born Before Arrival']
    if record.get('mode_of_delivery') and record.get('mode_of_delivery') not in valid_modes:
        logger.warning(f"Invalid mode of delivery '{record.get('mode_of_delivery')}' in record {record.get('ip_number')}, setting to 'Normal'")
        record['mode_of_delivery'] = 'Normal'

    # Validate date consistency (e.g., date_of_birth should not be after record_date)
    if record.get('record_date') and record.get('date_of_birth'):
        if record['record_date'] < record['date_of_birth']:
            logger.warning(f"Invalid date: date_of_birth {record['date_of_birth']} is after record_date {record['record_date']} in record {record.get('ip_number')}")
            record['date_of_birth'] = None

    return record