import pandas as pd
from typing import List, Dict, Any
from datetime import datetime
import io

def parse_excel_file(file_content: bytes) -> List[Dict[str, Any]]:
    """
    Parse Excel file content and extract birth records
    Expected format matches your existing Excel structure
    """
    try:
        # Read Excel file
        df = pd.read_excel(io.BytesIO(file_content))
        
        # Clean column names - remove extra spaces and standardize
        df.columns = df.columns.str.strip()
        
        # Expected column mapping (adjust based on your Excel format)
        column_mapping = {
            'Date': 'record_date',
            'IP. NO': 'ip_number',
            'Date of Admission': 'admission_date',
            'Date of Discharge': 'discharge_date',
            'Date of Birth': 'date_of_birth',
            'Gender': 'gender',
            'Mode of Delivery': 'mode_of_delivery',
            "Child's Name": 'child_name',
            "Father's Name": 'father_name',
            'Birth Notification No': 'birth_notification_no'
        }
        
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        # Remove rows where essential fields are null
        essential_fields = ['record_date', 'ip_number', 'date_of_birth', 'child_name', 'birth_notification_no']
        df = df.dropna(subset=essential_fields)
        
        # Convert dates
        date_fields = ['record_date', 'admission_date', 'discharge_date', 'date_of_birth']
        for field in date_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce').dt.date
        
        # Clean and standardize text fields
        text_fields = ['gender', 'mode_of_delivery', 'child_name', 'father_name']
        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].astype(str).str.strip().str.title()
        
        # Convert IP number to string
        df['ip_number'] = df['ip_number'].astype(str)
        df['birth_notification_no'] = df['birth_notification_no'].astype(str)
        
        # Convert to list of dictionaries
        records = df.to_dict('records')
        
        # Filter out records with invalid data
        valid_records = []
        for record in records:
            # Skip if essential fields are missing or invalid
            if (record.get('record_date') and 
                record.get('ip_number') and 
                record.get('date_of_birth') and 
                record.get('child_name') and 
                record.get('birth_notification_no')):
                
                # Handle None values for optional fields
                if pd.isna(record.get('discharge_date')):
                    record['discharge_date'] = None
                
                valid_records.append(record)
        
        return valid_records
        
    except Exception as e:
        raise ValueError(f"Error parsing Excel file: {str(e)}")

def validate_birth_record_data(record: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and clean individual birth record data"""
    # Add any additional validation logic here
    # For example, check date ranges, gender values, etc.
    
    # Validate gender
    valid_genders = ['Male', 'Female', 'Other']
    if record.get('gender') not in valid_genders:
        record['gender'] = 'Other'
    
    # Validate mode of delivery
    valid_modes = ['Normal', 'C-Section', 'Vacuum', 'Forceps', 'Breech']
    if record.get('mode_of_delivery') not in valid_modes:
        record['mode_of_delivery'] = 'Normal'
    
    return record