from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError
from uuid import UUID
from app.models.birth_record import BirthRecord
from app.schemas.birth_record import BirthRecordCreate, BirthRecordUpdate
from datetime import date
import logging

# Set up logging
logger = logging.getLogger(__name__)

class CRUDBirthRecord:
    def get(self, db: Session, id: UUID) -> Optional[BirthRecord]:
        """Retrieve a birth record by ID."""
        return db.query(BirthRecord).filter(BirthRecord.id == id).first()

    def get_multi(
        self, db: Session, *, skip: int = 0, limit: int = 100
    ) -> List[BirthRecord]:
        """Retrieve multiple birth records with pagination."""
        return db.query(BirthRecord).offset(skip).limit(limit).all()

    def get_by_notification_no(
        self, db: Session, *, notification_no: str
    ) -> Optional[BirthRecord]:
        """Retrieve a birth record by birth notification number."""
        return db.query(BirthRecord).filter(
            BirthRecord.birth_notification_no == notification_no
        ).first()

    def get_by_notification_nos(
        self, db: Session, *, notification_nos: List[str]
    ) -> Dict[str, Optional[BirthRecord]]:
        """
        Retrieve multiple birth records by their notification numbers in a single query.
        Returns a dictionary mapping notification numbers to records (or None if not found).
        """
        records = db.query(BirthRecord).filter(
            BirthRecord.birth_notification_no.in_(notification_nos)
        ).all()
        return {record.birth_notification_no: record for record in records}

    def get_by_date_range(
        self, db: Session, *, start_date: date, end_date: date, skip: int = 0, limit: int = 100
    ) -> List[BirthRecord]:
        """Retrieve birth records within a date range with pagination."""
        return db.query(BirthRecord).filter(
            and_(
                BirthRecord.record_date >= start_date,
                BirthRecord.record_date <= end_date
            )
        ).offset(skip).limit(limit).all()

    def search(
        self, db: Session, *, query: str, skip: int = 0, limit: int = 100
    ) -> List[BirthRecord]:
        """Search birth records by child name, mother name, father name, notification number, or IP number."""
        query_conditions = [
            BirthRecord.child_name.ilike(f"%{query}%"),
            BirthRecord.mother_name.ilike(f"%{query}%"),
            BirthRecord.birth_notification_no.ilike(f"%{query}%"),
            BirthRecord.ip_number.ilike(f"%{query}%"),
        ]
        # Handle nullable father_name
        if query:
            query_conditions.append(
                BirthRecord.father_name.ilike(f"%{query}%")
            )
        return db.query(BirthRecord).filter(
            or_(*query_conditions)
        ).offset(skip).limit(limit).all()

    def create(
        self, db: Session, *, obj_in: BirthRecordCreate, created_by: int
    ) -> BirthRecord:
        """Create a new birth record with error handling."""
        try:
            db_obj = BirthRecord(
                **obj_in.dict(),
                created_by=created_by
            )
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Created birth record with ID: {db_obj.id}, birth_notification_no: {db_obj.birth_notification_no}")
            return db_obj
        except IntegrityError as ie:
            db.rollback()
            logger.error(f"Integrity error creating birth record: {str(ie)}")
            raise ValueError(f"Database integrity error: {str(ie)}")
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error creating birth record: {str(e)}")
            raise ValueError(f"Error creating birth record: {str(e)}")

    def update(
        self, db: Session, *, db_obj: BirthRecord, obj_in: BirthRecordUpdate
    ) -> BirthRecord:
        """Update an existing birth record."""
        try:
            update_data = obj_in.dict(exclude_unset=True)
            for field, value in update_data.items():
                setattr(db_obj, field, value)
            
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Updated birth record with ID: {db_obj.id}")
            return db_obj
        except IntegrityError as ie:
            db.rollback()
            logger.error(f"Integrity error updating birth record: {str(ie)}")
            raise ValueError(f"Database integrity error: {str(ie)}")
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error updating birth record: {str(e)}")
            raise ValueError(f"Error updating birth record: {str(e)}")

    def remove(self, db: Session, *, id: UUID) -> BirthRecord:
        """Delete a birth record by ID."""
        obj = db.query(BirthRecord).get(id)
        if obj is None:
            logger.warning(f"Attempted to delete non-existent birth record with ID: {id}")
            raise ValueError(f"BirthRecord with ID {id} not found")
        db.delete(obj)
        db.commit()
        logger.info(f"Deleted birth record with ID: {id}")
        return obj

birth_record = CRUDBirthRecord()