from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from uuid import UUID
from app.models.birth_record import BirthRecord
from app.schemas.birth_record import BirthRecordCreate, BirthRecordUpdate
from datetime import date

class CRUDBirthRecord:
    def get(self, db: Session, id: UUID) -> Optional[BirthRecord]:
        return db.query(BirthRecord).filter(BirthRecord.id == id).first()

    def get_multi(
        self, db: Session, *, skip: int = 0, limit: int = 100
    ) -> List[BirthRecord]:
        return db.query(BirthRecord).offset(skip).limit(limit).all()

    def get_by_notification_no(
        self, db: Session, *, notification_no: str
    ) -> Optional[BirthRecord]:
        return db.query(BirthRecord).filter(
            BirthRecord.birth_notification_no == notification_no
        ).first()

    def get_by_date_range(
        self, db: Session, *, start_date: date, end_date: date, skip: int = 0, limit: int = 100
    ) -> List[BirthRecord]:
        return db.query(BirthRecord).filter(
            and_(
                BirthRecord.record_date >= start_date,
                BirthRecord.record_date <= end_date
            )
        ).offset(skip).limit(limit).all()

    def search(
        self, db: Session, *, query: str, skip: int = 0, limit: int = 100
    ) -> List[BirthRecord]:
        return db.query(BirthRecord).filter(
            or_(
                BirthRecord.child_name.ilike(f"%{query}%"),
                BirthRecord.father_name.ilike(f"%{query}%"),
                BirthRecord.birth_notification_no.ilike(f"%{query}%"),
                BirthRecord.ip_number.ilike(f"%{query}%")
            )
        ).offset(skip).limit(limit).all()

    def create(
        self, db: Session, *, obj_in: BirthRecordCreate, created_by: int
    ) -> BirthRecord:
        db_obj = BirthRecord(
            **obj_in.dict(),
            created_by=created_by
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def update(
        self, db: Session, *, db_obj: BirthRecord, obj_in: BirthRecordUpdate
    ) -> BirthRecord:
        update_data = obj_in.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_obj, field, value)
        
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def remove(self, db: Session, *, id: UUID) -> BirthRecord:
        obj = db.query(BirthRecord).get(id)
        if obj is None:
            raise ValueError(f"BirthRecord with ID {id} not found")
        db.delete(obj)
        db.commit()
        return obj

birth_record = CRUDBirthRecord()