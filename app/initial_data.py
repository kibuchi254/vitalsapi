import os
from sqlalchemy.orm import Session
from app.core.database import SessionLocal, engine
from app.models.user import User
from app.models.birth_record import BirthRecord
from app.core.security import get_password_hash
from app.core.config import settings

def init_db() -> None:
    """Initialize database with tables and initial data"""
    
    # Create all tables
    User.metadata.create_all(bind=engine)
    BirthRecord.metadata.create_all(bind=engine)
    
    db: Session = SessionLocal()
    
    try:
        # Check if any superuser exists
        existing_superuser = db.query(User).filter(User.is_superuser == True).first()
        
        if not existing_superuser:
            # Create initial superuser
            admin_user = User(
                email=settings.FIRST_SUPERUSER_EMAIL,
                hashed_password=get_password_hash(settings.FIRST_SUPERUSER_PASSWORD),
                full_name="System Administrator",
                is_active=True,
                is_superuser=True
            )
            
            db.add(admin_user)
            db.commit()
            db.refresh(admin_user)
            
            print(f"✅ Initial superuser created:")
            print(f"   Email: {settings.FIRST_SUPERUSER_EMAIL}")
            print(f"   Password: {settings.FIRST_SUPERUSER_PASSWORD}")
            print(f"   ⚠️  IMPORTANT: Change this password after first login!")
            
        else:
            print("✅ Superuser already exists in the database")
            
    except Exception as e:
        print(f"❌ Error creating initial user: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def create_sample_data():
    """Create sample birth records for testing (optional)"""
    db: Session = SessionLocal()
    
    try:
        # Check if sample data already exists
        existing_records = db.query(BirthRecord).count()
        
        if existing_records == 0:
            # Get admin user
            admin_user = db.query(User).filter(User.is_superuser == True).first()
            
            if admin_user:
                from datetime import date
                sample_records = [
                    BirthRecord(
                        record_date=date(2025, 1, 8),
                        ip_number="SAMPLE001",
                        admission_date=date(2025, 1, 8),
                        discharge_date=date(2025, 1, 10),
                        date_of_birth=date(2025, 1, 9),
                        gender="Male",
                        mode_of_delivery="Normal",
                        child_name="Sample Baby Boy",
                        father_name="Sample Father",
                        birth_notification_no="BN_SAMPLE_001",
                        created_by=admin_user.id
                    ),
                    BirthRecord(
                        record_date=date(2025, 1, 8),
                        ip_number="SAMPLE002",
                        admission_date=date(2025, 1, 8),
                        discharge_date=date(2025, 1, 10),
                        date_of_birth=date(2025, 1, 9),
                        gender="Female",
                        mode_of_delivery="C-Section",
                        child_name="Sample Baby Girl",
                        father_name="Sample Father 2",
                        birth_notification_no="BN_SAMPLE_002",
                        created_by=admin_user.id
                    )
                ]
                
                for record in sample_records:
                    db.add(record)
                
                db.commit()
                print("✅ Sample birth records created")
                
    except Exception as e:
        print(f"❌ Error creating sample data: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    print("🚀 Initializing database...")
    init_db()
    
    # Uncomment next line if you want sample data
    # create_sample_data()
    
    print("🎉 Database initialization completed!")