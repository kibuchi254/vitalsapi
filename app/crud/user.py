from typing import Any, Dict, Optional, Union
from sqlalchemy.orm import Session
from app.core.security import get_password_hash, verify_password
from app.models.user import User
from app.schemas.user import UserCreate, UserUpdate
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CRUDUser:
    """
    CRUD operations for the User model.
    """
    def get(self, db: Session, id: int) -> Optional[User]:
        """
        Retrieve a user by their ID.
        """
        return db.query(User).filter(User.id == id).first()

    def get_by_email(self, db: Session, *, email: str) -> Optional[User]:
        """
        Retrieve a user by their email address.
        """
        return db.query(User).filter(User.email == email).first()

    def create(self, db: Session, *, obj_in: UserCreate) -> User:
        """
        Create a new user.
        """
        db_obj = User(
            email=obj_in.email,
            hashed_password=get_password_hash(obj_in.password),
            full_name=obj_in.full_name,
            is_active=obj_in.is_active,
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        logger.info(f"User created with email: {db_obj.email}")
        return db_obj

    def update(
        self, db: Session, *, db_obj: User, obj_in: Union[UserUpdate, Dict[str, Any]]
    ) -> User:
        """
        Update an existing user.
        """
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.dict(exclude_unset=True)
            
        if "password" in update_data:
            hashed_password = get_password_hash(update_data["password"])
            del update_data["password"]
            update_data["hashed_password"] = hashed_password
        
        for field, value in update_data.items():
            setattr(db_obj, field, value)
        
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        logger.info(f"User updated with email: {db_obj.email}")
        return db_obj

    def authenticate(self, db: Session, *, email: str, password: str) -> Optional[User]:
        """
        Authenticate a user by email and password.
        Returns the user object if successful, otherwise None.
        """
        user = self.get_by_email(db, email=email)
        
        if not user:
            logger.warning(f"Authentication failed for email '{email}': User not found.")
            return None
        
        if not verify_password(password, getattr(user, "hashed_password", "")):
            logger.warning(f"Authentication failed for email '{email}': Incorrect password.")
            return None

        logger.info(f"Authentication successful for user: {user.email}")
        return user

    def is_active(self, user: User) -> bool:
        """
        Check if a user is active.
        """
        return bool(user.is_active)

    def is_superuser(self, user: User) -> bool:
        """
        Check if a user has superuser privileges.
        """
        return bool(user.is_superuser)

user = CRUDUser()