from datetime import datetime
from argon2 import PasswordHasher
from src.models.user import User, Preferences
from src.config.beanie_config import init_db

class UserManager:
    def __init__(self, name: str, lastname: str, email: str, phone: str, password_plain: str, num_consecutivo: int = 1):
        self.name = name
        self.lastname = lastname
        self.email = email
        self.phone = phone
        self.password_plain = password_plain
        self.num_consecutivo = num_consecutivo
        self.hasher = PasswordHasher()

    async def create_user(self) -> User:
        await init_db(document_models=[User])

        password_hash = self.hasher.hash(self.password_plain)

        user = User(
            name=self.name,
            lastname=self.lastname,
            email=self.email,
            phone=self.phone,
            password=password_hash,
            num_consecutivo=self.num_consecutivo,
            preferences=Preferences(),
            createdAt=datetime.now(),
            updatedAt=datetime.now(),
        )
        await user.insert()
        return user
