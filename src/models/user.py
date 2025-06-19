from beanie import Document
from pydantic import EmailStr, BaseModel
from datetime import datetime
from typing import Optional


class Preferences(BaseModel):
    inventoryManagement: str = "standard"
    lastSalesPull: datetime = datetime.now()
    salesPullConcurrency: str = "monthly"
    notificationsFrequency: str = "Daily"


class User(Document):
    name: str
    lastname: str
    email: EmailStr
    phone: str
    password: str
    newUser: bool = True
    verified: bool = True
    __v: int = 0
    activeInventory: int = 2
    num_consecutivo: int = 1 
    preferences: Preferences = Preferences()
    createdAt: datetime = datetime.now()
    updatedAt: datetime = datetime.now()


    class Settings:
        name = "users"  
