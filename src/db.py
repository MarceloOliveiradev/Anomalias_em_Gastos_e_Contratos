from sqlalchemy import create_engine
from src.config import settings

engine = create_engine(settings.database_url, future=True)