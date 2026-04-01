from motor.motor_asyncio import AsyncIOMotorClient
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

mongo_client: AsyncIOMotorClient = None


async def init_db():
    global mongo_client
    try:
        mongo_client = AsyncIOMotorClient(settings.MONGO_URI)
        await mongo_client.admin.command("ping")
        logger.info("MongoDB connected successfully")
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise


def get_db():
    return mongo_client["tourism_connections"]


def get_collection(name: str):
    return get_db()[name]