# File: infra/airflow/dags/libs/fallback_manager.py
import os
import json
import logging
import random
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class FallbackManager:
    """
    Quan ly du lieu du phong tu thu muc data/seed.
    Giup pipeline tiep tuc chay khi API het han muc.
    """
    
    def __init__(self, seed_dir: str = "/opt/airflow/data/seed"):
        # Neu chay o local (ngoai docker), thay doi path
        if not os.path.exists(seed_dir):
            seed_dir = os.path.join(os.getcwd(), "data", "seed")
        
        self.seed_dir = seed_dir

    def get_seed_data(self, source_prefix: str) -> Dict[str, Any]:
        """
        Lay mot file du lieu mau ngau nhien theo prefix (google_local hoac tripadvisor).
        """
        try:
            if not os.path.exists(self.seed_dir):
                logger.warning(f"Seed directory not found: {self.seed_dir}")
                return {}

            # Lay danh sach file theo nguon
            files = [f for f in os.listdir(self.seed_dir) if f.startswith(source_prefix) and f.endswith(".json")]
            
            if not files:
                logger.warning(f"No seed files found for prefix: {source_prefix}")
                return {}

            # Lua chon ngau nhien mot file
            selected_file = random.choice(files)
            file_path = os.path.join(self.seed_dir, selected_file)
            
            logger.info(f"Using fallback data from: {selected_file}")
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading seed data: {e}")
            return {}

    def get_all_hanoi_attractions(self) -> List[Dict]:
        """Lay du lieu tu file hanoi_attractions_seed."""
        try:
            files = [f for f in os.listdir(self.seed_dir) if f.startswith("hanoi_attractions_seed") and f.endswith(".json")]
            if not files: return []
            
            with open(os.path.join(self.seed_dir, files[0]), "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
