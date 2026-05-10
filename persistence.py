import os
import json
import shutil
import tempfile
from logger import logger

def safe_load_json(file_path, default=None):
    """
    Safely load a JSON file. If corrupted or missing, attempts to restore from backups.
    Returns the loaded data or the default value.
    """
    if default is None:
        default = {}
        
    if not os.path.exists(file_path):
        return default
        
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"JSON corruption detected in {file_path}: {e}")
        # Try to load from backup if corrupted
        for i in range(1, 4):
            backup_path = f"{file_path}.bak{i}"
            if os.path.exists(backup_path):
                try:
                    logger.info(f"Attempting to restore from {backup_path}")
                    with open(backup_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        # If successful, restore this backup as the main file
                        logger.info(f"Successfully restored {file_path} from backup {i}")
                        safe_save_json(file_path, data)
                        return data
                except Exception as backup_e:
                    logger.error(f"Backup {backup_path} also corrupted: {backup_e}")
                    
        logger.error(f"Failed to restore {file_path} from any backup. Returning default.")
        return default

def safe_save_json(file_path, data, indent=2):
    """
    Safely save data to a JSON file using atomic writes and rotating backups.
    Prevents partial write corruption.
    """
    try:
        # 1. Rotate backups (keep last 3)
        if os.path.exists(file_path):
            # Shift backups: .bak2 -> .bak3, .bak1 -> .bak2
            for i in range(2, 0, -1):
                src = f"{file_path}.bak{i}"
                dst = f"{file_path}.bak{i+1}"
                if os.path.exists(src):
                    shutil.copy2(src, dst)
            # Create newest backup from current file
            shutil.copy2(file_path, f"{file_path}.bak1")
            
        # 2. Atomic write
        dirname = os.path.dirname(os.path.abspath(file_path))
        if not dirname:
            dirname = "."
            
        fd, temp_path = tempfile.mkstemp(dir=dirname, suffix=".tmp")
        with os.fdopen(fd, 'w', encoding="utf-8") as f:
            json.dump(data, f, indent=indent)
            f.flush()
            os.fsync(f.fileno()) # Ensure it's written to disk before renaming
            
        # 3. Replace target atomically
        os.replace(temp_path, file_path)
    except Exception as e:
        logger.error(f"Error atomically saving {file_path}: {e}")
