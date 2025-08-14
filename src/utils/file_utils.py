"""
File management utilities for VAHAN web scraper.
Handles file operations, directory management, and data export.
"""

import os
import shutil
from pathlib import Path
from typing import List, Optional, Union
import pandas as pd
from datetime import datetime

from ..core.config import Config
from ..core.exceptions import ExportError
from ..utils.logging_utils import get_logger

class FileManager:
    """Utility class for file and directory management."""
    
    def __init__(self):
        """Initialize the file manager."""
        self.logger = get_logger(self.__class__.__name__)
        Config.ensure_directories()
    
    def save_dataframe(self, df: pd.DataFrame, filename: str, 
                      directory: Path = None, format: str = 'csv') -> str:
        """Save DataFrame to file.
        
        Args:
            df: DataFrame to save
            filename: Output filename
            directory: Target directory (uses output dir if None)
            format: File format ('csv', 'excel', 'json')
            
        Returns:
            str: Full path to saved file
            
        Raises:
            ExportError: If save operation fails
        """
        try:
            if directory is None:
                directory = Config.OUTPUT_DIR
            
            # Ensure directory exists
            directory.mkdir(parents=True, exist_ok=True)
            
            # Add timestamp if not present
            if not any(char.isdigit() for char in filename):
                timestamp = datetime.now().strftime(Config.TIMESTAMP_FORMAT)
                name_parts = filename.rsplit('.', 1)
                if len(name_parts) == 2:
                    filename = f"{name_parts[0]}_{timestamp}.{name_parts[1]}"
                else:
                    filename = f"{filename}_{timestamp}.{format}"
            
            filepath = directory / filename
            
            # Save based on format
            if format.lower() == 'csv':
                df.to_csv(filepath, index=False)
            elif format.lower() == 'excel':
                df.to_excel(filepath, index=False)
            elif format.lower() == 'json':
                df.to_json(filepath, orient='records', indent=2)
            else:
                raise ExportError(f"Unsupported format: {format}")
            
            self.logger.info(f"💾 Saved {len(df)} records to {filepath}")
            return str(filepath)
            
        except Exception as e:
            raise ExportError(f"Failed to save DataFrame: {e}")
    
    def load_dataframe(self, filepath: Union[str, Path], 
                      format: str = None) -> pd.DataFrame:
        """Load DataFrame from file.
        
        Args:
            filepath: Path to file
            format: File format (auto-detected if None)
            
        Returns:
            pd.DataFrame: Loaded data
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ExportError: If load operation fails
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        try:
            # Auto-detect format if not specified
            if format is None:
                format = filepath.suffix.lower().lstrip('.')
            
            if format == 'csv':
                df = pd.read_csv(filepath)
            elif format in ['xlsx', 'xls', 'excel']:
                df = pd.read_excel(filepath)
            elif format == 'json':
                df = pd.read_json(filepath)
            else:
                raise ExportError(f"Unsupported format: {format}")
            
            self.logger.info(f"📖 Loaded {len(df)} records from {filepath}")
            return df
            
        except Exception as e:
            raise ExportError(f"Failed to load DataFrame: {e}")
    
    def create_backup(self, source_path: Union[str, Path], 
                     backup_dir: Path = None) -> str:
        """Create backup of a file or directory.
        
        Args:
            source_path: Path to backup
            backup_dir: Backup directory (uses default if None)
            
        Returns:
            str: Path to backup
        """
        source_path = Path(source_path)
        
        if backup_dir is None:
            backup_dir = Config.BASE_DIR / "backups"
        
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Create timestamped backup name
        timestamp = datetime.now().strftime(Config.TIMESTAMP_FORMAT)
        backup_name = f"{source_path.name}_{timestamp}"
        backup_path = backup_dir / backup_name
        
        try:
            if source_path.is_file():
                shutil.copy2(source_path, backup_path)
            elif source_path.is_dir():
                shutil.copytree(source_path, backup_path)
            else:
                raise FileNotFoundError(f"Source path not found: {source_path}")
            
            self.logger.info(f"📦 Created backup: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to create backup: {e}")
            raise
    
    def cleanup_old_files(self, directory: Path, 
                         days_old: int = 30, 
                         pattern: str = "*") -> int:
        """Clean up old files in a directory.
        
        Args:
            directory: Directory to clean
            days_old: Files older than this many days will be deleted
            pattern: File pattern to match
            
        Returns:
            int: Number of files deleted
        """
        if not directory.exists():
            return 0
        
        cutoff_time = datetime.now().timestamp() - (days_old * 24 * 60 * 60)
        deleted_count = 0
        
        try:
            for file_path in directory.glob(pattern):
                if file_path.is_file() and file_path.stat().st_mtime < cutoff_time:
                    file_path.unlink()
                    deleted_count += 1
                    self.logger.debug(f"🗑️ Deleted old file: {file_path}")
            
            if deleted_count > 0:
                self.logger.info(f"🧹 Cleaned up {deleted_count} old files from {directory}")
            
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"❌ Error during cleanup: {e}")
            return 0
    
    def get_file_info(self, filepath: Union[str, Path]) -> dict:
        """Get information about a file.
        
        Args:
            filepath: Path to file
            
        Returns:
            dict: File information
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            return {"exists": False}
        
        stat = filepath.stat()
        
        return {
            "exists": True,
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "created": datetime.fromtimestamp(stat.st_ctime),
            "modified": datetime.fromtimestamp(stat.st_mtime),
            "is_file": filepath.is_file(),
            "is_directory": filepath.is_dir(),
            "extension": filepath.suffix.lower()
        }
    
    def list_files(self, directory: Path, 
                  pattern: str = "*", 
                  recursive: bool = False) -> List[Path]:
        """List files in a directory.
        
        Args:
            directory: Directory to search
            pattern: File pattern to match
            recursive: Whether to search recursively
            
        Returns:
            List[Path]: List of matching files
        """
        if not directory.exists():
            return []
        
        try:
            if recursive:
                files = list(directory.rglob(pattern))
            else:
                files = list(directory.glob(pattern))
            
            # Filter to files only
            files = [f for f in files if f.is_file()]
            
            return sorted(files)
            
        except Exception as e:
            self.logger.error(f"❌ Error listing files: {e}")
            return []
    
    def ensure_directory(self, directory: Union[str, Path]) -> Path:
        """Ensure a directory exists, creating it if necessary.
        
        Args:
            directory: Directory path
            
        Returns:
            Path: Directory path
        """
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        return directory
    
    def get_available_filename(self, directory: Path, 
                              base_name: str, 
                              extension: str = 'csv') -> str:
        """Get an available filename by adding numbers if file exists.
        
        Args:
            directory: Target directory
            base_name: Base filename
            extension: File extension
            
        Returns:
            str: Available filename
        """
        counter = 1
        filename = f"{base_name}.{extension}"
        
        while (directory / filename).exists():
            filename = f"{base_name}_{counter}.{extension}"
            counter += 1
        
        return filename
