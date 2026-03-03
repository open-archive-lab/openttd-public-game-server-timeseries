import logging
import shutil

from config import settings
from database import close_db

logger = logging.getLogger(__name__)


def clean():
    # Archive downloaded files
    if settings.DOWNLOAD_DIR.exists():
        # Ensure archive directory exists
        if not settings.ARCHIVE_DIR.exists():
            settings.ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

        for downloaded_path in settings.DOWNLOAD_DIR.rglob("*"):
            if downloaded_path.is_file():
                relative_path = downloaded_path.relative_to(
                    settings.DOWNLOAD_DIR
                )
                archive_path = settings.ARCHIVE_DIR / relative_path
                archive_path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Archiving: {downloaded_path} -> {archive_path}.")
                try:
                    shutil.move(src=downloaded_path, dst=archive_path)
                except Exception as e:
                    logger.error(f"Error moving {downloaded_path.name}: {e}")

    # Delete Staging Database file
    if settings.STAGING_DB.exists():
        close_db()
        try:
            logger.info(f"Removing staging database: {settings.STAGING_DB}.")
            settings.STAGING_DB.unlink()
        except Exception as e:
            logger.error(f"Error removing staging database: {e}")
    else:
        logger.info("Staging database not found.")

    logger.info("Cleanup complete.")


if __name__ == "__main__":
    clean()
