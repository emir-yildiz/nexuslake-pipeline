"""
NEXUSLAKE LOGGING MODULE
------------------------
Bu modül, proje genelinde standartlaştırılmış loglama formatı sağlar.
Tüm işlemlerin izlenebilirliğini (traceability) garanti altına alır.
"""

import logging
import sys


def get_logger(name: str):
    """Belirtilen isimle yapılandırılmış bir logger döndürür."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Konsol çıktısı
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger