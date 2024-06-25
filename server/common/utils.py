import logging
import os


def configure_log(
    logger_name, name_file="log.log", dir_mame="logs", nivel_registro=logging.DEBUG
):

    logger = logging.getLogger(logger_name)
    logger.setLevel(nivel_registro)

    if not os.path.exists(dir_mame):
        os.makedirs(dir_mame)

    name_file = os.path.join(dir_mame, name_file)

    file_handler = logging.FileHandler(name_file, mode="w")
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(nivel_registro)
    console_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    return logger
