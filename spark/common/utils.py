import logging
import os
import sys
import subprocess

file_path = os.path.dirname(__file__)
sys.path.append(file_path)


def configure_log(
    logger_name, name_file="log.log", dir_mame="logs", nivel_registro=logging.DEBUG
):
    project_root = os.path.join(os.path.dirname(__file__), "..")

    logger = logging.getLogger(logger_name)
    logger.setLevel(nivel_registro)

    dir_mame = os.path.join(project_root, dir_mame)

    if not os.path.exists(dir_mame):
        os.makedirs(dir_mame)

    name_file = os.path.join(dir_mame, name_file)

    file_handler = logging.FileHandler(name_file, mode="a")
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


def create_hdfs_directory(hdfs_path):
    logger = configure_log("utils.createHdfsDirectory", "utils.log")
    try:
        subprocess.run(
            f"hdfs dfs -mkdir -p {hdfs_path}",
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.info(f"Directory {hdfs_path} created successfully in HDFS.")
    except subprocess.CalledProcessError as e:
        if "File exists" not in e.stderr.decode():
            logger.error(
                f"Error creating directory {hdfs_path} in HDFS: {e.stderr.decode()}"
            )
            logger.error(
                f"Command: hdfs dfs -mkdir -p {hdfs_path} failed with return code {e.returncode} and error message: {e.stderr.decode()}"
            )
        else:
            logger.info(f"Directory {hdfs_path} already exists in HDFS.")


def upload_to_hdfs(local_path, hdfs_path, method="put"):
    logger = configure_log("utils.uploadToHdfs", "utils.log")

    if not os.path.exists(local_path):
        logger.error(f"File or directory {local_path} does not exist.")
        raise FileNotFoundError(f"File or directory {local_path} does not exist.")

    if method not in ["put", "copyFromLocal", "moveFromLocal"]:
        logger.error(f"Method must be 'put', 'copyFromLocal' or 'moveFromLocal'.")
        raise ValueError("Method must be 'put', 'copyFromLocal' or 'moveFromLocal'.")

    hdfs_command = f"hdfs dfs -{method} -p {local_path} {hdfs_path}"

    try:
        result = subprocess.run(
            hdfs_command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.info(result.stdout.decode())
        logger.info(
            f"File or directory {local_path} uploaded successfully to {hdfs_path} using method '{method}'."
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"Error uploading {local_path} to HDFS: {e.stderr.decode()}")
        logger.error(
            f"Command: {hdfs_command} failed with return code {e.returncode} and error message: {e.stderr.decode()}"
        )


def upload_directory_to_hdfs(local_dir, hdfs_dir, method="put"):
    logger = configure_log("utils.uploadDirectoryToHdfs", "utils.log")
    if not os.path.exists(local_dir):
        logger.error(f"Directory {local_dir} does not exist.")
        raise FileNotFoundError(f"Direcotry {local_dir} does not exist.")

    create_hdfs_directory(hdfs_dir)

    if method not in ["put", "copyFromLocal", "moveFromLocal"]:
        logger.error("Method must be 'put', 'copyFromLocal' or 'moveFromLocal'.")
        raise ValueError("Method must be 'put', 'copyFromLocal' or 'moveFromLocal'.")

    for root, _, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            hdfs_path = os.path.join(hdfs_dir, relative_path)
            upload_to_hdfs(local_path, hdfs_path, method)
    logger.info(
        f"Directory {local_dir} uploaded successfully to {hdfs_dir} using method '{method}'."
    )
