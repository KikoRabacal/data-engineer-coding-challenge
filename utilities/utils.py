import zipfile
from pathlib import Path


def extract_zip_to_directory(path_to_zip_file: str,
                             directory_to_extract_to: str) -> None:
    """
    Extracts contents of a .zip file into a chose directory
    :param path_to_zip_file: location of the file to be decompressed
    :param directory_to_extract_to: where extracted files will be place
    """
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)


def remove_dir_recursively(dir_path: str) -> None:
    """
    Removes a directory and all files inside it
    :param dir_path: the directory to be removed
    """
    pth = Path(dir_path)
    for child in pth.glob('*'):
        if child.is_file():
            child.unlink()
    pth.rmdir()
