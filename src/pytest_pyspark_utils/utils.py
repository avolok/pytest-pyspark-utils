"""File-discovery utilities for pytest-pyspark-utils."""

from pathlib import Path


def determine_file_path(base_path: str, filename: str) -> str:
    """Find the unique CSV or JSONL file matching *filename* in *base_path*.

    Args:
        base_path: Directory to search in.
        filename: Stem name (no extension) to match.

    Returns:
        Absolute path string to the matched file.

    Raises:
        FileNotFoundError: If no matching file exists.
        FileExistsError: If more than one matching file exists.
    """
    file_matches = [file for file in Path(base_path).glob(f"{filename}.*") if file.suffix in [".jsonl", ".csv"]]

    if not file_matches:
        raise FileNotFoundError(f"No file found for {filename} in {base_path}")
    elif len(file_matches) > 1:
        raise FileExistsError(
            f"Multiple files found for {filename} in {base_path}: {[file.name for file in file_matches]}. "
            f"Please ensure there is only one file for {filename} in the directory."
        )
    else:
        return f"{base_path}/{file_matches[0].name}"
