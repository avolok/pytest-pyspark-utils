"""Utilities for pytest-pyspark-utils."""

from pathlib import Path

DELTA_JAR_MAPPING: dict[tuple[int, int], str] = {
    (4, 0): "io.delta:delta-spark_2.13:4.0.1",
    (3, 5): "io.delta:delta-spark_2.12:3.3.2",
    (3, 4): "io.delta:delta-core_2.12:2.4.0",
    (3, 3): "io.delta:delta-core_2.12:2.3.0",
    (3, 2): "io.delta:delta-core_2.12:2.0.2",
}


def determine_delta_jar(pyspark_version: str | None = None) -> str:
    """Return Delta Lake Maven coordinates for the given PySpark version.

    Args:
        pyspark_version: PySpark version string (e.g. ``"4.0.2"``).
            When ``None``, reads ``pyspark.__version__`` at call time.

    Returns:
        Maven coordinate string, e.g. ``"io.delta:delta-spark_2.13:4.0.0"``.

    Raises:
        ValueError: If the PySpark major.minor version is not in the mapping.
    """
    if pyspark_version is None:
        import pyspark

        pyspark_version = pyspark.__version__

    parts = pyspark_version.split(".")
    major, minor = int(parts[0]), int(parts[1])
    key = (major, minor)

    if key not in DELTA_JAR_MAPPING:
        supported = ", ".join(f"{m}.{n}" for m, n in sorted(DELTA_JAR_MAPPING, reverse=True))
        raise ValueError(f"Unsupported PySpark version: {pyspark_version}. Supported major.minor versions: {supported}")

    return DELTA_JAR_MAPPING[key]


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
