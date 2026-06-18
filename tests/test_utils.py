from unittest.mock import patch

import pytest

from pytest_pyspark_utils.utils import determine_delta_jar


@pytest.mark.parametrize(
    "version, expected",
    [
        ("4.0.0", "io.delta:delta-spark_2.13:4.0.1"),
        ("4.0.2", "io.delta:delta-spark_2.13:4.0.1"),
        ("3.5.0", "io.delta:delta-spark_2.12:3.3.2"),
        ("3.5.3", "io.delta:delta-spark_2.12:3.3.2"),
        ("3.4.0", "io.delta:delta-core_2.12:2.4.0"),
        ("3.4.1", "io.delta:delta-core_2.12:2.4.0"),
        ("3.3.0", "io.delta:delta-core_2.12:2.3.0"),
        ("3.3.2", "io.delta:delta-core_2.12:2.3.0"),
        ("3.2.0", "io.delta:delta-core_2.12:2.0.2"),
        ("3.2.4", "io.delta:delta-core_2.12:2.0.2"),
    ],
)
def test_determine_delta_jar(version, expected):
    assert determine_delta_jar(version) == expected


@pytest.mark.parametrize("version", ["3.1.0", "5.0.0", "2.4.8"])
def test_determine_delta_jar_unsupported_version(version):
    with pytest.raises(ValueError, match=version):
        determine_delta_jar(version)


def test_determine_delta_jar_default_reads_pyspark_version():
    with patch("pyspark.__version__", "3.5.2"):
        assert determine_delta_jar() == "io.delta:delta-spark_2.12:3.3.2"
