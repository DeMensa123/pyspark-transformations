import pytest
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from scripts.hash_utils import get_md4_hash
from scripts.transformations import add_claim_type, add_claim_period, add_claim_priority, add_source_system_id


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("PySparkTestApp")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_claim_type_transformation(spark):
    df = spark.createDataFrame(
        [("CL_123",), ("RX_456",), ("AB_789",)],
        ["claim_id"]
    )

    transformed = add_claim_type(df)

    expected = spark.createDataFrame(
        [
            ("CL_123", "Coinsurance"),
            ("RX_456", "Reinsurance"),
            ("AB_789", None)
        ],
        ["claim_id", "claim_type"]
    )

    assertDataFrameEqual(transformed, expected)


def test_claim_priority_transformation(spark):
    df = spark.createDataFrame(
        [(4000,), (3999,), (4001,)],
        ["claim_amount"]
    )
    transformed = add_claim_priority(df)

    expected = spark.createDataFrame(
        [
            (4000, "Normal"),
            (3999, "Normal"),
            (4001, "Urgent")
        ],
        ["claim_amount", "claim_priority"]
    )

    assertDataFrameEqual(transformed, expected)


def test_claim_period_transformation(spark):
    df = spark.createDataFrame(
        [("2024-07-15",), ("2023-12-01",)],
        ["claim_date"]
    )
    transformed = add_claim_period(df)

    expected = spark.createDataFrame(
        [
            ("2024-07-15", "2024-07"),
            ("2023-12-01", "2023-12")
        ],
        ["claim_date", "claim_period"]
    )

    assertDataFrameEqual(transformed, expected)


def test_source_system_id_extraction(spark):
    df = spark.createDataFrame(
        [("CL_123",), ("RX_4567",)],
        ["claim_id"]
    )
    transformed = add_source_system_id(df)

    expected = spark.createDataFrame(
        [
            ("CL_123", "123"),
            ("RX_4567", "4567")
        ],
        ["claim_id", "source_system_id"]
    )

    assertDataFrameEqual(transformed, expected)


@patch("requests.get")
def test_get_md4_hash(mock_get):
    # Mock API JSON response
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"Digest": "abc123claimfakehash"}

    claim_id = "CL_123"
    result = get_md4_hash(claim_id)

    assert result == "abc123claimfakehash"
    mock_get.assert_called_once_with(f"https://api.hashify.net/hash/md4/hex?value={claim_id}", timeout=10)


