"""Unit tests for the pipeline transforms.

Each test builds a handful of rows by hand, runs one transform, and asserts on
the result. Small deliberate inputs catch logic errors that 1.3M real rows hide.
"""

import pytest
from pyspark.sql import Row

from transforms import (
    cast_types, flag_quality, split_quarantine, mask_pii, deduplicate,
    add_card_features, add_merchant_distance, add_temporal_features,
    build_daily_summary, PII_COLUMNS,
)


def bronze_row(**overrides):
    """A valid all-STRING bronze row. Override any field to make it invalid."""
    base = dict(
        _c0="1",
        trans_date_trans_time="2019-08-04 10:54:30",
        cc_num="4841313716651064",
        merchant="fraud_Example LLC",
        category="gas_transport",
        amt="61.15",
        first="Omar", last="Gay", gender="M",
        street="2161 Blankenship Freeway",
        city="West Hartford", state="VT", zip="5084",
        lat="43.7185", long="-72.4439", city_pop="140",
        job="Development worker", dob="1989-10-28",
        trans_num="abc123", unix_time="1344077670",
        merch_lat="44.208355", merch_long="-72.133812",
        is_fraud="0",
    )
    base.update(overrides)
    return Row(**base)


# --------------------------------------------------------------------------
# Casting
# --------------------------------------------------------------------------

def test_cast_types_produces_real_types(spark):
    df = cast_types(spark.createDataFrame([bronze_row()]))
    types = dict(df.dtypes)

    assert types["amt"] == "double"
    assert types["is_fraud"] == "int"
    assert types["trans_ts"] == "timestamp"
    assert types["unix_time"] == "bigint"
    assert "_c0" not in df.columns


def test_cast_types_nulls_unparseable_timestamp(spark):
    df = cast_types(spark.createDataFrame([bronze_row(trans_date_trans_time="not a date")]))
    assert df.first()["trans_ts"] is None


# --------------------------------------------------------------------------
# Quality rules and quarantine
# --------------------------------------------------------------------------

def test_clean_row_has_no_quality_failures(spark):
    df = flag_quality(cast_types(spark.createDataFrame([bronze_row()])))
    assert df.first()["_quality_failures"] == []


@pytest.mark.parametrize("overrides,expected_rule", [
    ({"amt": "-5.00"},                      "non_positive_amount"),
    ({"amt": "0"},                          "non_positive_amount"),
    ({"amt": None},                         "null_amount"),
    ({"trans_date_trans_time": "garbage"},  "unparseable_timestamp"),
    ({"is_fraud": "7"},                     "invalid_fraud_label"),
    ({"trans_num": None},                   "null_transaction_id"),
])
def test_bad_rows_are_flagged(spark, overrides, expected_rule):
    df = flag_quality(cast_types(spark.createDataFrame([bronze_row(**overrides)])))
    assert expected_rule in df.first()["_quality_failures"]


def test_quarantine_separates_good_from_bad(spark):
    rows = [
        bronze_row(trans_num="good1"),
        bronze_row(trans_num="bad1", amt="-1.00"),
        bronze_row(trans_num="good2"),
    ]
    flagged = flag_quality(cast_types(spark.createDataFrame(rows)))
    passed, quarantined = split_quarantine(flagged)

    assert passed.count() == 2
    assert quarantined.count() == 1
    assert quarantined.first()["trans_num"] == "bad1"
    # The reason travels with the rejected row.
    assert "_quality_failures" in quarantined.columns
    assert "_quality_failures" not in passed.columns


# --------------------------------------------------------------------------
# PII masking
# --------------------------------------------------------------------------

def test_mask_pii_removes_every_identifier(spark):
    df = mask_pii(cast_types(spark.createDataFrame([bronze_row()])))
    for col in PII_COLUMNS:
        assert col not in df.columns, f"{col} survived masking"


def test_card_id_is_a_stable_sha256(spark):
    rows = [bronze_row(trans_num="a"), bronze_row(trans_num="b")]
    ids = [r["card_id"] for r in
           mask_pii(cast_types(spark.createDataFrame(rows))).collect()]

    assert len(ids[0]) == 64                      # SHA-256 hex
    assert ids[0] == ids[1]                        # same card -> same id
    assert "4841313716651064" not in ids[0]        # not the card number


def test_customer_age_is_derived_from_dob(spark):
    # born 1989-10-28, transacting 2019-08-04 -> 29 (birthday not yet reached)
    df = mask_pii(cast_types(spark.createDataFrame([bronze_row()])))
    assert df.first()["customer_age"] == 29


# --------------------------------------------------------------------------
# Deduplication
# --------------------------------------------------------------------------

def test_deduplicate_removes_repeated_transaction_id(spark):
    rows = [bronze_row(trans_num="dup"), bronze_row(trans_num="dup"),
            bronze_row(trans_num="unique")]
    df = deduplicate(cast_types(spark.createDataFrame(rows)))
    assert df.count() == 2


# --------------------------------------------------------------------------
# Window features
# --------------------------------------------------------------------------

def card_history(spark):
    """One card, four transactions. The fourth is 3 days after the third,
    so it falls outside the 24-hour window."""
    rows = [
        bronze_row(trans_num="t1", amt="10.00", unix_time="1000000000",
                   trans_date_trans_time="2001-09-09 01:46:40"),
        bronze_row(trans_num="t2", amt="20.00", unix_time="1000003600",
                   trans_date_trans_time="2001-09-09 02:46:40"),
        bronze_row(trans_num="t3", amt="60.00", unix_time="1000007200",
                   trans_date_trans_time="2001-09-09 03:46:40"),
        bronze_row(trans_num="t4", amt="10.00", unix_time="1000300000",
                   trans_date_trans_time="2001-09-12 13:06:40"),
    ]
    return add_card_features(mask_pii(cast_types(spark.createDataFrame(rows))))


def test_txn_sequence_increments_in_time_order(spark):
    seqs = {r["trans_num"]: r["card_txn_seq"]
            for r in card_history(spark).collect()}
    assert seqs == {"t1": 1, "t2": 2, "t3": 3, "t4": 4}


def test_first_transaction_has_no_previous(spark):
    rows = {r["trans_num"]: r for r in card_history(spark).collect()}
    assert rows["t1"]["secs_since_prev_txn"] is None
    assert rows["t2"]["secs_since_prev_txn"] == 3600


def test_running_average_expands_over_history(spark):
    rows = {r["trans_num"]: r for r in card_history(spark).collect()}
    assert rows["t1"]["card_avg_amt"] == pytest.approx(10.0)
    assert rows["t2"]["card_avg_amt"] == pytest.approx(15.0)   # (10+20)/2
    assert rows["t3"]["card_avg_amt"] == pytest.approx(30.0)   # (10+20+60)/3


def test_24h_window_is_time_based_not_row_based(spark):
    """The regression test for rangeBetween vs rowsBetween.

    t4 is 3 days after t3, so the earlier transactions have aged out and its
    24h count must fall back to 1. With rowsBetween it would keep climbing.
    """
    rows = {r["trans_num"]: r for r in card_history(spark).collect()}
    assert rows["t3"]["txn_count_24h"] == 3
    assert rows["t4"]["txn_count_24h"] == 1


def test_amt_vs_card_average_flags_an_outlier(spark):
    rows = {r["trans_num"]: r for r in card_history(spark).collect()}
    # t3 is 60.00 against a running average of 30.00
    assert rows["t3"]["amt_vs_card_avg"] == pytest.approx(2.0)


def test_separate_cards_do_not_share_history(spark):
    rows = [
        bronze_row(trans_num="a1", cc_num="1111", unix_time="1000000000"),
        bronze_row(trans_num="b1", cc_num="2222", unix_time="1000000001"),
    ]
    out = add_card_features(mask_pii(cast_types(spark.createDataFrame(rows))))
    # Each card's first transaction is sequence 1 -- partitioning works.
    assert {r["card_txn_seq"] for r in out.collect()} == {1}


# --------------------------------------------------------------------------
# Distance
# --------------------------------------------------------------------------

def test_haversine_matches_known_distance(spark):
    """One degree of longitude at the equator is ~111.19 km."""
    row = bronze_row(lat="0.0", long="0.0", merch_lat="0.0", merch_long="1.0")
    df = add_merchant_distance(cast_types(spark.createDataFrame([row])))
    assert df.first()["merch_distance_km"] == pytest.approx(111.19, abs=0.1)


def test_zero_distance_when_merchant_is_at_home(spark):
    row = bronze_row(lat="43.7", long="-72.4", merch_lat="43.7", merch_long="-72.4")
    df = add_merchant_distance(cast_types(spark.createDataFrame([row])))
    assert df.first()["merch_distance_km"] == pytest.approx(0.0, abs=1e-6)


# --------------------------------------------------------------------------
# Temporal features
# --------------------------------------------------------------------------

def test_night_flag_set_for_early_hours(spark):
    night = bronze_row(trans_date_trans_time="2019-08-04 03:15:00")
    day   = bronze_row(trans_date_trans_time="2019-08-04 14:15:00")
    out = {r["hour_of_day"]: r["is_night"] for r in
           add_temporal_features(cast_types(spark.createDataFrame([night, day]))).collect()}
    assert out[3] == 1
    assert out[14] == 0


# --------------------------------------------------------------------------
# Daily summary
# --------------------------------------------------------------------------

def test_daily_summary_counts_and_rate(spark):
    rows = [
        bronze_row(trans_num="d1", amt="100.00", is_fraud="0",
                   trans_date_trans_time="2019-08-04 10:00:00"),
        bronze_row(trans_num="d2", amt="300.00", is_fraud="1",
                   trans_date_trans_time="2019-08-04 11:00:00"),
        bronze_row(trans_num="d3", amt="50.00",  is_fraud="0",
                   trans_date_trans_time="2019-08-05 10:00:00"),
    ]
    df = add_temporal_features(cast_types(spark.createDataFrame(rows)))
    summary = {str(r["txn_date"]): r for r in build_daily_summary(df).collect()}

    assert summary["2019-08-04"]["txn_count"] == 2
    assert summary["2019-08-04"]["fraud_count"] == 1
    assert summary["2019-08-04"]["total_amt"] == pytest.approx(400.0)
    assert summary["2019-08-04"]["fraud_rate_pct"] == pytest.approx(50.0)
    assert summary["2019-08-05"]["fraud_count"] == 0
