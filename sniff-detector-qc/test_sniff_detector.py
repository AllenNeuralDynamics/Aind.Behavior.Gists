"""Sniff detector QC checks, run against a real recording on S3.

Target: ``s3://aind-open-data/878045_2026-10-06_18-54-07/behavior/SniffDetector.harp``
(public, anonymous reads). Auto-skips when the bucket isn't reachable.
"""

import socket

import pytest

import harpstore
import sniff_detector as sd

BUCKET = "aind-open-data"
URI = f"s3://{BUCKET}/878045_2026-10-06_18-54-07/behavior/SniffDetector.harp"
STORAGE_OPTIONS = {"region": "us-west-2", "skip_signature": True}

# Thresholds (defaults from contraqctor's HarpSniffDetectorTestSuite)
QUANTIZATION_RATIO_THR = 0.1
CLUSTERING_THR = 0.05
CLIPPING_THR = 0.05
SUDDEN_JUMPS_THR = 0.001
NOTCH_FILTER_FREQ = 50
BREATHING_RATE_RANGE_HZ = (2, 10)
MIN_AMPLITUDE_FRACTION = 0.01  # peak-to-peak must span >= 1 % of the 12-bit range


def _bucket_reachable() -> bool:
    try:
        socket.create_connection((f"{BUCKET}.s3.amazonaws.com", 443), timeout=5).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _bucket_reachable(), reason=f"{BUCKET} not reachable")


@pytest.fixture(scope="module")
def dataset():
    return harpstore.open_dataset(URI, storage_options=STORAGE_OPTIONS)


@pytest.fixture(scope="module")
def signal(dataset):
    return dataset["RawVoltage"].read()["value"]


@pytest.fixture(scope="module")
def fs(dataset):
    return float(dataset["RawVoltageDispatchRate"].read()["value"].iloc[-1])


@pytest.fixture(scope="module")
def filtered(signal, fs):
    return sd.filter_signal(signal, fs, NOTCH_FILTER_FREQ)[1]


def test_whoami(dataset):
    assert dataset.device.WHO_AM_I == 1401


def test_sampling_rate(signal, fs):
    measured = sd.sampling_rate(signal)
    assert abs(measured - fs) <= 0.1, f"Expected {fs} Hz but got {measured:.2f} Hz"


def test_signal_quality(signal):
    m = sd.signal_quality(signal)
    print(m)
    assert m["quantization_ratio"] > QUANTIZATION_RATIO_THR
    assert m["clustering_ratio"] < CLUSTERING_THR
    assert m["min_clipping"] < CLIPPING_THR
    assert m["max_clipping"] < CLIPPING_THR
    assert m["sudden_jumps_ratio"] < SUDDEN_JUMPS_THR


def test_physiological_relevance(filtered, fs):
    m = sd.breathing_metrics(filtered, fs)
    print(m)
    assert m["num_peaks"] >= 2, "Failed to detect sufficient peaks in the breathing signal"
    lo, hi = BREATHING_RATE_RANGE_HZ
    assert lo <= m["breathing_rate_hz"] <= hi


def test_amplitude(filtered):
    m = sd.amplitude(filtered)
    print(m)
    assert m["fraction_of_full_scale"] >= MIN_AMPLITUDE_FRACTION
