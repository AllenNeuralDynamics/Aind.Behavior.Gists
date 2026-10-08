"""Sniff detector QC metrics, computed on a real recording straight from S3.

Target: ``s3://aind-open-data/878045_2026-10-06_18-54-07/behavior/SniffDetector.harp``
(public, anonymous reads).
"""

import harpstore

import sniff_detector as sd

URI = "s3://aind-open-data/878045_2026-10-06_18-54-07/behavior/SniffDetector.harp"
STORAGE_OPTIONS = {"region": "us-west-2", "skip_signature": True}

# Thresholds (defaults from contraqctor's HarpSniffDetectorTestSuite)
QUANTIZATION_RATIO_THR = 0.1
CLUSTERING_THR = 0.05
CLIPPING_THR = 0.05
SUDDEN_JUMPS_THR = 0.001
NOTCH_FILTER_FREQ = 50
BREATHING_RATE_RANGE_HZ = (2, 10)
MIN_AMPLITUDE_FRACTION = 0.01  # peak-to-peak must span >= 1 % of the 12-bit range


def report(name: str, ok: bool, detail: object) -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")


def main() -> None:
    ds = harpstore.open_dataset(URI, storage_options=STORAGE_OPTIONS)
    signal = ds["RawVoltage"].read()["value"]
    fs = float(ds["RawVoltageDispatchRate"].read()["value"].iloc[-1])

    report("whoami", ds.device.WHO_AM_I == 1401, ds.device.WHO_AM_I)

    measured = sd.sampling_rate(signal)
    report(
        "sampling_rate",
        abs(measured - fs) <= 0.1,
        f"{measured:.2f} Hz (expected {fs} Hz)",
    )

    q = sd.signal_quality(signal)
    report(
        "quantization_ratio",
        q["quantization_ratio"] > QUANTIZATION_RATIO_THR,
        q["quantization_ratio"],
    )
    report(
        "clustering_ratio",
        q["clustering_ratio"] < CLUSTERING_THR,
        q["clustering_ratio"],
    )
    report("min_clipping", q["min_clipping"] < CLIPPING_THR, q["min_clipping"])
    report("max_clipping", q["max_clipping"] < CLIPPING_THR, q["max_clipping"])
    report(
        "sudden_jumps_ratio",
        q["sudden_jumps_ratio"] < SUDDEN_JUMPS_THR,
        q["sudden_jumps_ratio"],
    )

    _, filtered = sd.filter_signal(signal, fs, NOTCH_FILTER_FREQ)

    b = sd.breathing_metrics(filtered, fs)
    lo, hi = BREATHING_RATE_RANGE_HZ
    rate = b.get("breathing_rate_hz")
    report("breathing_rate", rate is not None and lo <= rate <= hi, b)

    a = sd.amplitude(filtered)
    report("amplitude", a["fraction_of_full_scale"] >= MIN_AMPLITUDE_FRACTION, a)


if __name__ == "__main__":
    main()
