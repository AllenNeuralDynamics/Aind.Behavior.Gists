# Sniff detector QC (proof of concept)

Re-implementation of the Harp SniffDetector QC checks from
[contraqctor](https://github.com/AllenNeuralDynamics/contraqctor/blob/main/src/contraqctor/qc/harp/sniff_detector.py)
as plain `pytest` tests that read the data directly from S3 with
[harpstore](https://github.com/bruno-f-cruz/harpstore) (no download, no contraqctor runner, no plots).

Target: `s3://aind-open-data/878045_2026-10-06_18-54-07/behavior/SniffDetector.harp` (public).

| Test | What it checks |
|---|---|
| `test_whoami` | Device WhoAmI is 1401 |
| `test_sampling_rate` | Measured rate matches `RawVoltageDispatchRate` (±0.1 Hz) |
| `test_signal_quality` | Quantization, clustering, clipping and sudden-jump ratios |
| `test_physiological_relevance` | Peak detection after notch/high/low-pass; breathing rate within 2-10 Hz |
| `test_amplitude` | Peak-to-peak (1st-99th percentile) of the filtered signal as a fraction of the 12-bit range (new) |

Metric computation lives in `sniff_detector.py`; thresholds live at the top of `test_sniff_detector.py`.

```bash
uv sync
uv run pytest -s   # -s prints the computed metrics
```

Tests skip automatically if the bucket is unreachable.
