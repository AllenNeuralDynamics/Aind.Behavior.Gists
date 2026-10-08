# Sniff detector QC (proof of concept)

Re-implementation of the Harp SniffDetector QC checks from
[contraqctor](https://github.com/AllenNeuralDynamics/contraqctor/blob/main/src/contraqctor/qc/harp/sniff_detector.py)
as a plain script that read the data directly from S3 with
[harpstore](https://github.com/bruno-f-cruz/harpstore) (no download, no contraqctor runner, no test framework, no plots).

Target: `s3://aind-open-data/878045_2026-10-06_18-54-07/behavior/SniffDetector.harp` (public).

| Check | What it measures |
|---|---|
| whoami | Device WhoAmI is 1401 |
| sampling_rate | Measured rate matches `RawVoltageDispatchRate` (±0.1 Hz) |
| quantization / clustering / clipping / sudden jumps | Raw-signal quality ratios |
| breathing_rate | Peak detection after notch/high/low-pass; rate within 2-10 Hz |
| amplitude | Std, RMS and peak-to-peak (1st-99th percentile) of the filtered signal, as a fraction of the 12-bit range (new) |

Metric computation lives in `sniff_detector.py`; `example.py` loads the data, prints PASS/FAIL per check and holds the thresholds.

```bash
uv sync
uv run example.py
```

