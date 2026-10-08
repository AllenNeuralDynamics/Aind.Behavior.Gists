"""Pure QC metric functions for a Harp SniffDetector raw-voltage signal (no plotting)."""

import numpy as np
import pandas as pd
from scipy.interpolate import interp1d
from scipy.signal import butter, filtfilt, find_peaks, iirnotch

FULL_BIT_DEPTH = 2**12


def sampling_rate(signal: pd.Series) -> float:
    """Mean sampling rate (Hz) from the harp timestamps in the index."""
    return 1.0 / float(np.mean(np.diff(signal.index.values)))


def signal_quality(signal: pd.Series) -> dict[str, float]:
    """Quantization, clustering, clipping and sudden-jump ratios of the raw signal."""
    values = signal.values.astype(float)
    n = len(values)

    hist, _ = np.histogram(values, bins=FULL_BIT_DEPTH)
    tol = (values.max() - values.min()) * 0.01
    derivative = np.diff(values) / np.diff(signal.index.values)

    return {
        "quantization_ratio": len(np.unique(values)) / FULL_BIT_DEPTH,
        "clustering_ratio": hist.max() / n,
        "min_clipping": np.sum(np.abs(values - values.min()) < tol) / n,
        "max_clipping": np.sum(np.abs(values - values.max()) < tol) / n,
        "sudden_jumps_ratio": np.sum(np.abs(derivative) > 3 * np.std(derivative)) / n,
    }


def filter_signal(
    signal: pd.Series, fs: float, notch_freq: float = 50.0
) -> tuple[np.ndarray, np.ndarray]:
    """Resample to a uniform grid, then notch + 0.2 Hz high-pass + 15 Hz low-pass.

    Returns (t_uniform, filtered).
    """
    t = signal.index.values
    t_uniform = np.arange(t[0], t[-1], 1.0 / fs)
    y = interp1d(
        t, signal.values, kind="linear", bounds_error=False, fill_value="extrapolate"
    )(t_uniform)

    b, a = iirnotch(notch_freq, 30.0, fs)
    y = filtfilt(b, a, y)
    b, a = butter(2, 0.2, "highpass", fs=fs)
    y = filtfilt(b, a, y)
    b, a = butter(2, 15, "lowpass", fs=fs)
    return t_uniform, filtfilt(b, a, y)


def breathing_metrics(filtered: np.ndarray, fs: float) -> dict[str, float]:
    """Peak detection on the filtered signal and inter-peak-interval statistics."""
    peaks, _ = find_peaks(filtered, height=0.5 * np.std(filtered), prominence=2.5)
    metrics: dict[str, float] = {"num_peaks": len(peaks)}
    if len(peaks) >= 2:
        ipi = np.diff(peaks) / fs
        metrics.update(
            mean_ipi=np.mean(ipi),
            std_ipi=np.std(ipi),
            breathing_rate_hz=1.0 / np.mean(ipi),
            # fastest / slowest 1 % of cycles, expressed as rates
            rate_p99_hz=1.0 / np.percentile(ipi, 1),
            rate_p01_hz=1.0 / np.percentile(ipi, 99),
        )
    return metrics


def amplitude(filtered: np.ndarray) -> dict[str, float]:
    """Amplitude of the band-passed sniff signal, in ADC counts.

    Uses robust percentiles so isolated spikes don't dominate; ``fraction_of_full_scale``
    puts the peak-to-peak value in context of the 12-bit range.
    """
    p01, p99 = np.percentile(filtered, [1, 99])
    peak_to_peak = float(p99 - p01)
    return {
        "std": float(np.std(filtered)),
        "rms": float(np.sqrt(np.mean(filtered**2))),
        "peak_to_peak": peak_to_peak,
        "fraction_of_full_scale": peak_to_peak / FULL_BIT_DEPTH,
    }
