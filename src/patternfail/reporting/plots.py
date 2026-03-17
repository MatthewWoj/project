from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd


def plot_failure_rates(df: pd.DataFrame, out_path: str) -> None:
    if df.empty:
        return
    ax = df.plot(kind="bar", x="context", y="failure_rate", legend=False, figsize=(8, 4))
    ax.set_ylabel("Failure rate")
    ax.figure.tight_layout()
    ax.figure.savefig(out_path)
    plt.close(ax.figure)
