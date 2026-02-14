import os
import json
import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import time

LOG_DIR = "scripts/reports"
os.makedirs(LOG_DIR, exist_ok=True)

timestamp = time.strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"analysis_terminal_{timestamp}.txt")

class Tee:
    def __init__(self, *files):
        self.files = files

    def write(self, obj):
        for f in self.files:
            f.write(obj)
            f.flush()

    def flush(self):
        for f in self.files:
            f.flush()

log_f = open(LOG_FILE, "w", encoding="utf-8")
sys.stdout = Tee(sys.stdout, log_f)
sys.stderr = Tee(sys.stderr, log_f)


SCORED_DIR = "scripts/scored"
OUT_DIR = "scripts/reports"
os.makedirs(OUT_DIR, exist_ok=True)

CRITERIA = ["correctness", "completeness", "groundedness"]

def load_latest():
    files = sorted(glob.glob(os.path.join(SCORED_DIR, "scored_*.jsonl")))
    if not files:
        raise RuntimeError("No scored files found.")
    return files[-1]

def print_stats(df, label):
    print(f"STATISTICS — {label}")
    print("\n")
    for crit in CRITERIA:
        print(f"\n{crit.upper()}")
        grp = df.groupby("mode")[crit]
        stats = grp.agg(["mean","median","std"])
        print(stats)

import seaborn as sns
import matplotlib.pyplot as plt
import os

def plot_distribution(df, label):
    for crit in CRITERIA:
        plt.figure(figsize=(10, 6))
        
        for mode in df["mode"].unique():
            subset = df[df["mode"] == mode]
            
            sns.kdeplot(
                data=subset,
                x=crit,
                label=mode,
                linewidth=2.2,         
            )
        
        plt.title(f"Distribution of {label} by {crit}")
        plt.xlabel(crit)
        plt.ylabel("Density")
        plt.legend()
        plt.grid(True, alpha=0.3, linestyle='--')
        plt.tight_layout()
        
        # Save plot
        save_path = os.path.join(OUT_DIR, f"{label}_distribution_{crit}.png")
        plt.savefig(save_path, dpi=200, bbox_inches='tight')
        plt.close()

def plot_boxplots(df, label):
    for crit in CRITERIA:
        plt.figure(figsize=(8,6))
        sns.boxplot(x="mode", y=crit, data=df)
        plt.title(f"Boxplot of {label} by ({crit})")
        plt.tight_layout()
        plt.savefig(os.path.join(OUT_DIR, f"{label}_boxplot_{crit}.png"), dpi=200)
        plt.close()


def main():
    path = load_latest()
    rows = [json.loads(l) for l in open(path,"r",encoding="utf-8")]
    df = pd.DataFrame(rows)

    real_df = df[df["set"]=="real"]
    stress_df = df[df["set"]=="stress"]

    # REAL
    print_stats(real_df, "real_use")
    plot_distribution(real_df, "real_use")
    plot_boxplots(real_df, "real_use")

    # STRESS
    print_stats(stress_df, "stress_test")
    plot_distribution(stress_df, "stress_test")
    plot_boxplots(stress_df, "stress_test")

    print("\nReports saved to:", OUT_DIR)

if __name__ == "__main__":
    main()
