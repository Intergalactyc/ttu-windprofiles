import os
import pandas as pd
from config import results_dir
import matplotlib.pyplot as plt

processed = os.path.join(results_dir, "processed", "dec18.csv")
fig_out = os.path.join(results_dir, "figures")

def turbulence_distribution(df, b, saveto):
    fig, axs = plt.subplots(figsize=(8,8), nrows=2)
    fig.suptitle(f"Boom {b} turbulence distributions")
    axs[0].set_xlabel("Turbulence intensity")
    axs[0].set_ylabel("Density")
    axs[0].hist(x=df[f"ti_{b}"], bins=60, density=True, alpha=0.4, range=(0,0.9), edgecolor="k")
    axs[1].set_xlabel(r"TKE $(m^2~s^{-2})$")
    axs[1].set_ylabel("Density")
    axs[1].hist(x=df[f"tke_{b}"], bins=60, density=True, alpha=0.4, range=(0,5), edgecolor="k")
    fig.tight_layout()
    plt.savefig(saveto, bbox_inches="tight")
    plt.close()
    return

def turbulence_scatter(df, b):
    plt.scatter(df[f"ti_{b}"], df[f"tke_{b}"], s=1)
    plt.xlabel("TI")
    plt.ylabel("TKE")
    plt.show()

def main():
    df = pd.read_csv(processed)

    for b in [1,2,3,4,5,6,7,9]:
        turbulence_distribution(df, b, os.path.join(fig_out, f"turbulence_{b}.png"))

    turbulence_scatter(df, 1)

if __name__ == "__main__":
    main()
