import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df = pd.read_csv("/Volumes/CFElab/Data_analysis/ISIIS/Baseline_v3/localizations.csv")

df_filtrado = df[df["label"].str.lower() != "unknown"]

df_filtrado.to_csv("/Users/fernandalecaros/Documents/ISIIS_data_processing/output_filtrado.csv", index=False)

df = pd.read_csv("/Users/fernandalecaros/Documents/ISIIS_data_processing/output_filtrado.csv")

# Asegurar que area es numérica
df["area"] = pd.to_numeric(df["area"], errors="coerce")

# ESD (mismas unidades que area → pixeles si el área está en pix^2)
df["esd"] = 2 * np.sqrt(df["area"] / np.pi)

# Eliminar valores inválidos
df = df.dropna(subset=["esd", "label"])
PIXEL_SIZE_UM = 10

for label, g in df.groupby("label"):
    plt.figure()
    plt.hist(g["esd"] * PIXEL_SIZE_UM, bins=30)
    plt.yscale("log")
    plt.xscale("log")
    plt.xlabel("Equivalent Spherical Diameter (ESD)")
    plt.ylabel("Frecuencia")
    plt.title(f"ESD distribution – {label} with {len(g)} labels")
    plt.tight_layout()
    plt.show()
