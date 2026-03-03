import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.metrics import confusion_matrix

# Leer CSV
df = pd.read_csv("/Volumes/CFElab/Data_analysis/ISIIS/Voxel51/voxel51_isiis_RachelCarson_2024_02_depth_gt0.csv")

print(df.columns)

# Filtrar filas válidas
df = df.dropna(subset=["ground_truth.label", "predicted_label"])

results = []

# Clases únicas que aparecen en predicted_label
classes = df["predicted_label"].unique()

for cls in classes:
    # Binario: cls vs resto
    y_true = (df["ground_truth.label"] == cls).astype(int)
    y_pred = (df["predicted_label"] == cls).astype(int)

    results.append({
        "class": cls,
        "accuracy": accuracy_score(y_true, y_pred),
        "precision": precision_score(y_true, y_pred, zero_division=0),
        "recall": recall_score(y_true, y_pred, zero_division=0),
        "f1": f1_score(y_true, y_pred, zero_division=0),
        "support": int(y_true.sum())  # número de GT positivos
    })

# DataFrame con resultados
metrics_df = pd.DataFrame(results).sort_values("f1", ascending=False)

print(metrics_df)



labels = df["predicted_label"].unique()
rows = []

for cls in labels:
    y_true = (df["ground_truth.label"] == cls).astype(int)
    y_pred = (df["predicted_label"] == cls).astype(int)

    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

    rows.append({
        "class": cls,
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "TN": tn,
        "precision": tp / (tp + fp) if (tp + fp) > 0 else 0,
        "recall": tp / (tp + fn) if (tp + fn) > 0 else 0,
        "f1": 2*tp / (2*tp + fp + fn) if (2*tp + fp + fn) > 0 else 0,
        "support": tp + fn
    })

per_class_df = pd.DataFrame(rows).sort_values("f1", ascending=False)
print(per_class_df)


from sklearn.metrics import confusion_matrix
import pandas as pd

labels = sorted(
    set(df["ground_truth.label"]) | set(df["predicted_label"])
)

cm = confusion_matrix(
    df["ground_truth.label"],
    df["predicted_label"],
    labels=labels
)

cm_df = pd.DataFrame(cm, index=labels, columns=labels)
cm_norm = cm_df.div(cm_df.sum(axis=1), axis=0)


import matplotlib.pyplot as plt
import numpy as np

plt.figure(figsize=(10, 8))
im = plt.imshow(cm_norm, interpolation="nearest", cmap="magma")

plt.colorbar(im, fraction=0.046, pad=0.04)

plt.xticks(np.arange(len(labels)), labels, rotation=45, ha="right")
plt.yticks(np.arange(len(labels)), labels)

plt.xlabel("Predicted label")
plt.ylabel("Ground truth label")
plt.title("Normalized Confusion Matrix (Recall per class)")

# Mostrar valores solo si son relevantes
for i in range(cm_norm.shape[0]):
    for j in range(cm_norm.shape[1]):
        val = cm_norm.iloc[i, j]
        if val > 0.05:  # evita ruido visual
            plt.text(
                j, i, f"{val:.2f}",
                ha="center", va="center",
                color="white" if val > 0.5 else "black",
                fontsize=8
            )

plt.tight_layout()
plt.show()

from sklearn.metrics import balanced_accuracy_score


valid_classes = df["predicted_label"].unique()

df_eval = df[df["ground_truth.label"].isin(valid_classes)]

bal_acc = balanced_accuracy_score(
    df_eval["ground_truth.label"],
    df_eval["predicted_label"]
)

print("Balanced accuracy:", bal_acc)

df["error"] = (
    df["ground_truth.label"] != df["predicted_label"]
).astype(int)

df["depth_bin"] = pd.cut(df["depth"], bins=20)

err_by_depth = (
    df.groupby("depth_bin", observed =False)["error"]
      .mean()
      .reset_index()
)

import matplotlib.pyplot as plt

plt.figure(figsize=(6,4))
plt.plot(err_by_depth["depth_bin"].astype(str),
         err_by_depth["error"])
plt.xticks(rotation=45)
plt.ylabel("Error rate")
plt.xlabel("Depth bin")
plt.title("Prediction error vs depth")
plt.tight_layout()
plt.show()


confusions = (
    df[df["ground_truth.label"] != df["predicted_label"]]
    .groupby(["ground_truth.label", "predicted_label"])
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
)

confusions.head(10)

