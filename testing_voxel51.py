import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import precision_recall_curve
from pathlib import Path
import shutil

df = pd.read_csv("/Volumes/CFElab/Data_analysis/ISIIS/Voxel51/voxel51_isiis_RachelCarson_2024_02_depth_gt0.csv")
# df = pd.read_csv("/Users/fernandalecaros/Downloads/outputt/labels.csv")

# print(df.groupby("ground_truth.label").size())

# # Obtener labels únicos
# labels = df["ground_truth.label"].dropna().unique()
labels = df["predicted_label"].dropna().unique()
# thr = 1

# df_no_cumplen = df[
#     (
#         (df["ground_truth.label"] == "copepod") 
        
#     ) |
#     (df["predicted_label"]  == "copepod")
# ]


# print(df_no_cumplen)
# output_csv = "to_vector_space_all_copepod.csv"
# df_no_cumplen.to_csv(output_csv, index=False)


# for lbl in labels:

#     mask = df["predicted_label"] == lbl
#     y_true = (df.loc[mask, "ground_truth.label"] == lbl).astype(int)
#     y_score = df.loc[mask, "score"]

#     if y_true.sum() < 10:
#         continue

    
#     plt.figure()
#     plt.plot(recall, precision)
#     plt.xlabel("Recall")
#     plt.ylabel("Precision")
#     plt.title(f"Precision–Recall curve — {lbl}")
#     plt.grid(True)
#     plt.tight_layout()
#     plt.show()

# thresholds = {}

# for lbl in labels:
#     #mask = df["predicted_label"] == lbl
#     y_true = (df["ground_truth.label"] == lbl).astype(int)
#     y_score = df["score"]

#     if y_true.sum() < 10:
#         continue

#     precision, recall, thr = precision_recall_curve(y_true, y_score)

#     f1 = 2 * precision[:-1] * recall[:-1] / (precision[:-1] + recall[:-1])
#     best_thr = thr[np.argmax(f1)]

#     thresholds[lbl] = best_thr

# print(thresholds)

# thresholds = {}
# for lbl in labels:
#     mask = df["predicted_label"] == lbl
#     y_true = (df.loc[mask, "ground_truth.label"] == lbl).astype(int)
#     y_score = df.loc[mask, "score"]
#     min_precision = 0.95  # o 0.9

#     if len(y_true) == 0:
#         continue

#     if y_true.sum() == 0:
#         continue

#     precision, recall, thr = precision_recall_curve(y_true, y_score)

#     valid = precision[:-1] >= min_precision

#     if valid.any():
#         best_thr = thr[valid][0]  # primer threshold que cumple
#     else:
#         best_thr = thr[np.argmax(precision[:-1])]  # fallback

#     thresholds[lbl] = best_thr

# print(thresholds)


################################################################
# out_dir = Path("/Users/fernandalecaros/Documents/ISIIS/results/")
# out_dir.mkdir(parents=True, exist_ok=True)

# BASE_PATH = Path("/Volumes/CFElab/Data_analysis/ISIIS/Voxel51/mnt/ML_SCRATCH/isiis/cfe_isiis_dino_v7-20250916/crops/")
# IMAGE_COL = "uuid"   # nombre del archivo relativo
# AREA_THRESHOLD = 400
# MAX_EXAMPLES = 5

# examples_dir = out_dir / "low_area_examples"
# examples_dir.mkdir(exist_ok=True)

# for lbl in labels:
#     sub = df[df["ground_truth.label"] == lbl]

#     small = sub[sub["area"] < AREA_THRESHOLD].head(MAX_EXAMPLES)

#     if small.empty:
#         continue

#     lbl_dir = examples_dir / lbl
#     lbl_dir.mkdir(exist_ok=True)

#     for _, row in small.iterrows():
#         uuid = Path(row[IMAGE_COL])   # por si viene con subdirs
#         area = row["area"]

#         # 🔑 construir path completo
#         src = BASE_PATH / lbl / f"{uuid}.jpg"

#         if not src.exists():
#             matches = list(BASE_PATH.rglob(f"{uuid}.jpg"))
#             if len(matches) == 0:
#                 print(f"❌ Not found anywhere: {uuid}")
#                 continue
#             elif len(matches) > 1:
#                 print(f"⚠️ Multiple matches for {uuid}, using first")
#             src = matches[0]
#         else:
#             print(f"FOUND")

#         dst = lbl_dir / f"{lbl}_area_{int(area)}{src.suffix}"
#         # shutil.copy(src, dst)
###################################################################

# for lbl in labels:
#     sub = df[
#         (df["ground_truth.label"] == lbl) &
#         (df["predicted_label"] == lbl)
#     ]

#     # Saltar si no hay datos correctos
#     if sub.empty:
#         continue

#     plt.figure()
#     plt.hist(sub["score"], bins=50)
#     plt.xlabel("Score")
#     plt.ylabel("Frequency")
#     plt.xlim(0, 1)
#     plt.title(f"Correct predictions — Score — {lbl}")
#     plt.tight_layout()
#     plt.show()


# for lbl in labels:

#     correct = df[
#         (df["predicted_label"] == lbl) &
#         (df["ground_truth.label"] == lbl)
#     ]

#     incorrect = df[
#         (df["predicted_label"] == lbl) &
#         (df["ground_truth.label"] != lbl)
#     ]

#     # Saltar si no hay datos suficientes
#     if correct.empty and incorrect.empty:
#         continue

#     plt.figure(figsize=(6, 4))

#     bins = 30

#     if not correct.empty:
#         plt.hist(
#             correct["area"],
#             bins=bins,
#             alpha=0.6,
#             label="Correct",
#             density=False
#         )

#     if not incorrect.empty:
#         plt.hist(
#             incorrect["area"],
#             bins=bins,
#             alpha=0.6,
#             label="Incorrect",
#             density=False
#         )

#     plt.xlabel("Area")
#     plt.ylabel("Frequency")
#     plt.title(f"Predicted = {lbl} — Area distribution")
#     plt.legend()
#     plt.tight_layout()
#     plt.show()

# for lbl in labels:

#     correct = df[
#         (df["predicted_label"] == lbl) &
#         (df["ground_truth.label"] == lbl)
#     ]

#     incorrect = df[
#         (df["predicted_label"] == lbl) &
#         (df["ground_truth.label"] != lbl)
#     ]

#     # Saltar si no hay datos suficientes
#     if correct.empty and incorrect.empty:
#         continue

#     plt.figure(figsize=(6, 4))

#     bins = 30

#     if not correct.empty:
#         plt.hist(
#             correct["score"],
#             bins=bins,
#             alpha=0.6,
#             label="Correct",
#             density=False
#         )

#     if not incorrect.empty:
#         plt.hist(
#             incorrect["score"],
#             bins=bins,
#             alpha=0.6,
#             label="Incorrect",
#             density=False
#         )

#     plt.xlabel("Area")
#     plt.ylabel("Frequency")
#     plt.title(f"Predicted = {lbl} — Area distribution")
#     plt.legend()
#     plt.tight_layout()
#     plt.show()

import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, roc_auc_score
import numpy as np

# Sólo considerar ground-truth que existen en predicted_label
predicted_label_set = set(df['predicted_label'].unique())
df_filtered = df[df['ground_truth.label'].isin(predicted_label_set)].copy()

# Lista de clases a evaluar
classes = df_filtered['ground_truth.label'].unique()

# Diccionarios para guardar resultados
optimal_thresholds = {}
aucs = {}

for cls in classes:
    # Filtrar por clase
    df_class = df_filtered[(df_filtered['ground_truth.label'] == cls) | (df_filtered['predicted_label'] == cls)].copy()

    # 1 = good prediction, 0 = incorrect
    df_class['is_good'] = (df_class['ground_truth.label'] == df_class['predicted_label']).astype(int)
    
    scores = df_class['area'].values
    labels = df_class['is_good'].values
    
    if len(np.unique(labels)) < 2:
        print(f"Skipping {cls}, not enough good/bad examples")
        continue
    
    # ROC
    fpr, tpr, thresholds = roc_curve(labels, scores)
    auc = roc_auc_score(labels, scores)
    
    # Youden J con TPR >= 50%
    J_scores = tpr - fpr
    best_idx = np.argmax([j if tpr[i] >= 0.7 else -np.inf for i, j in enumerate(J_scores)])
    best_threshold = thresholds[best_idx]
    
    # Guardar resultados
    optimal_thresholds[cls] = best_threshold
    aucs[cls] = auc
    
    print(f"Class: {cls}")
    print(f"  AUC using area: {auc:.3f}")
    print(f"  Optimal area threshold (TPR >= 70%): {best_threshold:.3f}")
    print(f"  TPR (good kept): {tpr[best_idx]:.1%}")
    print(f"  FPR (bad kept): {fpr[best_idx]:.1%}")
    
    # Histograma por clase
    # plt.figure(figsize=(6,4))
    # plt.hist(df_class[df_class['is_good']==1]['area'], bins=30, alpha=0.5, label='Good')
    # plt.hist(df_class[df_class['is_good']==0]['area'], bins=30, alpha=0.5, label='Bad')
    # plt.axvline(best_threshold, color='k', linestyle='--', label='Optimal Threshold')
    # plt.title(f"Class: {cls}")
    # plt.xlabel('Area')
    # plt.ylabel('Frequency')
    # plt.legend()
    # plt.savefig(f"plot_area_{cls}.png", dpi=300, bbox_inches='tight')
    # plt.show()
print(optimal_thresholds)

print(df_filtered)
print(df_class)
df_filtered["area_threshold"] = df_filtered["predicted_label"].map(optimal_thresholds)


df_filtered["keep_by_area"] = (
    df_filtered["area_threshold"].notna() &
    (df_filtered["area"] >= df_filtered["area_threshold"])
)

df_final = df_filtered[df_filtered["keep_by_area"]]
print(df_final)


# Diccionarios para guardar resultados
optimal_thresholds = {}
aucs = {}

for cls in classes:
    # Filtrar por clase
    df_class = df_final[(df_final['ground_truth.label'] == cls) | (df_final['predicted_label'] == cls)].copy()
    
    # 1 = good prediction, 0 = incorrect
    df_class['is_good'] = (df_class['ground_truth.label'] == df_class['predicted_label']).astype(int)
    
    scores = df_class['score'].values
    labels = df_class['is_good'].values
    
    if len(np.unique(labels)) < 2:
        print(f"Skipping {cls}, not enough good/bad examples")
        continue
    
    # ROC
    fpr, tpr, thresholds = roc_curve(labels, scores)
    auc = roc_auc_score(labels, scores)
    
    # Youden J con TPR >= 50%
    J_scores = tpr - fpr
    best_idx = np.argmax([j if tpr[i] >= 0.7 else -np.inf for i, j in enumerate(J_scores)])
    best_threshold = thresholds[best_idx]
    
    # Guardar resultados
    optimal_thresholds[cls] = best_threshold
    aucs[cls] = auc
    
    print(f"Class: {cls}")
    print(f"  AUC using score: {auc:.3f}")
    print(f"  Optimal score threshold (TPR >= 70%): {best_threshold:.3f}")
    print(f"  TPR (good kept): {tpr[best_idx]:.1%}")
    print(f"  FPR (bad kept): {fpr[best_idx]:.1%}")
    
    # Histograma por clase
    # plt.figure(figsize=(6,4))
    # plt.hist(df_class[df_class['is_good']==1]['score'], bins=30, alpha=0.5, label='Good')
    # plt.hist(df_class[df_class['is_good']==0]['score'], bins=30, alpha=0.5, label='Bad')
    # plt.axvline(best_threshold, color='k', linestyle='--', label='Optimal Threshold')
    # plt.title(f"Class: {cls}")
    # plt.xlabel('Score')
    # plt.ylabel('Frequency')
    # plt.legend()
    # plt.savefig(f"plot_score_{cls}.png", dpi=300, bbox_inches='tight')
    # plt.show()
print(optimal_thresholds)

df_filtered["score_threshold"] = df_filtered["predicted_label"].map(optimal_thresholds)


df_filtered["keep_by_score"] = (
    df_filtered["keep_by_area"] &
    df_filtered["score_threshold"].notna() &
    (df_filtered["score"] >= df_filtered["score_threshold"])
)
df_final = df_filtered[(df_filtered["keep_by_score"]== False)&(df_filtered["keep_by_area"]== True)].copy()
print(df_final)

# df_finall = df_filtered[(df_filtered["keep_by_score"]== True)&(df_filtered["keep_by_area"]== True)].copy()
# print(df_finall)
# Cambiar nombre de la columna
df_filtered = df_filtered.rename(columns={"ground_truth.label": "ground_truth_label"})


df_filtered.to_csv('vector_results_all_classes2.csv', index=False)

