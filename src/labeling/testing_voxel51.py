"""
Script: Voxel51/Tator prediction QA - area/score threshold search

Purpose:
    - Load a CSV of ground-truth vs. predicted labels (with area and
      score columns) exported from Voxel51.
    - For each class, find the area threshold and then the score
      threshold that best separate correct from incorrect predictions
      (maximizing Youden's J subject to a minimum TPR).
    - Use those thresholds to flag rows that pass the area filter but
      fail the score filter, and save the annotated dataset.
"""

import argparse

import pandas as pd
import numpy as np
from sklearn.metrics import roc_curve, roc_auc_score


def compute_optimal_thresholds(df, classes, score_col, min_tpr=0.7):
    """
    For each class, find the score_col threshold that maximizes Youden's J
    (TPR - FPR) subject to TPR >= min_tpr.

    A row belongs to a class's evaluation set if either its ground-truth
    or predicted label matches that class; "good" means both agree.

    Returns (thresholds, aucs), each a dict keyed by class.
    """
    thresholds = {}
    aucs = {}

    for cls in classes:
        df_class = df[(df['ground_truth.label'] == cls) | (df['predicted_label'] == cls)].copy()
        df_class['is_good'] = (df_class['ground_truth.label'] == df_class['predicted_label']).astype(int)

        scores = df_class[score_col].values
        labels = df_class['is_good'].values

        if len(np.unique(labels)) < 2:
            print(f"Skipping {cls}, not enough good/bad examples")
            continue

        fpr, tpr, roc_thresholds = roc_curve(labels, scores)
        auc = roc_auc_score(labels, scores)

        j_scores = tpr - fpr
        best_idx = np.argmax([j if tpr[i] >= min_tpr else -np.inf for i, j in enumerate(j_scores)])
        best_threshold = roc_thresholds[best_idx]

        thresholds[cls] = best_threshold
        aucs[cls] = auc

        print(f"Class: {cls}")
        print(f"  AUC using {score_col}: {auc:.3f}")
        print(f"  Optimal {score_col} threshold (TPR >= {min_tpr:.0%}): {best_threshold:.3f}")
        print(f"  TPR (good kept): {tpr[best_idx]:.1%}")
        print(f"  FPR (bad kept): {fpr[best_idx]:.1%}")

    return thresholds, aucs


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find per-class area/score thresholds and flag review candidates."
    )
    parser.add_argument(
        "--input-csv",
        default="/Volumes/CFElab/Data_analysis/ISIIS/Voxel51/voxel51_isiis_RachelCarson_2024_02_depth_gt0.csv",
        help="CSV with ground_truth.label, predicted_label, area, score columns.",
    )
    parser.add_argument(
        "--output-csv",
        default="vector_results_all_classes2.csv",
        help="Path to save the annotated dataset with threshold/keep columns.",
    )
    parser.add_argument(
        "--min-tpr",
        type=float,
        default=0.7,
        help="Minimum true-positive rate required when picking the optimal threshold.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    df = pd.read_csv(args.input_csv)

    # Only consider ground-truth classes that also appear as predictions.
    predicted_label_set = set(df['predicted_label'].unique())
    df_filtered = df[df['ground_truth.label'].isin(predicted_label_set)].copy()
    classes = df_filtered['ground_truth.label'].unique()

    # ------------------------- AREA THRESHOLDS -------------------------
    area_thresholds, _ = compute_optimal_thresholds(df_filtered, classes, 'area', min_tpr=args.min_tpr)
    print(area_thresholds)

    df_filtered["area_threshold"] = df_filtered["predicted_label"].map(area_thresholds)
    df_filtered["keep_by_area"] = (
        df_filtered["area_threshold"].notna() &
        (df_filtered["area"] >= df_filtered["area_threshold"])
    )
    df_area_kept = df_filtered[df_filtered["keep_by_area"]]
    print(df_area_kept)

    # ------------------------- SCORE THRESHOLDS -------------------------
    # Computed on the area-filtered set, so the score threshold reflects
    # separation among rows that already passed the area filter.
    score_thresholds, _ = compute_optimal_thresholds(df_area_kept, classes, 'score', min_tpr=args.min_tpr)
    print(score_thresholds)

    df_filtered["score_threshold"] = df_filtered["predicted_label"].map(score_thresholds)
    df_filtered["keep_by_score"] = (
        df_filtered["keep_by_area"] &
        df_filtered["score_threshold"].notna() &
        (df_filtered["score"] >= df_filtered["score_threshold"])
    )

    # Rows that passed the area filter but failed the score filter --
    # candidates for manual review.
    review_candidates = df_filtered[
        (df_filtered["keep_by_score"] == False) & (df_filtered["keep_by_area"] == True)
    ].copy()
    print(review_candidates)

    df_filtered = df_filtered.rename(columns={"ground_truth.label": "ground_truth_label"})
    df_filtered.to_csv(args.output_csv, index=False)


if __name__ == "__main__":
    main()
