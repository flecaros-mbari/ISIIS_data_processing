"""
Risk Score Analysis Script

Description
-----------
This script analyzes prediction results from a classification model and a
vector similarity search system (cosine similarity). It computes a combined
"anomaly score" for each prediction using a weighted combination of:

1. The model prediction confidence score.
2. The cosine similarity distance from a vector search system.

The idea is to combine both signals to estimate how "anomalous" or uncertain
a prediction might be.

The final anomaly score is computed as:

    anomaly_score = alpha * model_score + (1 - alpha) * cosine_similarity

Where:
    model_score = confidence score from the classification model
    cosine_similarity = (1 - cosine_distance)

The script then visualizes the distribution of anomaly scores using a
histogram, and optionally writes the scored rows back out to a CSV.

Inputs
------
A CSV produced by joining predict/huggingface.py's classification output
with cosine-distance/cosine_distance.py's vector-search output.

Expected columns in the CSV:
    class               -> label predicted by the classification model
    score                -> confidence score of the model prediction [0,1]
    vector_prediction_1  -> closest label from vector similarity search
    vector_score_1       -> cosine distance to closest vector match

Output
------
A histogram showing the distribution of computed anomaly scores, and
(with --output-csv) the input data with an added anomaly_score column.
"""

import argparse

import pandas as pd
import matplotlib.pyplot as plt


def compute_anomaly_score(row, alpha, verbose=False):
    """
    Compute a combined anomaly score for a single row.

    The anomaly score is calculated as a weighted combination of the model
    prediction score and the cosine similarity score.

    Parameters
    ----------
    row : pandas.Series
        A row from the dataset containing prediction and similarity data.
    alpha : float
        Weight controlling the contribution of the model confidence score.
        alpha close to 1 -> trust the model more.
        alpha close to 0 -> trust the cosine similarity more.
    verbose : bool
        If True, print intermediate values for each row.

    Returns
    -------
    float
        Computed anomaly score.
    """

    model_score = row["score"]
    cosine_dist = row["vector_score_1"]

    # Convert cosine distance to cosine similarity
    cosine_similarity = 1 - cosine_dist

    # Compute weighted anomaly score
    anomaly_score = alpha * model_score + (1 - alpha) * cosine_similarity

    if verbose:
        print(
            f"For this sample -> model score: {model_score}, "
            f"cosine similarity: {cosine_similarity}, "
            f"combined anomaly: {anomaly_score}"
        )

    return anomaly_score


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute a combined model-confidence / vector-similarity anomaly score."
    )
    parser.add_argument(
        "--input-csv",
        default="final_all_dataset.csv",
        help="CSV with 'class', 'score', 'vector_prediction_1', 'vector_score_1' columns.",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.3,
        help="Weight for the model confidence score vs. cosine similarity (default: 0.3).",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="If set, write the input data with an added anomaly_score column to this path.",
    )
    parser.add_argument(
        "--no-show",
        action="store_true",
        help="Skip displaying the histogram (useful for headless/batch runs).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-row intermediate score values.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    data = pd.read_csv(args.input_csv)

    data["anomaly_score"] = data.apply(
        lambda row: compute_anomaly_score(row, args.alpha, args.verbose), axis=1
    )

    if args.output_csv:
        data.to_csv(args.output_csv, index=False)
        print(f"Saved scored data to {args.output_csv}")

    if not args.no_show:
        plt.hist(
            data["anomaly_score"],
            bins=100,
            label="scores",
            alpha=0.7,
        )
        plt.xlabel("Anomaly")
        plt.ylabel("Count")
        plt.title(f"Anomaly values vs frequency (alpha = {args.alpha})")
        plt.legend()
        plt.show()


if __name__ == "__main__":
    main()
