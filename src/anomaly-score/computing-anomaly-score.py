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

The final anomalous score is computed as:

    anomaly socre = alpha * model_score + (1 - alpha) * cosine_similarity

Where:
    model_score = confidence score from the classification model
    cosine_similarity = (1 - cosine_distance)

The script then visualizes the distribution of anomlay scores using a histogram.

Inputs
------
A csv 

Expected columns in the CSV:
    predicted_label   -> label predicted by the model
    score             -> confidence score of the model prediction [0,1]
    prediction_1      -> closest label from vector similarity search
    prediction_2      -> second closest label
    prediction_3      -> third closest label
    score_1           -> cosine distance to closest vector match
    ground_truth_label (optional, used for evaluation)

Output
------
A histogram showing the distribution of computed risk scores.
"""

import pandas as pd
import matplotlib.pyplot as plt

# -------------------------------------------------------------------------
# Load dataset
# -------------------------------------------------------------------------

# Read the CSV file containing predictions and similarity scores
data = pd.read_csv("final_all_dataset.csv")

# -------------------------------------------------------------------------
# Parameters
# -------------------------------------------------------------------------

# Weight controlling the contribution of the model confidence score.
# alpha close to 1 → trust the model more
# alpha close to 0 → trust the cosine similarity more
# Here you can try different alpha's value if necesary 
alpha = 0.3


# -------------------------------------------------------------------------
# Anomlaly score computation
# -------------------------------------------------------------------------

def resolve_label(row):
    """
    Compute a combined anomaly score for a single row.

    The anomaly score is calculated as a weighted combination of the model
    prediction score and the cosine similarity score.

    Parameters
    ----------
    row : pandas.Series
        A row from the dataset containing prediction and similarity data.

    Returns
    -------
    float
        Computed anomaly score.
    """

    # Model prediction information
    model_label = row["predicted_label"]
    model_score = row["score"]

    # Vector similarity search results
    cosine_label = row["prediction_1"]
    cosine_dist = row["score_1"]

    # Top-3 cosine similarity predictions (not currently used for computation,
    # but may be useful for debugging or future extensions)
    top3_labels = [
        row["prediction_1"],
        row["prediction_2"],
        row["prediction_3"],
    ]

    # Convert cosine distance to cosine similarity
    cosine_similarity = 1 - cosine_dist

    # Compute weighted risk score
    risk = alpha * model_score + (1 - alpha) * cosine_similarity

    # Debug print showing intermediate values
    print(
        f"For this sample -> model score: {model_score}, "
        f"cosine similarity: {cosine_similarity}, "
        f"combined anomaly: {risk}"
    )

    return risk


# -------------------------------------------------------------------------
# Apply risk score calculation to the entire dataset
# -------------------------------------------------------------------------

# Compute risk score for every row
data["anomaly_score"] = data.apply(resolve_label, axis=1)


# -------------------------------------------------------------------------
# Visualization
# -------------------------------------------------------------------------

# Plot histogram of risk scores
plt.hist(
    data["anomaly_score"],
    bins=100,      # number of bins in the histogram
    label=["scores"],
    alpha=0.7
)

# Label axes
plt.xlabel("Anomaly")
plt.ylabel("Count")

# Plot title showing alpha value used
plt.title(f"Anomaly values vs frequency (alpha = {alpha})")

# Display legend
plt.legend()

# Show the plot
plt.show()