import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv("final_all_dataset.csv")

# filtered_data_good = data[data["prediction_3"] == data["ground_truth_label"]]
# filtered_data_bad = data[data["prediction_1"] != data["ground_truth_label"]]
alpha = 0.3


def resolve_label(row):
    model_label = row["predicted_label"]
    model_score = row["score"]

    cosine_label = row["prediction_1"]
    cosine_dist = row["score_1"]

    top3_labels = [
        row["prediction_1"],
        row["prediction_2"],
        row["prediction_3"],
    ]
    d= alpha * model_score + (1-alpha)*(1-cosine_dist)
    print(f"for this one model {model_score} and cosine {1-cosine_dist} with risk {d}")
    return alpha * model_score + (1-alpha)*(1-cosine_dist)


data["risk_score"] = data.apply(resolve_label, axis=1)
# filtered_data_good = data[data["ground_truth_label"]]
# filtered_data_bad = data[data["risk_score"]]

plt.hist(
    data["risk_score"],
    bins=100,
    label=["scores"],
    alpha=0.7
)

plt.xlabel("Risk")
plt.ylabel("Count")
plt.title(f"Risk values vs frecuency with {alpha}")
plt.legend()
plt.show()








# # Somethign about if theres more tha 2 the same we asume that class?

# plt.hist(
#     [filtered_data_good["score_1"], filtered_data_bad["score_1"]],
#     bins=50,
#     label=["Correct", "Incorrect"],
#     alpha=0.7
# )

# plt.xlabel("Prediction score")
# plt.ylabel("Count")
# plt.legend()
# plt.show()

# Data model = cosine



