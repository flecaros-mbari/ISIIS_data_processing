import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix, accuracy_score, f1_score, precision_score

if __name__ == '__main__':

    # Load the CSV file
    df = pd.read_csv("/Users/fernandalecaros/Downloads/everything-best-STT24-1_ROIs.csv")

    # Extract the true and predicted labels
    df['true_label'] = df['filepath'].apply(lambda x: x.split('/')[-2])  # Obtener el nombre de la carpeta del segundo nivel
    true_labels = df['true_label']
    print(true_labels)
    predicted_labels = df['prediction']


    # Confusion Matrix and Metrics
    cm = confusion_matrix(true_labels, predicted_labels, labels=list(set(true_labels)))
    accuracy = accuracy_score(true_labels, predicted_labels)
    f1 = f1_score(true_labels, predicted_labels, average='weighted')
    precision = precision_score(true_labels, predicted_labels, average='weighted')

    # Print metrics
    print(f'Accuracy: {accuracy}')
    print(f'F1 Score: {f1}')
    print(f'Precision: {precision}')

    # Plot the confusion matrix
    plt.figure(figsize=(10, 7))
    sns.heatmap(cm, annot=True, fmt='g', cmap='Blues', xticklabels=set(true_labels), yticklabels=set(true_labels))
    plt.xlabel('Predicted Label')
    plt.ylabel('True Label')
    plt.title('Confusion Matrix')
    plt.tight_layout()

    plt.savefig("confusion_matrix.png")
    plt.close()

    # Frequency plot for true vs predicted labels
    true_label_counts = true_labels.value_counts()
    predicted_label_counts = predicted_labels.value_counts()
    freq_df = pd.DataFrame({
        'True Labels': true_label_counts,
        'Predicted Labels': predicted_label_counts
    }).fillna(0)

    # Plot frequency of true vs predicted labels
    ax = freq_df.plot(kind='bar', figsize=(10, 7), color=['blue', 'orange'])
    plt.xlabel('Labels')
    plt.ylabel('Frequency')
    plt.title('Frequency of True vs Predicted Labels')
    plt.tight_layout()

    plt.savefig("label_frequencies.png")
    plt.close()




