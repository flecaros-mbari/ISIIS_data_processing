import argparse
import pandas as pd
import torch
import os
from tqdm import tqdm
from transformers import AutoModelForImageClassification, AutoImageProcessor
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed

def parse_args():
    parser = argparse.ArgumentParser(description="Batch classify ROIs in CSV files and post-process results.")
    parser.add_argument(
        "--model",
        required=True,
        help="Path or name of the pretrained model.",
    )
    parser.add_argument(
        "--csv-dir",
        required=True,
        help="Directory containing CSV files to process.",
    )
    return parser.parse_args()

def load_model(model_name):
    print("[INFO] Loading model and image processor...")
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = AutoModelForImageClassification.from_pretrained(model_name).to(device)
    processor = AutoImageProcessor.from_pretrained(model_name)
    print(f"[INFO] Model loaded successfully on {device}.")
    return model, processor, device

def predict_class(image_path, model, processor, device):
    """Predicts the class for a Region of Interest (ROI) in a given image."""
    try:
        image = Image.open(image_path).convert("RGB")
        inputs = processor(images=image, return_tensors="pt").to(device)

        with torch.no_grad():
            outputs = model(**inputs)
            probabilities = torch.nn.functional.softmax(outputs.logits, dim=1).squeeze().cpu().numpy()
            predicted_class_idx = probabilities.argmax()
            predicted_class = model.config.id2label[predicted_class_idx]
            scores_dict = {model.config.id2label[i]: probabilities[i] for i in range(len(probabilities))}
            scores_dict["class"] = predicted_class

        # Free memory
        del inputs, outputs
        torch.cuda.empty_cache()
        torch.cuda.ipc_collect()

        return scores_dict

    except Exception as e:
        print(f"[ERROR] Error processing image {image_path}: {e}")
        return {"class": "Error"}

def process_row(row, model, processor, device):
    return predict_class(row['crop_path'], model, processor, device)

def classify_csv(file_path, model, processor, device):
    """Step 1: Classify Unknown entries in the CSV using the model."""
    try:
        data = pd.read_csv(file_path)
        if True:
        #if "Unknown" in data['class'].values:
            
            data["crop_path"] = data["crop_path"].apply(fix_path)
            results = []
            for _, row in tqdm(
                data.iterrows(),
                total=len(data),
                desc=f"Classifying {os.path.basename(file_path)}",
                unit="row"
            ):
                result = process_row(row, model, processor, device)
                results.append(result)

            results_df = pd.DataFrame(results)

            # Update the main DataFrame
            data['class'] = results_df['class']
            for class_name in results_df.columns:
                if class_name != 'class':
                    data[class_name] = results_df[class_name]

            data.to_csv(file_path, index=False)

        return file_path

    except Exception as e:
        print(f"[ERROR] Error processing file {file_path}: {e}")
        return None

def fix_path(path):
    # Normaliza por seguridad
    path = os.path.normpath(path)

    # Dividir la ruta en componentes
    parts = path.split(os.sep)

    # Si la ruta original comienza con slash
    if parts[0] == "":
        parts = parts[1:]

    # Reemplazar la primera carpeta por "mnt"
    parts[0] = "Volumes"

    # Reconstruir ruta con /
    new_path = "/" + "/".join(parts)
    return new_path

def postprocess_csv(file_path):
    """Step 2: Post-process the classified CSV to add score, class_s, score_s."""
    try:
        df = pd.read_csv(file_path)

        # Classes are assumed to be after crop_path column
        if "crop_path" not in df.columns:
            print(f"[WARNING] Skipping {file_path}: missing crop_path column")
            return
        

        crop_idx = df.columns.get_loc("crop_path")
        class_columns = df.columns[crop_idx+1:]

        if len(class_columns) < 2:
            print(f"[WARNING] Skipping {file_path}: not enough class columns found")
            return

        # Probability of the predicted class
        df["score"] = df.apply(lambda row: row.get(row["class"], None), axis=1)

        # Second best class and score
        def get_second_best(row):
            sorted_scores = row[class_columns].sort_values(ascending=False)
            class_s = sorted_scores.index[1]
            score_s = sorted_scores.iloc[1]
            return pd.Series([class_s, score_s])

        df[["class_s", "score_s"]] = df.apply(get_second_best, axis=1)

        # Save
        df.to_csv(file_path, index=False)
        print(f"[INFO] Post-processed {file_path}")

    except Exception as e:
        print(f"[ERROR] Post-processing failed for {file_path}: {e}")

def main():
    args = parse_args()
    model_name = args.model
    csv_dir = args.csv_dir

    model, processor, device = load_model(model_name)

    print(f"[INFO] Processing CSV files in {csv_dir}...")
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith(".csv")]

    # Step 1: Classify
    classified_files = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_file = {
            executor.submit(classify_csv, os.path.join(csv_dir, csv_file), model, processor, device): csv_file
            for csv_file in csv_files
        }
        for future in tqdm(
            as_completed(future_to_file),
            total=len(csv_files),
            desc="Classification Progress",
            unit="file"
        ):
            file_path = future.result()
            if file_path:
                classified_files.append(file_path)

    # Step2: Post-processing
    print("[INFO] Starting post-processing...")
    for file_path in classified_files:
        postprocess_csv(file_path)

    print("[INFO] All CSV files have been processed successfully.")

if __name__ == "__main__":
    main()
