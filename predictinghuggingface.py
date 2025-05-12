import pandas as pd
import torch
import os
from tqdm import tqdm
from transformers import AutoModelForImageClassification, AutoImageProcessor
from PIL import Image
import torch.nn as nn
from concurrent.futures import ThreadPoolExecutor, as_completed

# Model configuration
MODEL_NAME = "/home/fernanda/vittrain/cfe_complete_v16-20250303"

print("[TWD] Loading model and image processor...")
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
MODEL = AutoModelForImageClassification.from_pretrained(MODEL_NAME).to(device)
processor = AutoImageProcessor.from_pretrained(MODEL_NAME)
print(f"[TWD] Model loaded successfully on {device}.")

def predict_class(image_path):
    """Predicts the class for a Region of Interest (ROI) in a given image."""

    try:
        # Open the full image
        image = Image.open(image_path).convert("RGB")

        # Preprocess the ROI and move tensors to GPU if available
        inputs = processor(images=image, return_tensors="pt").to(device)
        
        with torch.no_grad():
            outputs = MODEL(**inputs)
            
            # Aplicar softmax para obtener probabilidades y asegurarse de no rastrear gradientes
            probabilities = torch.nn.functional.softmax(outputs.logits, dim=1).squeeze().detach().cpu().numpy()
            
            # Obtener la clase con mayor score
            predicted_class_idx = probabilities.argmax()
            predicted_class = MODEL.config.id2label[predicted_class_idx]

            # Guardar todas las clases y scores en un diccionario
            scores_dict = {MODEL.config.id2label[i]: probabilities[i] for i in range(len(probabilities))}
            scores_dict["class"] = predicted_class  # Agregar la clase predicha al diccionario

        return scores_dict

    except Exception as e:
        print(f"[ERROR] Error processing image {image_path}: {e}")
        return "Error"

def process_row(row):
    """Processes a single row from the CSV file."""
    return predict_class(
        row['crop_path']
    )

def process_csv(file_path):
    """Processes a CSV file in parallel and updates it in place."""
    try:
        data = pd.read_csv(file_path)
        if "Unknown" in data['class'].values:

            # Procesar filas en paralelo
            with ThreadPoolExecutor() as executor:
                results = list(tqdm(executor.map(process_row, [row for _, row in data.iterrows()]), 
                                    total=len(data), desc=f"Processing {os.path.basename(file_path)}", unit="row"))

            # Convertir los resultados en un DataFrame
            results_df = pd.DataFrame(results)

            # Actualizar las columnas del CSV con las predicciones
            data['class'] = results_df['class']  # Clase predicha
            for class_name in results_df.columns:
                if class_name != 'class':  # Evitar sobrescribir la columna principal
                    data[class_name] = results_df[class_name]  # Agregar scores de cada clase

            # Guardar el archivo sobrescribiéndolo
            data.to_csv(file_path, index=False)
    
    except Exception as e:
        print(f"[ERROR] Error processing file {file_path}: {e}")

# Directory with CSV files
csv_dir = "/home/fernanda/RachelCarson_detections_224/det_filtered/csv-vits16"  # Change to your actual directory

print(f"[TWD] Processing CSV files in {csv_dir}...")

# Process all CSV files using multiprocessing
csv_files = [f for f in os.listdir(csv_dir) if f.endswith(".csv")]

with ThreadPoolExecutor() as executor:
    future_to_file = {executor.submit(process_csv, os.path.join(csv_dir, csv_file)): csv_file for csv_file in csv_files}
    
    for future in tqdm(as_completed(future_to_file), total=len(csv_files), desc="Overall Progress", unit="file"):
        csv_file = future_to_file[future]
        try:
            future.result()  # Ensures we catch any errors in processing
        except Exception as e:
            print(f"[ERROR] Error processing {csv_file}: {e}")

print(f"[TWD] All CSV files have been processed and updated in {csv_dir}.")
