#!/usr/bin/env python
# run-vss.py
#
# -----------------------------------------------------------------------------
# Vector Similarity Search (VSS) using Vision Transformer (ViT) embeddings
# -----------------------------------------------------------------------------
#
# DESCRIPTION
# This script performs vector similarity search between a set of exemplar images
# and images listed in a CSV file. The workflow is:
#
# 1. Load exemplar images from a directory.
# 2. Compute embeddings for the exemplar images using a Vision Transformer model.
# 3. Read a CSV file containing query image paths.
# 4. Compute embeddings for the query images.
# 5. Normalize all embeddings so cosine similarity can be computed via dot product.
# 6. For each query image, find the top-K most similar exemplar images.
# 7. Save the similarity results (predicted exemplar names and similarity scores)
#    back into a new CSV file.
#
# This approach can be used for:
# - image retrieval
# - similarity search
# - unsupervised classification
# - exemplar-based labeling
#
# The script uses the ViTWrapper class from the sdcat package to compute
# embeddings from images.
#
# -----------------------------------------------------------------------------
#
# Example usage:
#
# python run-vss.py \
#     --exemplar-dir exemplars \
#     --query-csv query_images.csv
#
# python run-vss.py \
#     --exemplar-dir exemplars \
#     --query-csv query_images.csv \
#     --top-k 10
#
# python run-vss.py \
#     --exemplar-dir exemplars \
#     --query-csv query_images.csv \
#     --device cuda:0
#
# -----------------------------------------------------------------------------

import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm

# Add the project root to the Python path so we can import sdcat modules
sys.path.insert(0, str(Path(__file__).parent))

from sdcat.cluster.embedding import ViTWrapper


def load_exemplar_images(exemplar_dir: Path,
                         extensions: tuple = (".png", ".jpg", ".jpeg")) -> list[Path]:
    """
    Load all image files from the exemplar directory.

    The function scans the directory for images with the specified extensions
    (case insensitive) and removes files that appear to be embedding outputs
    (e.g., prediction text files or numpy arrays).

    Parameters
    ----------
    exemplar_dir : Path
        Directory containing exemplar images.

    extensions : tuple
        Valid image file extensions.

    Returns
    -------
    list[Path]
        Sorted list of image file paths.
    """

    images = []

    for ext in extensions:
        images.extend(exemplar_dir.glob(f"*{ext}"))
        images.extend(exemplar_dir.glob(f"*{ext.upper()}"))

    # Remove embedding or prediction files accidentally present
    images = [p for p in images if "_pred.txt" not in p.name and ".npy" not in p.name]

    return sorted(images)


def compute_embeddings(vit: ViTWrapper,
                       image_paths: list[Path],
                       batch_size: int = 32) -> np.ndarray:
    """
    Compute embeddings for a list of images using a Vision Transformer.

    Images are processed in batches to avoid memory overload.

    Parameters
    ----------
    vit : ViTWrapper
        Initialized Vision Transformer embedding model.

    image_paths : list[Path]
        List of image file paths.

    batch_size : int
        Number of images processed simultaneously.

    Returns
    -------
    np.ndarray
        Array of shape (num_images, embedding_dimension).
    """

    all_embeddings = []

    # Process images in batches
    for i in range(0, len(image_paths), batch_size):

        batch_paths = [str(p) for p in image_paths[i:i + batch_size]]

        batch_embeddings, _, _ = vit.process_images(batch_paths)

        all_embeddings.append(batch_embeddings)

    return np.vstack(all_embeddings)


def normalize_embeddings(embeddings: np.ndarray) -> np.ndarray:
    """
    Normalize embeddings using L2 normalization.

    Normalizing vectors allows cosine similarity to be computed using
    a simple dot product.

    Parameters
    ----------
    embeddings : np.ndarray
        Array of embeddings with shape (num_images, embedding_dim).

    Returns
    -------
    np.ndarray
        L2-normalized embeddings.
    """

    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)

    return embeddings / (norms + 1e-8)


def find_similar(query_embedding: np.ndarray,
                 exemplar_embeddings: np.ndarray,
                 top_k: int = 5) -> tuple[np.ndarray, np.ndarray]:
    """
    Find the top-K most similar exemplar embeddings to a query embedding.

    Cosine similarity is computed using the dot product since all vectors
    are normalized beforehand.

    Parameters
    ----------
    query_embedding : np.ndarray
        Normalized query embedding with shape (1, embedding_dim).

    exemplar_embeddings : np.ndarray
        Normalized exemplar embeddings with shape
        (num_exemplars, embedding_dim).

    top_k : int
        Number of most similar images to return.

    Returns
    -------
    tuple[np.ndarray, np.ndarray]

        top_indices :
            Indices of the most similar exemplars.

        top_scores :
            Corresponding cosine similarity scores.
    """

    # Compute cosine similarity
    similarities = np.dot(exemplar_embeddings, query_embedding.T).flatten()

    # Sort by highest similarity
    top_indices = np.argsort(similarities)[::-1][:top_k]

    top_scores = similarities[top_indices]

    return top_indices, top_scores


def main():
    """
    Main execution function.

    Steps:
    1. Parse command line arguments.
    2. Load exemplar images.
    3. Initialize the Vision Transformer model.
    4. Compute embeddings for exemplar images.
    5. Load query images from the CSV file.
    6. Compute query embeddings.
    7. Perform similarity search.
    8. Store predictions and similarity scores in a new CSV file.
    """

    parser = argparse.ArgumentParser(
        description="Vector Similarity Search (VSS) example using ViT embeddings",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--exemplar-dir",
        type=Path,
        required=True,
        help="Directory containing exemplar images"
    )

    parser.add_argument(
        "--query-csv",
        type=Path,
        required=True,
        help="CSV file containing the query images to analyze"
    )

    parser.add_argument(
        "--model",
        type=str,
        default="/mnt/DeepSea-AI/models/CFE/dinov7",
        help="Path or name of the ViT model"
    )

    parser.add_argument(
        "--device",
        type=str,
        default="cpu",
        help="Device used for inference (cpu, cuda:0, cuda:1, etc.)"
    )

    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of similar images returned for each query"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Batch size for embedding computation"
    )

    args = parser.parse_args()

    # ---------------------------------------------------------
    # Validate inputs
    # ---------------------------------------------------------

    if not args.exemplar_dir.exists():
        print(f"Error: Exemplar directory not found: {args.exemplar_dir}")
        sys.exit(1)

    print(f"Loading exemplar images from: {args.exemplar_dir}")

    exemplar_paths = load_exemplar_images(args.exemplar_dir)

    df = pd.read_csv(args.query_csv)

    if len(exemplar_paths) == 0:
        print("Error: No images found in exemplar directory")
        sys.exit(1)

    print(f"Found {len(exemplar_paths)} exemplar images")

    # ---------------------------------------------------------
    # Initialize the Vision Transformer embedding model
    # ---------------------------------------------------------

    print(f"Initializing ViT model: {args.model} on {args.device}")

    vit = ViTWrapper(
        device=args.device,
        model_name=args.model,
        batch_size=args.batch_size
    )

    print(f"Embedding dimension: {vit.vector_dimensions}")

    # ---------------------------------------------------------
    # Compute embeddings for exemplar images
    # ---------------------------------------------------------

    print(f"Computing embeddings for {len(exemplar_paths)} exemplar images...")

    exemplar_embeddings = compute_embeddings(
        vit,
        exemplar_paths,
        args.batch_size
    )

    print(f"Exemplar embeddings shape: {exemplar_embeddings.shape}")

    # Normalize exemplar embeddings
    exemplar_embeddings_norm = normalize_embeddings(exemplar_embeddings)

    # ---------------------------------------------------------
    # Compute embeddings for query images from CSV
    # ---------------------------------------------------------

    print(f"Computing embeddings for query csv: {args.query_csv}")

    query_embedding, query_labels, query_scores, paths = \
        vit.process_images_from_csv(str(args.query_csv))

    query_embedding_norm = normalize_embeddings(query_embedding)

    # ---------------------------------------------------------
    # Prepare output columns
    # ---------------------------------------------------------

    for k in range(1, args.top_k + 1):
        df[f"vector_prediction_{k}"] = None
        df[f"vector_score_{k}"] = None

    # ---------------------------------------------------------
    # Perform similarity search for each query image
    # ---------------------------------------------------------

    for qi, q_emb in enumerate(query_embedding_norm):

        top_indices, top_scores = find_similar(
            q_emb[None, :],
            exemplar_embeddings_norm,
            args.top_k
        )

        for rank, (idx, score) in enumerate(zip(top_indices, top_scores), 1):

            pred_name = exemplar_paths[idx].stem

            df.loc[qi, f"vector_prediction_{rank}"] = pred_name
            df.loc[qi, f"vector_score_{rank}"] = float(score)

    # ---------------------------------------------------------
    # Save results
    # ---------------------------------------------------------

    out_csv = args.query_csv.with_name(
        args.query_csv.stem + "_with_vector_predictions.csv"
    )

    df.to_csv(out_csv, index=False)

    print(f"\nResults saved to: {out_csv}")


if __name__ == "__main__":
    main()