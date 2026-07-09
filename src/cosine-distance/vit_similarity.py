#!/usr/bin/env python
# vit_similarity.py
#
# Shared Vision Transformer (ViT) embedding and cosine-similarity helpers
# used by both run-vss.py (single-image CLI) and cosine_distance.py
# (batch CSV-driven CLI).

from pathlib import Path

import numpy as np


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


def compute_embeddings(vit, image_paths: list[Path], batch_size: int = 32) -> np.ndarray:
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
