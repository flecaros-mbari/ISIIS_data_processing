#!/usr/bin/env python
# run-vss.py
# Simple Vector Similarity Search (VSS) example using ViT embeddings
#
# Usage:
#   python run-vss.py --exemplar-dir exemplars --query-image path/to/query.png
#   python run-vss.py --exemplar-dir exemplars --query-image path/to/query.png --top-k 10
#   python run-vss.py --exemplar-dir exemplars --query-image path/to/query.png --device cuda:0

import argparse
import sys
from pathlib import Path

# Add the project root to the path so we can import sdcat modules
sys.path.insert(0, str(Path(__file__).parent))

from sdcat.cluster.embedding import ViTWrapper

from vit_similarity import (
    load_exemplar_images,
    compute_embeddings,
    normalize_embeddings,
    find_similar,
)


def main():
    parser = argparse.ArgumentParser(
        description="Vector Similarity Search (VSS) example using ViT embeddings",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run-vss.py --exemplar-dir exemplars --query-image copepod.png
  python run-vss.py --exemplar-dir exemplars --query-image query.png --top-k 10
  python run-vss.py --exemplar-dir exemplars --query-image query.png --device cuda:0
        """
    )
    parser.add_argument(
        "--exemplar-dir",
        type=Path,
        required=True,
        help="Directory containing exemplar images"
    )
    parser.add_argument(
        "--query-image",
        type=Path,
        required=True,
        help="Path to query image"
    )
    parser.add_argument(
        "--model",
        type=str,
        default="/mnt/DeepSea-AI/models/CFE/cfe_isiis_final-20250509/",
        help="ViT model name (default: /mnt/DeepSea-AI/models/CFE/cfe_isiis_final-20250509/)"
    )
    parser.add_argument(
        "--device",
        type=str,
        default="cpu",
        help="Device to use (cpu, cuda:0, cuda:1, etc.)"
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of similar images to return (default: 5)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Batch size for embedding computation (default: 32)"
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not args.exemplar_dir.exists():
        print(f"Error: Exemplar directory not found: {args.exemplar_dir}")
        sys.exit(1)
    
    if not args.query_image.exists():
        print(f"Error: Query image not found: {args.query_image}")
        sys.exit(1)
    
    # Load exemplar images
    print(f"Loading exemplar images from: {args.exemplar_dir}")
    exemplar_paths = load_exemplar_images(args.exemplar_dir)
    
    if len(exemplar_paths) == 0:
        print("Error: No images found in exemplar directory")
        sys.exit(1)
    
    print(f"Found {len(exemplar_paths)} exemplar images")
    
    # Initialize ViT model
    print(f"Initializing ViT model: {args.model} on {args.device}")
    vit = ViTWrapper(device=args.device, model_name=args.model, batch_size=args.batch_size)
    print(f"Embedding dimension: {vit.vector_dimensions}")
    
    # Compute embeddings for exemplars
    print(f"Computing embeddings for {len(exemplar_paths)} exemplar images...")
    exemplar_embeddings = compute_embeddings(vit, exemplar_paths, args.batch_size)
    print(f"Exemplar embeddings shape: {exemplar_embeddings.shape}")
    
    # Normalize embeddings for cosine similarity
    exemplar_embeddings_norm = normalize_embeddings(exemplar_embeddings)
    
    # Compute query embedding
    print(f"Computing embedding for query image: {args.query_image}")
    query_embedding, query_labels, query_scores = vit.process_images([str(args.query_image)])
    query_embedding_norm = normalize_embeddings(query_embedding)
    
    # Find similar images
    print(f"\nFinding top-{args.top_k} similar images...")
    top_indices, top_scores = find_similar(query_embedding_norm, exemplar_embeddings_norm, args.top_k)
    
    # Print results
    print("\n" + "=" * 60)
    print(f"Query Image: {args.query_image}")
    if query_labels:
        print(f"Query Prediction: {query_labels[0]} (score: {query_scores[0]})")
    print("=" * 60)
    print(f"\nTop-{args.top_k} Similar Exemplars:")
    print("-" * 60)
    
    for rank, (idx, score) in enumerate(zip(top_indices, top_scores), 1):
        exemplar_path = exemplar_paths[idx]
        print(f"{rank}. {exemplar_path.name}")
        print(f"   Path: {exemplar_path}")
        print(f"   Cosine Similarity: {score:.4f}")
        print()
    
    print("=" * 60)
    print("Done!")

    # Save query image and similar exemplars to a CSV file
    print(f"Saving results to run_vss.csv...")
    with open("run_vss.csv", "w") as f:
        f.write(f"query_image,{args.query_image}\n")
        for rank, (idx, score) in enumerate(zip(top_indices, top_scores), 1):
            exemplar_path = exemplar_paths[idx]
            f.write(f"{rank},{exemplar_path}\n")


if __name__ == "__main__":
    main()

