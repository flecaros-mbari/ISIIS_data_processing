
[![MBARI](https://www.mbari.org/wp-content/uploads/2014/11/logo-mbari-3b.png)](http://www.mbari.org)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/language-Python-blue.svg)](https://www.python.org/downloads/)

# ISIIS_data_processing

This repository processes imagery and video from the ISIIS (In Situ Ichthyoplankton
Imaging System) instrument end-to-end: converting raw videos into frames, matching
each frame to a depth from the ROV's CTD log, cropping and organizing regions of
interest (ROIs) into class folders, running classification and vector-similarity
models over those crops, and computing evaluation metrics, anomaly scores, and
carbon-flux/volume estimates from the results.

The ISIIS images look like this:

![Alt text](<assets/img/CFE_ISIIS-243-2024-03-19 12-58-20.186_0588 (1).jpg>)

## Repository structure

```
src/
  data-handling/   video -> frame conversion, depth matching, ROI cropping/dataset assembly
  labeling/        export labeled data from Tator / FiftyOne (Voxel51), labeling QA
  predict/         batch image classification inference (Hugging Face models)
  cosine-distance/ vector similarity search (ViT embeddings) against exemplar images
  anomaly-score/   combine classification confidence + vector similarity into a risk score
  metrics/         classification metrics, confusion matrices, depth-distribution QA
  volume/          particle volume estimation and carbon-flux calculation
assets/            static assets (example image, etc.)
environment.yml    conda environment definition (name/prefix only, see Installation)
requirements.txt   pinned Python dependencies
```

### `src/data-handling/`

| Script | Purpose |
|---|---|
| `avi2mp4.py` | Recursively transcode `.avi` videos to fast-start `.mp4` (via `ffmpeg`), preserving directory structure. |
| `videos2frames.py` | Extract frames from `.mp4` videos at a given frame rate, in parallel. |
| `matchdepth.py` | Match ROV CTD pressure/time log entries to image timestamps, convert pressure to depth, and optionally rename image files with their matched depth. |
| `depth_profile.py` | Plot per-class label counts across depth ranges from a Tator TSV export. |
| `create_folder.py` | Crop ROIs out of images (or copy existing crops) into per-class folders, based on detection CSVs. |
| `get_data_train.py` | Crop ROIs into class-named training folders from either a Tator TSV export or per-video detection CSVs. |
| `class2folder.py` | Copy already-cropped images into per-class folders based on a predictions CSV. |
| `adding_columns.py` | Clean up detection/crop CSVs: drop unused columns, remap image/crop paths, add `elemental_id` and `predicted_class` columns. |
| `choosing-data.py` | Interactively (via `pygame`) review and curate a target-sized sample of depth-tagged images. |
| `roi_crop.py` | Shared helper (not a CLI) used by `create_folder.py` and `get_data_train.py` to crop and clamp a bounding box out of an image. |

### `src/labeling/`

| Script | Purpose |
|---|---|
| `pulling_data.py` | Export verified localizations/labels from Tator to `isiis_labels.tsv`. Requires `TATOR_TOKEN` env var and network access to the Tator server. |
| `download.py` | Export a labeled dataset from FiftyOne/Voxel51 to CSV. Requires network access to the MBARI FiftyOne/MongoDB server. |
| `testing_voxel51.py` | Find per-class area/score thresholds (ROC + Youden's J) that separate correct from incorrect Voxel51 predictions, and flag review candidates. |

### `src/predict/`

| Script | Purpose |
|---|---|
| `huggingface.py` | Batch-classify ROI crops referenced in CSVs using a Hugging Face image-classification model, then post-process to add second-best class/score columns. Overwrites the input CSVs in place. |

### `src/cosine-distance/`

| Script | Purpose |
|---|---|
| `run-vss.py` | Vector similarity search for a single query image against a directory of exemplar images, using ViT embeddings. |
| `cosine_distance.py` | Same idea, batch mode: reads query images from a CSV and writes `vector_prediction_{k}`/`vector_score_{k}` columns for the top-k matches. |
| `vit_similarity.py` | Shared helper (not a CLI): embedding computation, normalization, and cosine-similarity search used by both scripts above. |

### `src/anomaly-score/`

| Script | Purpose |
|---|---|
| `computing-anomaly-score.py` | Combine a classification model's confidence (`class`/`score`, from `predict/huggingface.py`) with vector-similarity output (`vector_prediction_1`/`vector_score_1`, from `cosine-distance/cosine_distance.py`) into a single weighted anomaly score, and plot its distribution. |

### `src/metrics/`

| Script | Purpose |
|---|---|
| `all_metrics.py` | Per-class accuracy/precision/recall/F1, confusion matrix, balanced accuracy, and error-rate-vs-depth plots from a ground-truth-vs-predicted CSV. |
| `depth_profile_predictions.py` | Normalize and plot predicted-class proportions by depth range against a reference image-count distribution. |
| `depth_utils.py` | Shared helper (not a CLI): canonical excluded-classes list, depth-from-filename extraction, and depth binning. |

### `src/volume/`

| Script | Purpose |
|---|---|
| `volumes.py` | Batch-process gel-trap particle images into volume and carbon-flux estimates, saved to Excel. |
| `optimization.py` | Fit per-class flux shape parameters (`a`, `b`) to match a target total flux, using the same image-processing pipeline as `volumes.py`. |
| `volume_core.py` | Shared helper (not a CLI): image segmentation, distance-transform volume calculation, metadata extraction, and flux math used by both scripts above. |

## Installation

```bash
conda env create -f environment.yml
conda activate isiis
pip install -r requirements.txt
```

`environment.yml` only pins the environment name/prefix — all Python dependencies come
from `requirements.txt`.

One dependency isn't a normal pip install and needs manual setup:

- **`sdcat`** — MBARI's clustering/embedding toolkit (github.com/mbari-org/sdcat), used by
  `cosine-distance/run-vss.py` and `cosine-distance/cosine_distance.py` for `ViTWrapper`.
  It's not published as a plain version pin; install it from source (clone the repo, then
  `pip install .` or `pip install -e .`) into this environment. If you keep it in a
  separate environment instead (as some setups do), you'll need to run the
  `cosine-distance/` scripts there rather than in `isiis`.

You'll also need **`ffmpeg`** installed as a system binary (not a Python package) for
`data-handling/avi2mp4.py`.

**Note on `requirements.txt` accuracy:** `torch`, `torchvision`, `tator`, `pygame`, and
`openpyxl` are pinned in `requirements.txt` (they're required by `predict/huggingface.py`,
`cosine-distance/`, `labeling/pulling_data.py`, `data-handling/choosing-data.py`, and
`volume/*.py`'s Excel I/O, respectively) but may not always be present in every `isiis`
environment — if a script fails with `ModuleNotFoundError` for one of these, install it
explicitly (`pip install torch torchvision`, etc.) or run that script from an environment
that already has it.

### Environment variables

- **`TATOR_TOKEN`** — required by `labeling/pulling_data.py`. Export it before running:
  ```bash
  export TATOR_TOKEN="<your-tator-api-token>"
  ```

### Network access

Several scripts talk to MBARI-internal servers and will only work on the MBARI network
(or over VPN):

- `labeling/pulling_data.py` — Tator server (`mantis.shore.mbari.org`).
- `labeling/download.py` — FiftyOne/MongoDB server (`mantis.shore.mbari.org:27017`).

### Hardware

`predict/huggingface.py` and `cosine-distance/*.py` run neural network inference and
accept a `--device`/model argument; they'll run on CPU but are much faster with a CUDA
GPU. `data-handling/create_folder.py` and the `volume/` scripts are CPU-only.

## Running the scripts

All scripts live under `src/<folder>/` and are run directly with `python`, e.g.:

```bash
python src/data-handling/avi2mp4.py --input raw_videos/ --output mp4_videos/
```

Every script exposes `--help` for its full argument list. Below is a typical pipeline
order with one example command per stage — adjust paths to your data.

1. **Convert video and extract frames**
   ```bash
   python src/data-handling/avi2mp4.py --input raw_videos/ --output mp4_videos/
   python src/data-handling/videos2frames.py --input mp4_videos/ --output frames/ --fps 1
   ```

2. **Match frames to depth** (needs a ROV CTD log)
   ```bash
   python src/data-handling/matchdepth.py --ctd-path ctd_log.csv --images-dir frames/ --rename
   ```

3. **Pull labeled data** (needs MBARI network access + credentials, see above)
   ```bash
   TATOR_TOKEN=... python src/labeling/pulling_data.py
   python src/labeling/download.py
   ```

4. **Crop ROIs into class folders**
   ```bash
   python src/data-handling/create_folder.py --csv-dir detections/ --min-area 210
   # or, from a Tator TSV export:
   python src/data-handling/get_data_train.py --tator --tsv-path isiis_labels.tsv
   ```

5. **Classify crops**
   ```bash
   python src/predict/huggingface.py --model <hf-model-name-or-path> --csv-dir detections/
   ```

6. **Vector similarity search** (needs `sdcat`)
   ```bash
   python src/cosine-distance/cosine_distance.py --exemplar-dir exemplars/ --query-csv detections/detections.csv
   ```

7. **Combine into an anomaly score**
   ```bash
   python src/anomaly-score/computing-anomaly-score.py --input-csv final_all_dataset.csv --output-csv scored.csv
   ```

8. **Evaluate**
   ```bash
   python src/metrics/all_metrics.py --input-csv predictions.csv
   python src/metrics/depth_profile_predictions.py --csv-path predictions.csv
   ```

9. **Volume / carbon flux** (gel-trap microscopy images, independent of the pipeline above)
   ```bash
   python src/volume/volumes.py --directory geltrap_images/ --metadata-excel trap_summary.xlsx
   python src/volume/optimization.py --directory geltrap_images/ --metadata-excel trap_summary.xlsx --target-flux 0.008
   ```

### Notes

- Most scripts' `--help` defaults reflect paths from the original development machine
  (e.g. `/Volumes/CFElab/...`, `/Users/fernandalecaros/Downloads/...`) — always pass
  your own paths explicitly rather than relying on the defaults.
- `predict/huggingface.py` overwrites its input CSVs in place; keep a backup if you need
  the pre-classification data.
- `src/labeling/testing_voxel51.py`, `src/anomaly-score/computing-anomaly-score.py`, and
  the `src/volume/` scripts assume specific upstream column names — check each script's
  module docstring for the exact expected input schema before running it on new data.
