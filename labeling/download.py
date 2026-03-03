import fiftyone as fo
import os

output_dir = "/Users/fernandalecaros/Downloads/output_dat_compltetex/"
os.makedirs(output_dir, exist_ok=True)

fo.config.database_uri = "mongodb://mantis.shore.mbari.org:27017"
dataset = fo.load_dataset("isiis_RachelCarson_2024_02")

print(dataset.get_field_schema().keys())
sample = dataset.first()
print(sample.tags)

dataset.export(
    export_dir=output_dir,
    dataset_type=fo.types.CSVDataset,
    fields=["uuid", "ground_truth.confidence", "ground_truth.label", "predicted_label", "score", "area", "ground_truth.verified", "depth"],
    export_media=True
)