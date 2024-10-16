import json
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds


#
# hand-defined schemas
#

# 'flusight': tasks.json:
# - task_ids: round 0: forecast_date, target, horizon, location
#   -> columns: forecast_date, horizon, target, location  |  output_type, output_type_id, value
# - rounds[0].model_tasks[0].output_type.pmf.value.type      : double
# - rounds[0].model_tasks[1].output_type.quantile.value.type : integer  # todo xx wrong!?
# - rounds[0].model_tasks[1].output_type.mean.value.type     : double
# - ex rows: 2023-04-24-hub-baseline.csv:
#     forecast_date|horizon|target               |location|output_type|output_type_id|value
#     2023-04-24   |1      |wk ahead inc flu hosp|US      |mean       |NA            |1033
#     2023-04-24   |1      |wk ahead inc flu hosp|US      |quantile   |0.01          |0
# - ex rows: 2023-04-24-hub-ensemble.csv:
#     forecast_date|horizon|target               |location|output_type|output_type_id|value
#     2023-04-24   |1      |wk ahead inc flu hosp|US      |quantile   |0.01          |232.14
# - ex rows: 2023-05-01-umass_ens.csv:
#     forecast_date|horizon|target                 |location|output_type|output_type_id|value
#     2023-05-01   |2      |wk flu hosp rate change|US      |pmf        |large_increase|0
#
flusight_schema = pa.schema([('forecast_date', pa.date32()),
                             ('target', pa.string()),
                             ('horizon', pa.int8()),
                             ('location', pa.string()),
                             ('output_type', pa.string()),
                             ('output_type_id', pa.string()),
                             ('value', pa.float32()),
                             ('model_id', pa.string())])

# 'parquet': tasks.json:
# - task_ids: round 0: origin_date, target, horizon, location
# - task_ids: round 1: origin_date, target, horizon, location, age_group  # todo xx how to handle multiple rounds?
#   -> columns: origin_date, target, horizon, location  |  output_type, output_type_id, value
# - rounds[0].model_tasks[0].output_type.mean.value.type     : integer
# - rounds[0].model_tasks[0].output_type.quantile.value.type : integer
# - rounds[1].model_tasks[0].output_type.mean.value.type     : integer
# - rounds[1].model_tasks[0].output_type.quantile.value.type : integer
parquet_schema = pa.schema([('origin_date', pa.date32()),
                            ('target', pa.string()),
                            ('horizon', pa.int8()),
                            ('location', pa.string()),
                            ('age_group', pa.string()),  # present only in some files
                            ('output_type', pa.string()),
                            ('output_type_id', pa.string()),
                            ('value', pa.float32()),
                            ('model_id', pa.string())])

# 'ecfh': tasks.json:
# - task_ids: round 0: reference_date, target, horizon, location, target_end_date
#   -> columns: location, reference_date, horizon, target_end_date, target  |  output_type, output_type_id, value
# - rounds[0].model_tasks[0].output_type.pmf.value.type      : double
# - rounds[0].model_tasks[1].output_type.cdf.value.type      : double
# - rounds[0].model_tasks[2].output_type.mean.value.type     : double
# - rounds[0].model_tasks[2].output_type.median.value.type   : double
# - rounds[0].model_tasks[2].output_type.quantile.value.type : double
# - rounds[0].model_tasks[2].output_type.sample.value.type   : integer
ecfh_schema = pa.schema([('reference_date', pa.date32()),
                         ('target', pa.string()),
                         ('horizon', pa.int8()),
                         ('location', pa.string()),
                         ('target_end_date', pa.date32()),
                         ('output_type', pa.string()),
                         ('output_type_id', pa.string()),
                         ('value', pa.float32()),
                         ('model_id', pa.string())])

# 'flusight_fhub': tasks.json:
# - task_ids: round 0: reference_date, target, horizon, location, target_end_date
#   -> columns: reference_date, target, horizon, location, target_end_date  |  output_type, output_type_id, value
# - rounds[0].model_tasks[0].output_type.pmf.value.type : double
# - rounds[0].model_tasks[1].output_type.quantile.value.type : double
# - rounds[0].model_tasks[1].output_type.sample.value.type : integer
# - rounds[0].model_tasks[2].output_type.quantile.value.type : double
# - rounds[0].model_tasks[3].output_type.pmf.value.type : double
flusight_fhub_schema = ecfh_schema

# 'flusight_archive': tasks.json:
# - task_ids: round 0: origin_date, target, horizon, location, origin_epiweek
#   -> columns: origin_date, origin_epiweek, location, target, horizon  |  output_type, output_type_id, value
# - rounds[0].model_tasks[0].output_type.mean.value.type : double
# - rounds[0].model_tasks[0].output_type.pmf.value.type  : double
# - rounds[0].model_tasks[1].output_type.mean.value.type : double
# - rounds[0].model_tasks[1].output_type.cdf.value.type  : double
# - rounds[0].model_tasks[2].output_type.mean.value.type : double
# - rounds[0].model_tasks[2].output_type.cdf.value.type  : double
flusight_archive_schema = pa.schema([('origin_date', pa.date32()),
                                     ('target', pa.string()),
                                     ('horizon', pa.int8()),
                                     ('location', pa.string()),
                                     ('origin_epiweek', pa.string()),
                                     ('output_type', pa.string()),
                                     ('output_type_id', pa.string()),
                                     ('value', pa.float32()),
                                     ('model_id', pa.string())])

#
# dict defining hubs and their schemas
#

projs_path = Path('/Users/cornell/IdeaProjects')
hub_to_dir_schema_s3_tuple = {
    'flusight': (projs_path / 'hubUtils/inst/testhubs/flusight',  # .csv, .parquet, .arrow [292 rows x 8 columns]
                 flusight_schema, None),
    'parquet': (projs_path / 'hubUtils/inst/testhubs/parquet',  # .parquet [599 rows x 9 columns]
                parquet_schema, None),
    'ecfh': (projs_path / 'example-complex-forecast-hub',  # .csv [553264 rows x 9 columns] (0.5M)
             ecfh_schema, 'example-complex-forecast-hub'),
    'flusight_fhub': (projs_path / 'FluSight-forecast-hub',  # .csv [5812608 rows x 9 columns] (5M)
                      flusight_fhub_schema, 'cdcepi-flusight-forecast-hub'),
    'flusight_archive': (projs_path / 'flusight_hub_archive',  # .parquet [29364474 rows x 9 columns] (29M)
                         flusight_archive_schema, 'uscdc-flusight-hub-v1'),
}


#
# main() and friends
#

def main():
    # hub_name = 'flusight'
    # hub_name = 'parquet'
    # hub_name = 'ecfh'
    hub_name = 'flusight_fhub'
    # hub_name = 'flusight_archive'

    # get hub info. for now the schema is hard-coded, but we'll need to eventually extract it from `tasks.json` via a
    # Python version of https://hubverse-org.github.io/hubData/reference/create_hub_schema.html :
    hub_path = hub_to_dir_schema_s3_tuple[hub_name][0]
    hub_schema = hub_to_dir_schema_s3_tuple[hub_name][1]
    hub_s3_bucket = hub_to_dir_schema_s3_tuple[hub_name][2]  # None if hub not cloud-enabled

    print(f"main(): entered. {hub_name=}, {hub_path=}, {hub_s3_bucket=}, hub_schema:\n{hub_schema}")
    with open(hub_path / 'hub-config/admin.json') as fp:
        admin_dict = json.load(fp)
        model_output_dir = admin_dict['model_output_dir'] if 'model_output_dir' in admin_dict else 'model-output'
        # hub_ds = make_path_dataset(hub_path, model_output_dir, hub_schema, admin_dict['file_format'])
        hub_ds = make_s3_dataset(hub_s3_bucket, model_output_dir, hub_schema)
        print_dataset(hub_ds)
        try_polars(hub_ds)


def make_path_dataset(hub_path: Path, model_output_dir: str, hub_schema: pa.Schema, file_formats: list[str]) \
        -> pa.dataset.Dataset:
    # create the dataset. NB: we are using dataset "directory partitioning" to automatically get the `model_id` column
    # from directory names. NB: each dataset is a pyarrow._dataset.FileSystemDataset (so far!)
    print(f"make_path_dataset(): entered")
    model_output_path = hub_path / model_output_dir
    datasets = [ds.dataset(model_output_path, format=file_format, partitioning=['model_id'], exclude_invalid_files=True,
                           schema=hub_schema)
                for file_format in file_formats]
    return ds.dataset([dataset for dataset in datasets
                       if isinstance(dataset, pa.dataset.FileSystemDataset) and (len(dataset.files) != 0)])


def make_s3_dataset(hub_s3_bucket: str, model_output_dir: str, hub_schema: pa.Schema) -> pa.dataset.Dataset:
    print(f"make_s3_dataset(): entered")
    s3_url = f"s3://{hub_s3_bucket}/{model_output_dir}/"
    return ds.dataset(s3_url, format='parquet', partitioning=['model_id'], exclude_invalid_files=True,
                      schema=hub_schema)


def print_dataset(hub_ds: pa.dataset.Dataset):
    def _print_FileSystemDataset(dataset: pa.dataset.FileSystemDataset):
        print(f"\n** {dataset=} ({len(dataset.files)})")

        head_count = 3
        files = dataset.files if len(dataset.files) <= head_count * 2 \
            else dataset.files[:head_count] + ['...'] + dataset.files[-head_count:]
        for file in files:
            print(f"{file!r}")


    print(f"print_dataset(): entered")

    print('\n* dataset')
    print(hub_ds)

    print(f"\n* schema={hub_ds.schema!r}")
    print(hub_ds.schema.to_string(show_field_metadata=False))

    print(f"\n* files")
    if isinstance(hub_ds, pa.dataset.UnionDataset):
        for child_ds in hub_ds.children:  # each dataset is a pyarrow._dataset.FileSystemDataset (so far!)
            _print_FileSystemDataset(child_ds)
    elif isinstance(hub_ds, pa.dataset.FileSystemDataset):
        _print_FileSystemDataset(hub_ds)
    else:
        print(f"can't handle dataset of type '{type(hub_ds)}'. {hub_ds=}")

    print('\n* table')
    print(hub_ds.to_table())  # Arrow table

    print('\n* table pandas')
    print(hub_ds.to_table().to_pandas())  # convert to see the contents of the scanned table


def try_polars(hub_ds: pa.dataset.Dataset):
    # from docs: "This functionality is considered unstable. It may be changed at any point without it being considered a breaking change."
    q = (
        pl.scan_pyarrow_dataset(hub_ds)
        .filter((pl.col("location") == "US") & (pl.col("output_type") == "quantile"))
        .select("model_id", "reference_date", "target", "output_type_id", "value")
    )
    print(f"try_polars(): {q=}")

    df = q.collect()
    print(f"try_polars(): {df=}")


if __name__ == '__main__':
    main()
