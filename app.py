import json
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds


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

# 'flusight_fhub_schema': tasks.json:
# - task_ids: round 0: reference_date, target, horizon, location, target_end_date
#   -> columns: reference_date, target, horizon, location, target_end_date  |  output_type, output_type_id, value
# - rounds[0].model_tasks[0].output_type.pmf.value.type : double
# - rounds[0].model_tasks[1].output_type.quantile.value.type : double
# - rounds[0].model_tasks[1].output_type.sample.value.type : integer
# - rounds[0].model_tasks[2].output_type.quantile.value.type : double
# - rounds[0].model_tasks[3].output_type.pmf.value.type : double
flusight_fhub_schema = ecfh_schema

# REF: inferred schema from below:
#   forecast_date: date32[day]
#   horizon: int64
#   target: string
#   location: string
#   output_type: string
#   output_type_id: double
#   value: int64
#   model_id: string

projs_path = Path('/Users/cornell/IdeaProjects')
hub_to_dir_schema_tuple = {
    'flusight': (projs_path / 'hubUtils/inst/testhubs/flusight', flusight_schema),  # .csv, .parquet, and .arrow (!)
    'parquet': (projs_path / 'hubUtils/inst/testhubs/parquet', parquet_schema),  # .parquet
    'simple': (projs_path / 'hubUtils/inst/testhubs/simple', None),  # .csv and .parquet
    'ecfh': (projs_path / 'example-complex-forecast-hub', ecfh_schema),  # .csv
    'flusight_archive': (projs_path / 'flusight_hub_archive', flusight_archive_schema),  # .parquet
    'flusight_fhub': (projs_path / 'FluSight-forecast-hub', flusight_fhub_schema),  # .csv
}


def main():
    """
    results from using pyarrow.dataset.dataset() to scan various file types:

    hub_path  ds format  result
    --------  ---------  ------
    * [mix: .csv and .parquet]
    flusight  csv        pyarrow.lib.ArrowInvalid: Could not open CSV input source '/Users/cornell/IdeaProjects/hubUtils/inst/testhubs/flusight/forecasts/hub-baseline/2023-05-08-hub-baseline.parquet': Invalid: CSV parse error: Row #2: Expected 2 columns, got 3: forecast_date...
                           -> PROB: trying to open a .parquet file as .csv
    flusight  parquet    pyarrow.lib.ArrowInvalid: Error creating dataset. Could not read schema from '/Users/cornell/IdeaProjects/hubUtils/inst/testhubs/flusight/forecasts/hub-baseline/2023-04-24-hub-baseline.csv'. Is this a 'parquet' file?: Could not open Parquet input source '/Users/cornell/IdeaProjects/hubUtils/inst/testhubs/flusight/forecasts/hub-baseline/2023-04-24-hub-baseline.csv': Parquet magic bytes not found in footer. Either the file is corrupted or this is not a parquet file.
    flusight  [none]     pyarrow.lib.ArrowInvalid: Error creating dataset. Could not read schema from '/Users/cornell/IdeaProjects/hubUtils/inst/testhubs/flusight/forecasts/hub-baseline/2023-04-24-hub-baseline.csv'. Is this a 'parquet' file?: Could not open Parquet input source '/Users/cornell/IdeaProjects/hubUtils/inst/testhubs/flusight/forecasts/hub-baseline/2023-04-24-hub-baseline.csv': Parquet magic bytes not found in footer. Either the file is corrupted or this is not a parquet file.
                           -> PROB: trying to open a .csv file as .parquet

    * mix, exclude_invalid_files=True
    flusight  parquet    pyarrow.lib.ArrowInvalid: Float value 367.89 was truncated converting to int32
                           -> PROB: schema (seems to scan `2023-05-08-hub-baseline.parquet` first, which has all ints in `value` col, then errors `2023-05-08-hub-ensemble.parquet` with floats in `value`)
    flusight  csv        pyarrow.lib.ArrowInvalid: Could not open CSV input source '/Users/cornell/IdeaProjects/hubUtils/inst/testhubs/flusight/forecasts/hub-ensemble/2023-04-24-hub-ensemble.csv': Invalid: In CSV column #6: Row #2: CSV conversion error to int64: invalid value '232.14'
                           -> PROB: schema ("" - scans `2023-04-24-hub-baseline.csv` first then errors on `2023-04-24-hub-ensemble.csv`)

    * [only: .parquet]
    parquet   csv        pyarrow.lib.ArrowInvalid: Error creating dataset. Could not read schema from '/Users/cornell/IdeaProjects/hubUtils/inst/testhubs/parquet/model-output/hub-baseline/2022-10-01-hub-baseline.parquet'. Is this a 'csv' file?: Could not open CSV input source '/Users/cornell/IdeaProjects/hubUtils/inst/testhubs/parquet/model-output/hub-baseline/2022-10-01-hub-baseline.parquet': Invalid: CSV parse error: Row #2: Expected 11 columns, got 1: &�
                           -> PROB: trying to open a .parquet file as .csv
    parquet   parquet    OK
    parquet   [none]     OK

    * [only: .csv]
    ecfh      csv        pyarrow.lib.ArrowInvalid: In CSV column #6: Row #26502: CSV conversion error to double: invalid value 'low'
                           -> PROB: for the `output_type_id` column, it's apparently inferring a schema based on some sample from the head before that row
    """

    hub_name = 'flusight'  # .csv, .parquet, and .arrow (!). [292 rows x 8 columns]
    # hub_name = 'parquet'  # .parquet . [599 rows x 9 columns]
    # hub_name = 'ecfh'  # .csv . [553264 rows x 9 columns] (0.5M)
    # hub_name = 'flusight_archive'  # .parquet . [29364474 rows x 9 columns] (29M)
    # hub_name = 'flusight_fhub'  # .csv . [5812608 rows x 9 columns] (5M)

    hub_path = hub_to_dir_schema_tuple[hub_name][0]

    # for now the schema is hard-coded, but we'll need to eventually extract it from `tasks.json` via a Python version
    # of https://hubverse-org.github.io/hubData/reference/create_hub_schema.html :
    hub_schema = hub_to_dir_schema_tuple[hub_name][1]

    print(f"entered. {hub_name=}, {hub_path=}, {hub_schema=}")
    with open(hub_path / 'hub-config/admin.json') as fp:
        admin_dict = json.load(fp)
        model_output_dir = admin_dict['model_output_dir'] if 'model_output_dir' in admin_dict else 'model-output'
        hub_ds = make_dataset(hub_path / model_output_dir, hub_schema, admin_dict['file_format'])
        print_dataset(hub_ds)


def make_dataset(model_output_path: Path, hub_schema: pa.Schema, file_formats: list[str]) -> pa.dataset.Dataset:
    # create the dataset. NB: we are using dataset "directory partitioning" to automatically get the `model_id` column
    # from directory names. NB: each dataset is a pyarrow._dataset.FileSystemDataset (so far!)
    datasets = [ds.dataset(model_output_path, format=file_format, partitioning=['model_id'], exclude_invalid_files=True,
                           schema=hub_schema)
                for file_format in file_formats]
    return ds.dataset([dataset for dataset in datasets
                       if isinstance(dataset, pa.dataset.FileSystemDataset) and (len(dataset.files) != 0)])


def print_dataset(hub_ds: pa.dataset.Dataset):
    print('\n* dataset')
    print(hub_ds)

    print('\n* schema')
    print(hub_ds.schema.to_string(show_field_metadata=False))

    print(f"\n* files")
    if isinstance(hub_ds, pa.dataset.UnionDataset):
        for child_ds in hub_ds.children:  # each dataset is a pyarrow._dataset.FileSystemDataset (so far!)
            print(f"\n** {child_ds=} ({len(child_ds.files)})")

            head_count = 3
            files = child_ds.files if len(child_ds.files) <= head_count * 2 \
                else child_ds.files[:head_count] + ['...'] + child_ds.files[-head_count:]
            for file in files:
                print(f"{file!r}")
    else:
        print(f"can't handle dataset of type '{type(hub_ds)}'. {hub_ds=}")

    print('\n* table')
    print(hub_ds.to_table())

    print('\n* table pandas')
    print(hub_ds.to_table().to_pandas())  # convert to see the contents of the scanned table


if __name__ == '__main__':
    main()
