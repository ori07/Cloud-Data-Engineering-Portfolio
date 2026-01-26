import os

from ecommerce_etl import data_source, io_manager, source_validator, transforms


def run_pipeline():
    # 1. Configuration
    input_path = os.getenv("INPUT_PATH")
    output_base_path = os.getenv("GOLD_OUTPUT_PATH")

    print(f"Init ETL for: {input_path}")
    # 2. Load data
    ds = data_source.DataSourceFactory.get_data_source(input_path)
    df = ds.read()

    # 2. Extraction & Validation (Technical Silver)
    df = source_validator.load_and_validate(df)

    # 3. Transformation (Silver - Business Layer)
    df_transformed = transforms.enrich_data(df)
    df_final = transforms.prepare_partitions(df_transformed)

    # 4. Load (Gold)
    io_manager.save_to_gold(df_final, output_base_path)

    print("ETL ended SUCCESS.")


if __name__ == "__main__":
    run_pipeline()
