import os

from ecommerce_etl import io_factory, source_validator, transforms

from src.ecommerce_etl import data_source_factory


def run_pipeline():
    # 1. Configuration
    input_path = os.getenv("INPUT_PATH")
    # output_base_path = os.getenv("GOLD_OUTPUT_PATH")

    print(f"Init ETL for: {input_path}")
    # 2. Load data
    manager = io_factory.IOFactory.get_manager(input_path)
    parser = data_source_factory.DataSourceFactory.get_data_source(input_path)

    with manager.open_stream(input_path) as stream:
        df = parser.read(stream)

    # 2. Validation (Technical Silver)
    df = source_validator.validate_data_source(df)

    # 3. Transformation (Silver - Business Layer)
    df_transformed = transforms.enrich_data(df)
    df_final = transforms.prepare_partitions(df_transformed)
    print(df_final)

    # 4. Load (Gold)
    # io_manager.save_to_gold(df_final, output_base_path)

    print("ETL ended SUCCESS.")


if __name__ == "__main__":
    run_pipeline()
