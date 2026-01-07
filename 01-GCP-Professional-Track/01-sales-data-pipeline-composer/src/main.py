import os

from ecommerce_etl import io_manager, source_validator, transforms


def run_pipeline():
    # 1. Configuration
    input_path = os.getenv(
        "INPUT_PATH", "gs://my-ecommerce-data-proyect/bronze/sample.csv"
    )
    output_base_path = os.getenv("GOLD_OUTPUT_PATH")

    print(f"Init ETL for: {input_path}")

    # 2. Extraction & Validation (Silver Técnica)
    df = source_validator.load_and_validate(input_path)

    # 3. Transformation (Silver - Business Layer)
    df_transformed = transforms.enrich_data(df)
    df_final = transforms.prepare_partitions(df_transformed)

    # 4. Load (Gold)
    io_manager.save_to_gold(df_final, output_base_path)

    print("ETL ended SUCCESS.")


if __name__ == "__main__":
    run_pipeline()
