"""POC entrypoint (stub)."""

from lexinvo.core.pipeline import run_pipeline


def main() -> None:
    run_pipeline(
        input_path="input/azure_invoice.json",
        output_dir="output",
        config_dir="config",
        data_dir="data",
    )


if __name__ == "__main__":
    main()
