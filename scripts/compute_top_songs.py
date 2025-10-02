import os

from src.constants import AirflowConfig
from src.pipelines.lastfm import Entrypoint


def main():
    input_dir = os.environ.get(AirflowConfig.LASTFM_DIR_ENV.value, AirflowConfig.DEFAULT_LASTFM_DIR.value)
    output_dir = os.environ.get(AirflowConfig.OUTPUT_DIR_ENV.value, AirflowConfig.DEFAULT_OUTPUT_DIR.value)

    # Use Entrypoint for full pipeline execution
    entrypoint = Entrypoint()
    entrypoint.run(input_dir=input_dir, output_dir=output_dir)


if __name__ == "__main__":
    main()
