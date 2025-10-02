import os

from src.constants import AirflowConfig
from src.pipelines.lastfm import Entrypoint


def main():
    input_dir = os.environ.get(AirflowConfig.LASTFM_DIR_ENV.value, AirflowConfig.DEFAULT_LASTFM_DIR.value)
    
    # Use Entrypoint for validation only
    entrypoint = Entrypoint()
    entrypoint.run(input_dir=input_dir, validate_only=True)


if __name__ == "__main__":
    main()
