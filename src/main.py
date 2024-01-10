from core.config import logger
from etl import load, transform, extract
from index import build_index


def main():
    """
    The idea of pipeline has been respectfully stolen from
    https://github.com/s-klimov/etl-template/tree/1-base
    """
    unloads = load()
    multiplication = transform(unloads)
    extract(multiplication)


if __name__ == "__main__":
    main()
    logger.info("All changes has been updated. ;)")

