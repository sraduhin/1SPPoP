from etl import load, transform, extract


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
