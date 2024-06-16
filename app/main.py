from internal.bootstrap import init_all
from internal.etl.processors import (
    BooksETLProcessor
)


def main():
    init_all()
    BooksETLProcessor.run()


if __name__ == '__main__':
    main()
