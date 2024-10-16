import evaluation
import pdslib
from pdslib import add


def main():
    print("Hello from on-device-budgeting!")

    print(evaluation.hello())

    print(add(1, 2))

    print(pdslib.__dict__)


if __name__ == "__main__":
    main()
