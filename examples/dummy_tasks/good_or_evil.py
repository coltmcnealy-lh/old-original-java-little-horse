import sys


if __name__ == '__main__':
    character = sys.argv[1]

    if character in {"obiwan", "ahsoka", "yoda"}:
        print("good")
    elif character in {"vader", "palpatine", "dooku"}:
        print("evil")
