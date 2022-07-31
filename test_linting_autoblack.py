import os


def test_linting(input_str: str) -> None:
    """test black formatting"""

    message = " a really long line of code which should be broken into multiple lines and reformatted by black "

    print(message)
    print("this should be on a new line not behind the previous print statement")

    print(["this", "list", "is", "also", "not", "formatted"])
