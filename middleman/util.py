import sys


def _print_message(message, file=None):
    if message:
        if file is None:
            file = sys.stderr
        file.write(message)
        file.write('\n')


def exit_prog(status=0, message=None):
    if message:
        _print_message(message, sys.stderr)
    sys.exit(status)
