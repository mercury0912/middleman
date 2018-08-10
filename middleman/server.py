import sys

from middleman.log import Log
from middleman import options


def main():
    try:
        options.Options.parse_options()
        Log().init_logging(options.Options.options)
    except Exception as exc:
        print('ERROR: ', exc, file=sys.stderr)
        sys.exit(1)

    options.Options.display_options_info()


if __name__ == "__main__":
    main()
