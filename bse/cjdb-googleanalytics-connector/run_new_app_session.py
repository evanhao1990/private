from scripts.fetch_new_app_sessions import main
import argparse


def run(env, **date_args):
    main(env, **date_args)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-env", "--environment", help="Specify the environment", required=True)
    parser.add_argument("-date_ranges", "--date_ranges", help="Specify the date range",nargs="+", type=str, required=False)
    parser.add_argument("-days", "--days", help="Specify how many days to go back",  required=False)
    parser.add_argument("-months", "--months", help="Specify how many months to go back", required=False)

    args = vars(parser.parse_args())
    run(env=args["environment"], date_ranges=args["date_ranges"], days=args["days"], months=args["months"])
