from scripts.fetch_dim_adwords_campaign import main
import argparse


def run(env, days):
    main(env, days)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-env", "--environment", help="Specify the environment", required=True)
    parser.add_argument("-days", "--days", help="Input how many days to go back or the date range required", nargs="+", type=str, required=True)

    args = vars(parser.parse_args())
    run(env=args["environment"], days=args["days"])
