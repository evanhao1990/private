from scripts.fetch_sovendus_commission import main
import argparse


def run(env):
    main(env)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-env", "--environment", help="Specify the environment", required=True)

    args = vars(parser.parse_args())
    run(env=args["environment"])
