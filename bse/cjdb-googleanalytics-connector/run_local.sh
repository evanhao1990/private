#!/usr/bin/env bash

# Teamcity build step
bash ./build_local.sh

# Teamcity Run command step
export CONTAINER_AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export CONTAINER_AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION
export db_host_rs=$db_host_rs
export db_password_rs=$db_password_rs
export db_user_rs=$db_user_rs
#export RUN_COMMAND="python /code/run_new_app_session.py -env dev -days -3"
export RUN_COMMAND="python /code/run_fact_ua_categories.py -env dev -date_ranges 2020-01-01 2020-02-05"
bash ./run.sh