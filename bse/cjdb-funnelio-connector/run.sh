#!/usr/bin/env bash
echo "####################################################"
echo "Running run.sh"
MSYS_NO_PATHCONV=1 docker run --rm=true \
           -e AWS_ACCESS_KEY_ID=$CONTAINER_AWS_ACCESS_KEY_ID \
           -e AWS_SECRET_ACCESS_KEY=$CONTAINER_AWS_SECRET_ACCESS_KEY \
           -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
           -e db_host_rs=$db_host_rs \
           -e db_password_rs=$db_password_rs \
           -e db_user_rs=$db_user_rs \
           -v `pwd`:/code \
           funnel_connector:latest $RUN_COMMAND
