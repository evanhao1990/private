#!/usr/bin/env bash
echo "#########################################"
echo "Running run.sh"
REPOSITORY_NAME=analytics
ACCOUNT_ID=381841683508
IMAGE_NAME=analytics-toolkit
REPOSITORY_URI=$ACCOUNT_ID.dkr.ecr.eu-west-1.amazonaws.com/$REPOSITORY_NAME

echo "########################################"
echo "Login to ECR"
REPOSITORY_LOGIN=`aws ecr get-login --registry-ids $ACCOUNT_ID --no-include-email`
eval $REPOSITORY_LOGIN

# Pull docker image that was created in build.sh
# Comment this if running build_local.sh without image existing in ecr already
echo "########################################"
echo "Pulling $IMAGE_NAME from ECR"
docker pull $REPOSITORY_URI:$IMAGE_NAME

echo "########################################"
echo "Run command"
# Mount volume at pwd, place inside a folder called code.
# -e pass all env variables we need into container
docker run --rm=true \
           -v `pwd`:/code \
           $REPOSITORY_URI:$IMAGE_NAME $RUN_COMMAND

