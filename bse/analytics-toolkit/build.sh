#!/usr/bin/env bash
echo "#############################################"
echo "Running build.sh"
REPOSITORY_NAME=analytics
ACCOUNT_ID=381841683508
IMAGE_NAME=analytics-toolkit
REPOSITORY_URI=$ACCOUNT_ID.dkr.ecr.eu-west-1.amazonaws.com/$REPOSITORY_NAME

echo "#############################################"
echo "Login to ECR"
REPOSITORY_LOGIN=`aws ecr get-login --registry-ids $ACCOUNT_ID --no-include-email`
eval $REPOSITORY_LOGIN

echo "#############################################"
echo "Build and tag image"
docker build --no-cache --pull -t $REPOSITORY_URI:$IMAGE_NAME .

echo "#############################################"
echo "Pushing image to ECR"
docker push $REPOSITORY_URI:$IMAGE_NAME

echo "#############################################"
echo "Cleaning up"
docker rmi $REPOSITORY_URI:$IMAGE_NAME
rm -rf deps/