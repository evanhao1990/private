#!/usr/bin/env bash
echo "#############################################"
echo "Running build.sh"

echo "#############################################"
echo "Build Dockerfile"
docker build --no-cache -t cost_processor:latest .
