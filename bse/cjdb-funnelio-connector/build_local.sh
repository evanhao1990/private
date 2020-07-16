#!/usr/bin/env bash
echo "########################################"
echo "Build Dockerfile"
docker build --no-cache -t funnel_connector:latest .