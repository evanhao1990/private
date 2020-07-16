#!/usr/bin/env bash
echo "########################################"
echo "Build Dockerfile"
docker build --no-cache -t rs_connector:latest .