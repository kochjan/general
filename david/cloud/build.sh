#!/bin/bash
version=17
docker build -t das-test .
docker tag -f das-test gcr.io/nipun-950-core/cjs-testingv2:$version
gcloud docker push gcr.io/nipun-950-core/cjs-testingv2:$version
