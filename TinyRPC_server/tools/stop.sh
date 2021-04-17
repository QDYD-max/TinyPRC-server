#!/usr/bin/env bash

PROJECT_PATH=$(cd "$(dirname "$0")"; cd ..; pwd)
TMP_PATH=${PROJECT_PATH}/tmp

for file in ${TMP_PATH}/*
do
if [ -f "$file" ] ; then
    ${file}
fi
done
