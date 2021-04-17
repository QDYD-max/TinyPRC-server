#!/usr/bin/env bash

#项目目录
SHELL_PATH=$(cd "$(dirname "$0")"; pwd)

cd ${SHELL_PATH};
cd ..

make

./tools/run.sh ./skynet/skynet ./etc/config.game game
