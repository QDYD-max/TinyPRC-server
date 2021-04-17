#!/usr/bin/env bash

#项目目录
SHELL_PATH=$(cd "$(dirname "$0")"; pwd)

#运行脚本
run()
{
    #检查启动程序
    if [ ! -x "${2}"  ]; then
        echo "启动程序没有,${2} "
        exit
    fi
    #关闭程序
    if [ -a "${TMP_PATH}kill_${1}.sh"  ]; then
        echo "关闭程序："
        sh ${TMP_PATH}kill_${1}.sh
    fi
    #设置日志存储
    LOG_NAME="${LOG_PATH}${1}.log"
    #备份日志
    if [ -a "${LOG_NAME}"  ]; then
        rm -rf ${LOG_NAME}
    fi
    #启动
    nohup ${2} ${3} >> ${LOG_NAME} 2>&1 &
    # (${2} ${3} &)
    #生成关闭的程序
    echo "#!/usr/bin/env bash" > ${TMP_PATH}kill_${1}.sh
    echo "echo 'run: ${2} ${3} pid: $!'" >> ${TMP_PATH}kill_${1}.sh
    echo "kill -9 $!" >> ${TMP_PATH}kill_${1}.sh
    chmod 777 ${TMP_PATH}kill_${1}.sh
    #显示运行的程序
    echo "运行程序："
    echo "run:$2 $3  pid:$!  log:${LOG_NAME} "
    #打印启动错误
    sleep 2
    if [ -s "${LOG_NAME}"  ]; then
        echo "启动日志："
        cat ${LOG_NAME}
        # exit
    fi
    sleep 1
}

cd ${SHELL_PATH};
cd ..
#日志目录
LOG_PATH="./log/"
if [ ! -x "$LOG_PATH"  ]; then
    mkdir "$LOG_PATH"
fi

#tmp目录
TMP_PATH="./tmp/"
if [ ! -x "$TMP_PATH"  ]; then
    mkdir "$TMP_PATH"
fi

SERVER=$1
CONFIG_NAME=$2
NAME=$3
echo "  >>---------- ${NAME} server"
echo ""
run ${NAME} ${SERVER} ${CONFIG_NAME}
echo ""
echo "  >>---------- end"
