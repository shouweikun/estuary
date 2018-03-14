#!/usr/bin/env bash
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`
if [ -r "$DEPLOY_DIR"/bin/setenv.sh ]; then
    . "$DEPLOY_DIR"/bin/setenv.sh
else
    echo "Cannot find $DEPLOY_DIR/bin/setenv.sh"
    echo "This file is needed to run this program"
    exit 1
fi

echo -e "Starting the $SERVER_NAME ...\c"
nohup java $JAVA_OPTS $JAVA_MEM_OPTS $JAVA_DEBUG_OPTS $JAVA_JMX_OPTS -classpath $CONF_DIR:$LIB_JARS @main-class@ > $STDOUT_FILE 2>&1 &

echo "OK!"
PIDS=`ps -f | grep java | grep "$DEPLOY_DIR" | awk '{print $2}'`
echo "PID: $PIDS"
echo "STDOUT: $STDOUT_FILE"