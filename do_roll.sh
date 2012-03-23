#!/bin/bash

function ensure_deploy_user() {
  ## make sure deploy is deploying
  if [ $(whoami) != "deploy" ]; then
    echo ""
    echo "$(basename $0) can only be run as deploy. Run like 'sudo -u deploy $0'."
    echo ""
    exit 1
  fi
}

ensure_deploy_user

if [[ -s ../rofl/commands/functions.inc ]]; then
  source ../rofl/commands/functions.inc
else
  echo "ERROR: ../rofl/commands/functions.inc not found, bailing out"
  exit 1
fi

function push_uuss_env() {
    local host="$1"
    local env="$2"
    local build_dir="$3"
    cd $build_dir
    local remote_path=""
    if [ "$env" = "production" ] || [ "$GAME" = "india" ] || [ "$GAME" = "rich" ] || ( [ "$GAME" = "dane" ] && ( [ "$env" = "next" ] || [ "$env" = "release" ] ) ) || [ "$GAME" = "victoria" ]; then
        remote_path="/var/www"
    else
        remote_path="/var/uuss-envs/$env/www"
    fi
    ssh $host "[ -e $remote_path ] || (mkdir -p $remote_path && echo '$remote_path created')"
    local EXCLUDES="--exclude=.svn --exclude=xls/ --exclude=xltm/ --exclude=xlam/ --exclude=schema_dumps/ --exclude=.git'*'"
    rsync -qrlpgoDz -c --delete $EXCLUDES --delete-excluded --progress lolapps uuss $host:$remote_path
}

function local_usage() {
    echo "usage: $0 ENV (dane|india|rich) <option>" 1>&2
    option_usage
}

if [ "x$1" = "x" ]; then
    local_usage
    exit 1
fi

ENVIRONMENT="$1"
shift

if [ "$ENVIRONMENT" = "production" ]; then
    ENVIRONMENT_GREP="."
else
    ENVIRONMENT_GREP="$ENVIRONMENT"
fi

if [ "$1" != "dane" ] && [ "$1" != "india" ] && [ "$1" != "rich" ]; then
    local_usage
    exit 1
fi

GAME="$1"
shift

if [ "x$1" = "x" ]; then
    hosts=$(cat /etc/hosts | awk '{print $3}' | grep uuss | grep $GAME)
else
    hosts="$*"
fi

BUILD_DIR=/tmp/uuss-build-$ENVIRONMENT
mkdir -p $BUILD_DIR
cd $BUILD_DIR

echo -n "Checking out lolapps to $BUILD_DIR..."
git clone -q git@github.com:lolapps/lolapps.git $BUILD_DIR/lolapps
echo "Done"

echo -n "Checking out uuss to $BUILD_DIR..."
git clone -q git@github.com:lolapps/uuss.git $BUILD_DIR/uuss
echo "Done"

function push_uuss_host() {
    local host="$1"
    #setup_uuss_server $host $BUILD_DIR $GAME
    echo -n "Failing healthcheck on $host..."
    ssh $host 'touch /tmp/healthcheck_fail.txt' > /dev/null 2>&1
    WAIT=20
    for (( wi = 1; wi < $WAIT; wi++ )); do
        echo -n "."
        sleep 1
    done
    echo "Done"
    ssh $host <<EOF 2> /dev/null
        svcs=\$(chkconfig | grep uuss | grep 2:on | grep $ENVIRONMENT_GREP | awk '{print \$1}')
        for svc in \$svcs; do
            service \$svc stop
        done
EOF
    echo -n "Pushing UUSS to $host..."
    push_uuss_env $host $ENVIRONMENT $BUILD_DIR
    echo "Done"
    ssh $host <<EOF 2> /dev/null
        #kill -9 \$(ps aux | grep uuss | grep -v grep | awk '{print \$2}')
        svcs=\$(chkconfig | grep uuss | grep 2:on | grep $ENVIRONMENT_GREP | awk '{print \$1}')
        rm -f /tmp/healthcheck_fail.txt
        for svc in \$svcs; do
            service \$svc start
        done
EOF
}

CHUNK_SIZE=1
TO_RUN=push_uuss_host
run_on_hosts $hosts

echo -n "Cleaning up $BUILD_DIR..."
rm -rf $BUILD_DIR
echo "Done"
