#!/bin/sh

set -e

PYTHON=python
CONFIG_DIR=/etc/hustle
WORK_DIR=${PWD}
FROM=${WORK_DIR}/settings.yaml
DEST=${CONFIG_DIR}/settings.yaml

cd deps && make install
cd ${WORK_DIR} && ${PYTHON} setup.py install

if [[ ! -d ${CONFIG_DIR} ]]; then
    mkdir ${CONFIG_DIR}
elif [[ -f ${DEST} ]]; then
    read -p "Settings file already exists, overwrite it? [Yes/No]: " rc;
    if [[ "$rc" =~ [Nn][Oo] ]]; then
        exit 0
    fi
fi
cp ${FROM} ${DEST}
