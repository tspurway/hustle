#!/bin/sh

set -e

PYTHON=python
CONFIG_DIR=/etc/hustle
FROM=${PWD}/settings.yaml
DEST=${CONFIG_DIR}/settings.yaml

cd deps && make install
${PYTHON} setup.py install

if [[ ! -d ${CONFIG_DIR} ]]; then
    mkdir ${CONFIG_DIR}
elif [[ -f ${DEST} ]]; then
    read -p "Settings file already exists, overwrite it? [Yes/No]: " rc;
    if [[ "$rc" =~ [Nn][Oo] ]]; then
        exit 0
    fi
fi
cp ${FROM} ${DEST}
