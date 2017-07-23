#!/bin/bash
AUTHKEY=$1
if [ -z $AUTHKEY ]; then
    echo "please specify the public key as the argument"
    exit 0
fi

cat $AUTHKEY >> ~/.ssh/authorized_keys
echo "$AUTHKEY was added into ~/.bash/authorized_keys"
