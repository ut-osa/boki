#!/bin/sh

PKGS="g++ clang make cmake pkg-config autoconf automake libtool curl unzip"

apt -y update && \
  apt -y install $PKGS && \
  apt -y clean
