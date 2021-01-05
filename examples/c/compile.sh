#!/bin/bash

CC=gcc

$CC -shared -fPIC -O2 -I../../worker/cpp/include -o libfoo.so foo.c
$CC -shared -fPIC -O2 -I../../worker/cpp/include -o libbar.so bar.c
