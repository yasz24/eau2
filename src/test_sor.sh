#!/bin/bash

expected="$1"
shift
output=$(./sorer $@)

if [ "$expected" == "$output" ]; then
    echo Success
    exit 0
else
    echo "Failed"
    echo "Expected: $expected"
    echo "Output: $output"
    exit 1
fi
