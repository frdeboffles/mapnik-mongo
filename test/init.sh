#!/usr/bin/env bash

set -x
(
  mkdir db && cd db
  mongod --dbpath . &
  sleep 5
)
node import.js
node test.js

if [[ -d /opt/test-res ]]; then
  cp 1.png 2.png /opt/test-res
fi