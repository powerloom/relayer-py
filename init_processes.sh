#!/bin/bash

echo 'starting queues';
poetry run python -m init_rabbitmq

sleep 3

echo 'starting processes...';
pm2 start pm2.config.js

# Waiting for other processes to start
echo 'waiting for processes to start..';
sleep 10

echo 'processes started';
pm2 logs --lines 1000
