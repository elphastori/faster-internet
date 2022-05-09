#!/bin/bash

export PYTHONPATH="~/Library/Python/3.9/bin"
export PATH="$PATH:$PYTHONPATH"
cd ~/src/faster/runner
pipenv run python speedtest.py >> speedtest-log.txt 2>&1