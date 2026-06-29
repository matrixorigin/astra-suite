#!/bin/bash
# MatrixOne entrypoint with file logging.

mkdir -p /mo-logs

/mo-service -launch /etc/quickstart/launch.toml -debug-http=:6060 2>&1 | tee -a /mo-logs/matrixone.log
