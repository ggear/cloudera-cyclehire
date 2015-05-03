#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/cyclehire.env

$ROOT_DIR/lib/process/bin/cyclehire-process-database.sh
$ROOT_DIR/bin/cyclehire-ingest.sh
$ROOT_DIR/bin/cyclehire-rewrite.sh
$ROOT_DIR/lib/process/bin/cyclehire-process-refresh.sh
