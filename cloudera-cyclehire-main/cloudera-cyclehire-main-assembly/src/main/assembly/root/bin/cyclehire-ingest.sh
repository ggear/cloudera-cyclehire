#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/cyclehire.env

$ROOT_DIR/lib/process/bin/cyclehire-process-database.sh
$ROOT_DIR/lib/ingress/bin/cyclehire-ingress.sh
$ROOT_DIR/lib/process/bin/cyclehire-process.sh
$ROOT_DIR/lib/process/bin/cyclehire-process-table.sh
$ROOT_DIR/lib/process/bin/cyclehire-process-rewrite.sh
