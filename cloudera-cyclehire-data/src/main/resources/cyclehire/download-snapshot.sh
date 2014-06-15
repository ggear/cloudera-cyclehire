#!/bin/bash

source $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/_library.sh

_download_snapshot
_archive_snapshot_full
