#!/bin/bash

source $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/_library.sh

for I in {1..3}; do
	_download_archive_snapshot
done
_archive_snapshot_part
