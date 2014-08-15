#!/bin/bash

DATA_SET=cyclehire/warehouse
DATA_SET_FORMAT=xml
DATA_SET_TAR_NUM=4000
DATA_SET_TAR_PART=_PART
DATA_SET_NAME=livecyclehireupdates
DATA_SET_URL=http://www.tfl.gov.uk/tfl/syndication/feeds/cycle-hire/livecyclehireupdates.xml
DATA_SET_ARCHIVE_URL=http://54.77.57.3/tmph3l7m2vv103/data-repos

DIR_BASE=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../../../../data/$DATA_SET
if [ $# -gt 0 ]; then
	DIR_BASE=$1
fi

function _download_archive() {
	cd $DIR_BASE/tar
	rm -rvf *$DATA_SET_TAR_PART.tar.gz
	wget -e robots=off -N -np -nH -nd -r -l 1 -A .tar.gz $DATA_SET_ARCHIVE_URL/$DATA_SET/tar
}

function _download_archive_snapshot() {
	_download_archive
	cd $DIR_BASE/$DATA_SET_FORMAT
	rm -rvf *.$DATA_SET_FORMAT
	wget -e robots=off -N -np -nH -nd -r -l 1 -A .$DATA_SET_FORMAT $DATA_SET_ARCHIVE_URL/$DATA_SET/$DATA_SET_FORMAT
}

function _download_snapshot() {
	cd $DIR_BASE/$DATA_SET_FORMAT
	wget -T 10 -t 1 -O $(date +%s)_$DATA_SET_NAME.$DATA_SET_FORMAT $DATA_SET_URL
}

function _archive_snapshot() {
	cd $DIR_BASE/$DATA_SET_FORMAT
	if [ $(ls | wc -l) -gt 0 ]; then
		mkdir ../tar/stage
		mv * ../tar/stage
		cd ../tar/stage
		tar cvf $(ls -1 | head -n 1 | cut -d"_" -f1)_$(ls -1 | tail -n 1 | cut -d"_" -f1)_$DATA_SET_NAME$1.tar *
		gzip *.tar
		mv *.gz ..
		cd ..
		rm -rf stage
	fi
}

function _archive_snapshot_part() {
	cd $DIR_BASE/$DATA_SET_FORMAT
	_archive_snapshot $DATA_SET_TAR_PART
}

function _archive_snapshot_full() {
	cd $DIR_BASE/$DATA_SET_FORMAT
	if [ $(ls | wc -l) -ge $DATA_SET_TAR_NUM ]; then
		_archive_snapshot ""
	fi
}
