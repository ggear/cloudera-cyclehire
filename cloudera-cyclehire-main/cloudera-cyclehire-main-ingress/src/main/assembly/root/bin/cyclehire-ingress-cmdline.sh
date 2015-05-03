#!/bin/bash

for (( i=1; i<$#; i++ )); do
	FILES=($(ls -d ${!i}/* 2> /dev/null))
	for FILE in "${FILES[@]}"
	do
	if [ -d "$FILE" ]; then
		$ROOT_DIR/../../bin/cyclehire-shell-hadoop.sh "fs -mkdir -p ${@: -1}/$(basename $FILE)" && \
		$ROOT_DIR/../../bin/cyclehire-shell-hadoop.sh "hadoop fs -put "$FILE" ${@: -1}/$(basename $FILE)" && \
		$ROOT_DIR/../../bin/cyclehire-shell-hadoop.sh "hadoop fs -touchz ${@: -1}/$(basename $FILE)/_SUCCESS"
	fi
	done
done
