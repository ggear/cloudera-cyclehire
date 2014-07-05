#!/bin/bash

for (( i=1; i<$#; i++ )); do
	FILES=($(ls -d ${!i}/* 2> /dev/null))
	for FILE in "${FILES[@]}"
	do
	if [ -d $FILE ]; then
		hadoop fs -mkdir -p ${@: -1}/$(basename $FILE) && \
		hadoop fs -put $FILE ${@: -1}/$(basename $FILE) && \
		hadoop fs -touchz ${@: -1}/$(basename $FILE)/_SUCCESS
	fi
	done
done
