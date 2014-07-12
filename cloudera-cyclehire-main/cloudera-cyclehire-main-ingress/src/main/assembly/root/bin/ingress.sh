#!/bin/bash

hadoop jar $1/lib/ingest/lib/*.jar com.cloudera.cyclehire.main.ingress.copy.CopyDriver \
	-Ddir.include=false -Dblock.single=true -Dtimeout.secs=1000 \
	-Dthread.number=3 -Dthread.queue=file -Dio.file.buffer.size=16384 $2 $3
