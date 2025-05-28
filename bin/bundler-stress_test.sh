#!/bin/bash

## Rund BundlerStressTest, writes results to $OUTPUT (removes it first, when started)

## The bundler to be tested
BUNDLERS="no-bundler transfer-queue per-destination"
OUTPUT="bst.txt"
THREADS="1 8 100"
PROPS="$HOME/tcp.xml"
PGM=BundlerStressTest

rm -f $OUTPUT
for i in $BUNDLERS;
  do for j in $THREADS;
    do jt $PGM -props $PROPS -bundler $i -num_sender_threads $j -interactive false -time 60 -warmup 60 >> $OUTPUT;
    done;
  done;

