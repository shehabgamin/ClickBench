#!/bin/bash

export SAIL_PARQUET__BINARY_AS_STRING="true"
export SAIL_PARQUET__PUSHDOWN_FILTERS="true"
export SAIL_PARQUET__REORDER_FILTERS="true"
export SAIL_RUNTIME__ENABLE_SECONDARY="true"
export RUST_LOG=off
export PYTHONWARNINGS=ignore

/Users/r/Desktop/lakesail/sail/target/release/sail spark server &
SAIL_PID=$!
echo "SAIL_PID=$SAIL_PID"

sleep 2

cat queries.sql | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    ./query.py <<< "${query}"
done

kill $SAIL_PID