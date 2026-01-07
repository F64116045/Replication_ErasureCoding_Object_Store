YCSB_BIN="./bin/go-ycsb"
ENDPOINT="http://localhost:8000"
STRATEGY="ec"  
DRIVER="hybridstore"
WORKLOAD_FILE="workloads/workloada"


THREADS=20
REC_COUNT=10000
OPS_COUNT=10000

OUTPUT_FILE="result_ec.txt"
LOAD_LOG="load_ec.log"

echo "=== [2/3] Testing Strategy: Erasure Coding (EC) ==="
echo "Config: Records=$REC_COUNT, Operations=$OPS_COUNT, Threads=$THREADS"


echo "[Phase 1] Loading Data ($REC_COUNT records)..."
$YCSB_BIN load $DRIVER -P $WORKLOAD_FILE \
    -p hybridstore.endpoint=$ENDPOINT \
    -p hybridstore.strategy=$STRATEGY \
    -p recordcount=$REC_COUNT \
    -p threadcount=$THREADS \
    > $LOAD_LOG 2>&1

if [ $? -ne 0 ]; then echo "Load Failed!"; exit 1; fi


echo "[Phase 2] Running Workload ($OPS_COUNT operations)..."
$YCSB_BIN run $DRIVER -P $WORKLOAD_FILE \
    -p hybridstore.endpoint=$ENDPOINT \
    -p hybridstore.strategy=$STRATEGY \
    -p operationcount=$OPS_COUNT \
    -p threadcount=$THREADS \
    -p readproportion=0.5 \
    -p updateproportion=0.5 \
    > $OUTPUT_FILE 2>&1

echo "--------------------------------------"
echo "EC Strategy Benchmarking Done."
echo "Results: $OUTPUT_FILE"
grep -E "OPS:|Avg\(us\):" $OUTPUT_FILE | tail -n 2