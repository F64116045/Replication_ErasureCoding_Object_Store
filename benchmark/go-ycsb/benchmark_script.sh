YCSB_BIN="./bin/go-ycsb"
ENDPOINT="http://localhost:8000"
# replication, ec, field_hybrid
STRATEGY="replication" 
DRIVER="hybridstore"
WORKLOAD_FILE="workloads/workloada"


THREADS=30      
REC_COUNT=100    
OPS_COUNT=10000 


OUTPUT_FILE="Rate1_${STRATEGY}_1500KB.txt"
LOAD_LOG="Rate1_${STRATEGY}_1500KB.log"

echo "=== 策略測試: $STRATEGY ==="
echo "配置: Records=$REC_COUNT, Operations=$OPS_COUNT, Threads=$THREADS"

# 1. Load 階段 (寫入 100 筆資料)
echo "[Phase 1] Loading Data ($REC_COUNT records)..."
$YCSB_BIN load $DRIVER -P $WORKLOAD_FILE \
    -p hybridstore.endpoint=$ENDPOINT \
    -p hybridstore.strategy=$STRATEGY \
    -p recordcount=$REC_COUNT \
    -p threadcount=$THREADS \
    > $LOAD_LOG 2>&1

if [ $? -eq 0 ]; then
    echo "Load Success!"
else
    echo "Load Failed!"
    exit 1
fi


echo "[Phase 2] Running Workload ($OPS_COUNT operations)..."
$YCSB_BIN run $DRIVER -P $WORKLOAD_FILE \
    -p hybridstore.endpoint=$ENDPOINT \
    -p hybridstore.strategy=$STRATEGY \
    -p recordcount=$REC_COUNT \
    -p operationcount=$OPS_COUNT \
    -p threadcount=$THREADS \
    -p readproportion=0.1 \
    -p updateproportion=0.9 \
    > $OUTPUT_FILE 2>&1

echo "--------------------------------------"
echo "測試完成。策略: $STRATEGY"
grep -E "OPS:|Avg\(us\):" $OUTPUT_FILE | tail -n 2