YCSB_BIN="./bin/go-ycsb"
ENDPOINT="http://localhost:8000"
STRATEGY="replication"
DRIVER="hybridstore"
WORKLOAD_FILE="workloads/workloada"


THREADS=20
REC_COUNT=10000
OPS_COUNT=10000

OUTPUT_FILE="result_replication.txt"
LOAD_LOG="load_replication.log"

echo "=== [1/3] Testing Strategy: $STRATEGY ==="
echo "Config: Records=$REC_COUNT, Operations=$OPS_COUNT, Threads=$THREADS"
echo "確保 Docker 服務已手動啟動且上次測試之 Volume 已清理。"

# 1. Load 階段 (寫入初始 10,000 筆資料)
echo "[Phase 1] Loading Data ($REC_COUNT records)..."
$YCSB_BIN load $DRIVER -P $WORKLOAD_FILE \
    -p hybridstore.endpoint=$ENDPOINT \
    -p hybridstore.strategy=$STRATEGY \
    -p recordcount=$REC_COUNT \
    -p threadcount=$THREADS \
    > $LOAD_LOG 2>&1

# 檢查 Load 是否成功
if [ $? -eq 0 ]; then
    echo "Load Success! Results in $LOAD_LOG"
else
    echo "Load Failed! 請檢查日誌 $LOAD_LOG"
    exit 1
fi

# 2. Run 階段 (執行 10,000 次讀寫測試)
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
echo "Benchmarking Done."
echo "Final Results: $OUTPUT_FILE"
grep -E "OPS:|Avg\(us\):" $OUTPUT_FILE | tail -n 2