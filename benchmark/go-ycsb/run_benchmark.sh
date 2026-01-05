
DB_ENDPOINT="http://localhost:8000"
THREADS=50
RECORD_COUNT=10000
OP_COUNT=50000
WORKLOAD_FILE="workloads/workload_hybrid"


STRATEGY=${1:-"field_hybrid"}


OUTPUT_DIR="results/${STRATEGY}"
mkdir -p "$OUTPUT_DIR"

echo "=================================================="
echo "   開始測試 (Strategy: $STRATEGY)"
echo "   Endpoint: $DB_ENDPOINT"
echo "   Threads:  $THREADS"
echo "   Records:  $RECORD_COUNT"
echo "   Output:   $OUTPUT_DIR"
echo "=================================================="


if [ ! -f "$WORKLOAD_FILE" ]; then
    echo " 建立 Workload 檔案..."
    cat <<EOF > "$WORKLOAD_FILE"
recordcount=$RECORD_COUNT
operationcount=$OP_COUNT
workload=core
fieldcount=10
fieldlength=100
requestdistribution=zipfian
measurementtype=histogram
histogram.buckets=2000
EOF
fi


echo ""
echo " [Phase 1] Loading Data..."
./bin/go-ycsb load hybridstore -P "$WORKLOAD_FILE" \
    -p hybridstore.endpoint="$DB_ENDPOINT" \
    -p hybridstore.strategy="$STRATEGY" \
    -p threadcount="$THREADS" \
    > "$OUTPUT_DIR/load.log" 2>&1

if [ $? -eq 0 ]; then
    echo " Load 完成！"
else
    echo " Load 失敗，請檢查 Server 是否開啟或 Log 內容。"
    tail -n 10 "$OUTPUT_DIR/load.log"
    exit 1
fi

# 2. Run Phase - Read Heavy (95% Read, 5% Update)
echo ""
echo " [Phase 2] Running Read-Heavy Benchmark (95/5)..."
./bin/go-ycsb run hybridstore -P "$WORKLOAD_FILE" \
    -p hybridstore.endpoint="$DB_ENDPOINT" \
    -p hybridstore.strategy="$STRATEGY" \
    -p threadcount="$THREADS" \
    -p readproportion=0.95 \
    -p updateproportion=0.05 \
    -p scanproportion=0 \
    -p insertproportion=0 \
    -p measurement.output_file="$OUTPUT_DIR/result_read_heavy.json" \
    > "$OUTPUT_DIR/run_read_heavy.log" 2>&1

echo " Read-Heavy 完成！數據已存入 $OUTPUT_DIR/result_read_heavy.json"

# 3. Run Phase - Write Heavy (50% Read, 50% Update)
echo ""
echo " [Phase 3] Running Write-Heavy Benchmark (50/50)..."
./bin/go-ycsb run hybridstore -P "$WORKLOAD_FILE" \
    -p hybridstore.endpoint="$DB_ENDPOINT" \
    -p hybridstore.strategy="$STRATEGY" \
    -p threadcount="$THREADS" \
    -p readproportion=0.5 \
    -p updateproportion=0.5 \
    -p measurement.output_file="$OUTPUT_DIR/result_write_heavy.json" \
    > "$OUTPUT_DIR/run_write_heavy.log" 2>&1

echo " Write-Heavy 完成, 數據已存入 $OUTPUT_DIR/result_write_heavy.json"

echo ""
echo " 所有測試結束, 查看 $OUTPUT_DIR 資料夾內的 JSON 報告。"