import http from 'k6/http';
import { check, sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { SharedArray } from 'k6/data';


const BASE_URL = 'http://localhost:8000';
const STRATEGY = __ENV.STRATEGY || 'field_hybrid'; 
const SIZE_LABEL = __ENV.SIZE || '800kb';
const KEYS_COUNT = 30; 

// Configure data size for the benchmark
const COLD_DATA_STRING = 'x'.repeat(
    SIZE_LABEL === '10kb' ? 10240 : (SIZE_LABEL === '100kb' ? 102400 : 819200)
);

const PRE_GENERATED_KEYS = new SharedArray('keys', function () {
    let arr = [];
    for (let i = 0; i < KEYS_COUNT; i++) {
        // Tag keys with strategy for easier tracking in metadata/logs
        arr.push(`read-bench-${STRATEGY}-${SIZE_LABEL}-${i}-${uuidv4()}`);
    }
    return arr;
});

const params = { headers: { 'Content-Type': 'application/json' } };

export const options = {
    setupTimeout: '600s',
    scenarios: {
        read_test: {
            executor: 'constant-vus',
            vus: 10,
            duration: '30s',
        }
    },
    // Threshold: 95% of requests should complete under 1.5s
    thresholds: { 'http_req_duration': ['p(95)<1500'] },
};

/**
 * Setup: Pre-populates the cluster with data using the target strategy.
 */
export function setup() {
    console.log(`[Setup] Preparing ${KEYS_COUNT} objects using ${STRATEGY} (${SIZE_LABEL})...`);
    
    let success = 0;
    for (let i = 0; i < KEYS_COUNT; i++) {
        let payload = JSON.stringify({ 
            id: i, 
            description: COLD_DATA_STRING 
        });
        
        let res = http.post(
            `${BASE_URL}/write?key=${PRE_GENERATED_KEYS[i]}&strategy=${STRATEGY}`, 
            payload, 
            params
        );
        
        if (res.status === 200) success++;
        // Throttling to prevent saturating WSL2 I/O during setup
        sleep(0.2); 
    }
    console.log(`[Setup] Done. ${success}/${KEYS_COUNT} objects ready for reading.`);
}

export default function () {
    const randomKey = PRE_GENERATED_KEYS[Math.floor(Math.random() * PRE_GENERATED_KEYS.length)];
    const res = http.get(`${BASE_URL}/read/${randomKey}`);
    
    check(res, {
        'status is 200': (r) => r.status === 200,
        'data integrity verified': (r) => r.json('description') && r.json('description').length >= COLD_DATA_STRING.length
    });
}