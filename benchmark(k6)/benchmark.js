import http from 'k6/http';
import { check, sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { SharedArray } from 'k6/data';

// ==============================================================================
// 1. Global Configuration
// ==============================================================================

const BASE_URL = 'http://localhost:8000';
const MODE = __ENV.MODE || 'replication';
const SIZE_LABEL = __ENV.SIZE || '800kb'; 

// SIZE settings
let COLD_DATA_STRING = '';
if (SIZE_LABEL === '10kb') {
    COLD_DATA_STRING = 'x'.repeat(10 * 1024);
} else if (SIZE_LABEL === '100kb') {
    COLD_DATA_STRING = 'x'.repeat(100 * 1024);
} else { // default 800kb (avoid Nginx 1MB limit)
    COLD_DATA_STRING = 'x'.repeat(800 * 1024);
}

const KEYS_COUNT = 20; 

const PRE_GENERATED_KEYS = new SharedArray('keys', function () {
    let arr = [];
    for (let i = 0; i < KEYS_COUNT; i++) {
        arr.push(`final-bench-${SIZE_LABEL}-${i}-${uuidv4()}`);
    }
    return arr;
});

const params = { headers: { 'Content-Type': 'application/json' } };

export const options = {
    setupTimeout: '600s',
    scenarios: {
        [MODE]: {
            executor: 'constant-vus',
            vus: 10, 
            duration: '20s',  
            // Note: K6 automatically calls the corresponding function based on exec property
            exec: MODE === 'replication' ? 'runReplication' : 
                  MODE === 'ec' ? 'runEC' : 'runHybridHot',
        }
    },
    thresholds: { http_req_failed: ['rate<0.05'] },
};

// ==============================================================================
// 2. Setup (Only responsible for writing data, no need to return keys)
// ==============================================================================
export function setup() {
    console.log(`[Setup] Mode: ${MODE}, Size: ${SIZE_LABEL} (${COLD_DATA_STRING.length} bytes)`);
    console.log(`[Setup] Pre-populating ${KEYS_COUNT} keys with SLOW delay (1s)...`);
    
    let successCount = 0;
    for (let i = 0; i < KEYS_COUNT; i++) {
        let payload = JSON.stringify({
            like_count: 1,
            description: COLD_DATA_STRING
        });
        
        // Use Hybrid write to establish Metadata uniformly
        let res = http.post(`${BASE_URL}/write?key=${PRE_GENERATED_KEYS[i]}&strategy=field_hybrid`, payload, params);
        
        if (res.status === 200) {
            successCount++;
        } else {
            console.error(`[Setup] Failed key ${i}: ${res.status}`);
        }
        
        // 1s delay to ensure disk IO is drained
        sleep(1); 
    }
    
    console.log(`[Setup] Done. ${successCount}/${KEYS_COUNT} keys ready.`);
}




export function runReplication() {
    const randomKey = PRE_GENERATED_KEYS[Math.floor(Math.random() * PRE_GENERATED_KEYS.length)];
    
    // Sanity check
    if (!randomKey) console.error("Key is undefined!");

    const payload = JSON.stringify({ like_count: Math.floor(Math.random() * 1000), description: COLD_DATA_STRING });
    http.post(`${BASE_URL}/write?key=${randomKey}&strategy=replication`, payload, params);
}

export function runEC() {
    const randomKey = PRE_GENERATED_KEYS[Math.floor(Math.random() * PRE_GENERATED_KEYS.length)];
    
    const payload = JSON.stringify({ like_count: Math.floor(Math.random() * 1000), description: COLD_DATA_STRING });
    http.post(`${BASE_URL}/write?key=${randomKey}&strategy=ec`, payload, params);
}

export function runHybridHot() {
    const randomKey = PRE_GENERATED_KEYS[Math.floor(Math.random() * PRE_GENERATED_KEYS.length)];
    
    const payload = JSON.stringify({ 
        like_count: Math.floor(Math.random() * 1000), 
        description: COLD_DATA_STRING 
    });
    
    // Remove hot_only=true to verify automation logic
    const res = http.post(`${BASE_URL}/write?key=${randomKey}&strategy=field_hybrid`, payload, params);
    
    check(res, { 
        'Is Pure Hot': (r) => r.status === 200 && (r.json('is_pure_hot_update') === true || r.json('operation_type') === 'Pure Hot Update') 
    });
}