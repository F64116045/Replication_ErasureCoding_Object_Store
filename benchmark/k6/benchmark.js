import http from 'k6/http';
import { check, sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { SharedArray } from 'k6/data';



const BASE_URL = 'http://localhost:8000';
const MODE = __ENV.MODE || 'replication';
const SIZE_LABEL = __ENV.SIZE || '800kb'; 

// Data size configuration
let COLD_DATA_STRING = '';
if (SIZE_LABEL === '10kb') {
    COLD_DATA_STRING = 'x'.repeat(10 * 1024);
} else if (SIZE_LABEL === '100kb') {
    COLD_DATA_STRING = 'x'.repeat(100 * 1024);
} else {
    COLD_DATA_STRING = 'x'.repeat(800 * 1024);
}

const KEYS_COUNT = 20; 

// Pre-generate keys to reduce CPU overhead during the actual test loop
const PRE_GENERATED_KEYS = new SharedArray('keys', function () {
    let arr = [];
    for (let i = 0; i < KEYS_COUNT; i++) {
        arr.push(`write-bench-${SIZE_LABEL}-${i}-${uuidv4()}`);
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
            exec: MODE === 'replication' ? 'runReplication' : 
                  MODE === 'ec' ? 'runEC' : 'runHybridHot',
        }
    },
    thresholds: { http_req_failed: ['rate<0.05'] },
};

/**
 * Setup: Initial data population (20 objects)
 * Establishes the baseline hash in Etcd for subsequent updates
 */
export function setup() {
    console.log(`[Setup] Mode: ${MODE}, Size: ${SIZE_LABEL}`);
    
    for (let i = 0; i < KEYS_COUNT; i++) {
        let payload = JSON.stringify({ 
            status_flags: 1, 
            description: COLD_DATA_STRING 
        });
        http.post(`${BASE_URL}/write?key=${PRE_GENERATED_KEYS[i]}&strategy=field_hybrid`, payload, params);
        
        // Brief pause to allow WSL2 I/O to stabilize
        sleep(0.5); 
    }
}

export function runReplication() {
    const randomKey = PRE_GENERATED_KEYS[Math.floor(Math.random() * PRE_GENERATED_KEYS.length)];
    const payload = JSON.stringify({ 
        status_flags: Math.floor(Math.random() * 1000), 
        description: COLD_DATA_STRING 
    });
    http.post(`${BASE_URL}/write?key=${randomKey}&strategy=replication`, payload, params);
}

export function runEC() {
    const randomKey = PRE_GENERATED_KEYS[Math.floor(Math.random() * PRE_GENERATED_KEYS.length)];
    const payload = JSON.stringify({ 
        status_flags: Math.floor(Math.random() * 1000), 
        description: COLD_DATA_STRING 
    });
    http.post(`${BASE_URL}/write?key=${randomKey}&strategy=ec`, payload, params);
}

export function runHybridHot() {
    const randomKey = PRE_GENERATED_KEYS[Math.floor(Math.random() * PRE_GENERATED_KEYS.length)];
    
    // Update hot field (status_flags) only; cold field remains identical
    const payload = JSON.stringify({ 
        status_flags: Math.floor(Math.random() * 1000), 
        description: COLD_DATA_STRING 
    });
    
    const res = http.post(`${BASE_URL}/write?key=${randomKey}&strategy=field_hybrid`, payload, params);
    
    // Verify optimization path via response metadata
    check(res, { 
        'Is Pure Hot': (r) => r.status === 200 && (r.json('is_pure_hot_update') === true || r.json('operation_type') === 'Pure Hot Update') 
    });
}