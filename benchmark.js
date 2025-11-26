import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { SharedArray } from 'k6/data';



const BASE_URL = 'http://localhost:8000';
const MODE = __ENV.MODE || 'replication';


const KEYS_COUNT = 20; 
const PRE_GENERATED_KEYS = new SharedArray('keys', function () {
    let arr = [];
    for (let i = 0; i < KEYS_COUNT; i++) {
        arr.push(`big-bench-${i}-${uuidv4()}`);
    }
    return arr;
});


const COLD_DATA_STRING = `x`.repeat(800 * 1024); 
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

    thresholds: { http_req_failed: ['rate<0.10'] },
};


export function setup() {
    console.log(`[Setup] Pre-populating ${KEYS_COUNT} keys with 800KB payload...`);
    
    let successCount = 0;
    for (let i = 0; i < KEYS_COUNT; i++) {

        let payload = JSON.stringify({
            like_count: 1,
            description: COLD_DATA_STRING
        });
        
        let res = http.post(`${BASE_URL}/write?key=${PRE_GENERATED_KEYS[i]}&strategy=field_hybrid`, payload, params);
        if (res.status === 200) {
            successCount++;
        } else {
            console.error(`[Setup] Failed key ${i}: Status ${res.status}`);
        }
        

        if (i % 5 === 0) console.log(`[Setup] Wrote ${i}/${KEYS_COUNT}...`);
    }
    
    console.log(`[Setup] Done. ${successCount}/${KEYS_COUNT} keys ready.`);
    return PRE_GENERATED_KEYS;
}



export function runReplication(keys) {
    const key = keys[Math.floor(Math.random() * keys.length)];
    const payload = JSON.stringify({ like_count: Math.floor(Math.random() * 1000), description: COLD_DATA_STRING });
    http.post(`${BASE_URL}/write?key=${key}&strategy=replication`, payload, params);
}

export function runEC(keys) {
    const key = keys[Math.floor(Math.random() * keys.length)];
    const payload = JSON.stringify({ like_count: Math.floor(Math.random() * 1000), description: COLD_DATA_STRING });
    http.post(`${BASE_URL}/write?key=${key}&strategy=ec`, payload, params);
}

export function runHybridHot(keys) {
    const key = keys[Math.floor(Math.random() * keys.length)];
    
    // Hybrid Hot: 
    const payload = JSON.stringify({ 
        like_count: Math.floor(Math.random() * 1000), 
        description: COLD_DATA_STRING 
    });
    
    const res = http.post(`${BASE_URL}/write?key=${key}&strategy=field_hybrid&hot_only=true`, payload, params);
    
    check(res, { 'Is Pure Hot': (r) => r.status === 200 && (r.json('is_pure_hot_update') === true || r.json('operation_type') === 'Pure Hot Update') });
}