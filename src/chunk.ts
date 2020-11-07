// We do the chunking in two stages
// 1.  First we hash each data pointer to get some more manageable size keys
// 2.  Then we loop over the hashes and do rabin finger printing with them
//
// Uses a crappy version of the gear FastCDC method from Wikipedia
// We assume that the hashed values are already more-or-less random so skip using the gear table.

import { CID } from './multiformat';

const MIN_SIZE = 64;
const MAX_SIZE = 1024;

// modified from paper to use fewer bits -> smaller window
const MASK_HI = 0x88000000;
const MASK_LO = 0x03000000;

const UINT32_MASK = ~0;

// just take last 4 bytes of cid for hash
function gear (cid:CID) {
    const bytes = cid.bytes;
    return bytes[bytes.length - 1] + (bytes[bytes.length - 2] << 8) + (bytes[bytes.length - 3] << 16) + (bytes[bytes.length - 4] << 24);
}

export function nextChunk (data:CID[], start:number) {
    const n = Math.min(data.length - start, MAX_SIZE);
    if (n < MIN_SIZE) {
        return data.length;
    }
    let ptr = start;
    let flo = 0;
    let fhi = 0;
    for (let i = 0; i < MIN_SIZE; ++i) {
        const x = ((flo << 1) >>> 0) + gear(data[ptr++]);
        fhi = (((fhi << 1) >>> 0) + (x > UINT32_MASK ? 1 : 0)) >>> 0;
        flo = x >>> 0;
    }
    for (let i = MIN_SIZE; i < n; ++i) {
        const x = ((flo << 1) >>> 0) + gear(data[ptr++]);
        fhi = (((fhi << 1) >>> 0) + (x > UINT32_MASK ? 1 : 0)) >>> 0;
        flo = x >>> 0;
        if (((MASK_HI & fhi) === 0) && ((MASK_LO & flo) === 0)) {
            return ptr;
        }
    }
    return ptr;
}