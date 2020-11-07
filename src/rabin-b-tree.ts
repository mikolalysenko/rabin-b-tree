import { nextChunk } from './chunk';
import { Hasher, Codec, CID, encode, Block, parseCID } from './multiformat';
import { Storage } from './storage';

function sum (x:number[], start:number, end:number) {
    let r = 0;
    for (let i = start; i < end; ++i) {
        r += x[i];
    }
    return r;
}

export class RabinBTree {
    constructor (
        public hasher:Hasher,
        public codec:Codec,
        public storage:Storage,
    ) {}

    // TODO: replace this with a more efficient encoding
    public async serializeNode (counts:number[], hashes:CID[]) {
        const block = await encode({
            value: [
                counts,
                hashes.map((h) => h.toString())
            ],
            hasher: this.hasher,
            codec: this.codec,
        });
        await this.storage.put(block);
        return block.cid;
    }

    // TODO: again, do something not so stupid here
    public async parseNode (cid:CID) {
        const block:Block<Uint8Array> = await this.storage.get(cid);
        const count = block.value[0];
        const hashes = block.value[1];
        if (!Array.isArray(count) || !Array.isArray(hashes) || count.length !== hashes.length) {
            throw new Error('invalid b-tree node ' + cid.toString());
        }
        return {
            count: count.map((c) => c >>> 0),
            hashes: hashes.map(parseCID),
        };
    }

    // simple bottom up tree construction
    public async create (hashes:CID[]) {
        let prevCount = hashes.map(() => 1);
        let prevHashes = hashes;

        while (prevHashes.length !== 1) {
            const nextCount:number[] = [];
            const nextHashes:Promise<CID>[] = [];

            for(let start = 0; start < prevHashes.length; ) {
                const end = nextChunk(prevHashes, start);
                nextCount.push(sum(prevCount, start, end));
                nextHashes.push(this.serializeNode(prevCount.slice(start, end), prevHashes.slice(start, end)));
                start = end;
            }

            prevCount = nextCount;
            prevHashes = await Promise.all(nextHashes);
        }

        return prevHashes[0];
    }

    // access element at position i
    public async query (node:CID, _index:number) {
        if (_index < 0) {
            throw new Error('out of bounds');
        }
        // should replace this scan with interpolation search once we start caching blocks
        // right now it shouldn't matter much since we still do an O(n) scan per-block when reading from the network
        const block = await this.parseNode(node);
        let index = _index;
        for (let i = 0; i < block.count.length; ++i) {
            const count = block.count[i];
            if (index < count) {
                if (count === 1) {
                    return block.hashes[i];
                } else {
                    return this.query(block.hashes[i], index);
                }
            }
            index -= count;
        }
        throw new Error('out of bounds');
    }
}
