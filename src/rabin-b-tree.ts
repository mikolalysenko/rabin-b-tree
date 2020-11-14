import { Hasher, Codec, Storage, CID, encode, Block, parseCID } from './multiformat';
import { nextChunk } from './chunk';
import { findPred, sum } from './helpers';

type RabinBTreeNode<K> = {
    leaf:boolean;
    count:number[];
    keys:K[];
    hashes:CID[];
}

export class RabinBTree<K> {
    constructor (
        public hasher:Hasher,
        public codec:Codec,
        public storage:Storage,
        public compare:(a:K, b:K) => number,
    ) {}

    public async serializeNode (leaf:boolean, counts:number[], keys:K[], hashes:CID[]) {
        const block = await encode({
            value: [
                leaf,
                counts,
                keys,
                hashes.map((h) => h.toString())
            ],
            hasher: this.hasher,
            codec: this.codec,
        });
        await this.storage.put(block);
        return block.cid;
    }

    public async parseNode (cid:CID) : Promise<RabinBTreeNode<K>> {
        const block:Block<any> = await this.storage.get(cid);
        const isLeaf = !!block.value[0];
        const count = block.value[1];
        const keys = block.value[2];
        const hashes = block.value[3];
        if (!Array.isArray(count) ||
            !Array.isArray(keys) ||
            !Array.isArray(hashes) ||
            count.length !== keys.length ||
            count.length !== hashes.length) {
            throw new Error('invalid RabinBTree node ' + cid.toString());
        }
        return {
            leaf: isLeaf,
            count: count.map((c) => c >>> 0),
            keys,
            hashes: hashes.map(parseCID),
        };
    }

    /**
     * Turns a map into a persistent ordered B-Tree
     * Time & space complexity: O(n log(n))
     * 
     * @param data A map of all the data we want to store
     * @returns The CID of the root of the new tree
     */
    public async create (data:Map<K, CID>) : Promise<CID> {
        if (data.size === 0) {
            return await this.serializeNode(true, [], [], []);
        }

        // unpack data into count/keys/hashes
        let prevKeys = Array.from(data.keys());
        prevKeys.sort(this.compare);
        let prevCount = prevKeys.map(() => 1);
        let prevHashes = prevKeys.map((k) => data.get(k));
        let leaf = true;

        do {
            const nextCount:number[] = [];
            const nextKeys:K[] = [];
            const nextHashes:Promise<CID>[] = [];

            for(let lo = 0; lo < prevHashes.length; ) {
                let hi = nextChunk(prevHashes, lo);
                if (hi < 0) {
                    hi = prevHashes.length;
                }
                nextCount.push(sum(prevCount, lo, hi));
                nextKeys.push(prevKeys[lo]);
                nextHashes.push(this.serializeNode(leaf, prevCount.slice(lo, hi), prevKeys.slice(lo, hi), prevHashes.slice(lo, hi)));
                lo = hi;
            }

            prevCount = nextCount;
            prevKeys = nextKeys;
            prevHashes = await Promise.all(nextHashes);
            leaf = false;
        } while (prevHashes.length !== 1)

        return prevHashes[0];
    }

    /**
     * Returns the element at index
     * Time complexity: O(log_B n)
     * 
     * @param root The CID of the list
     * @param index The index of the element we are accessing
     * @returns The CID of the element at the index 
     */
    public async at (root:CID, index:number) : Promise<CID|null> {
        if (index < 0) {
            return null;
        }
        let cid = root;
        let ptr = index;
        search_loop: while (true) {
            const block = await this.parseNode(cid);

            // should replace this scan with interpolation search once we start caching blocks
            // blocks should be stored as a prefix sum instead of an array of counts
            // right now it shouldn't matter much since we still do an O(n) scan per-block when reading from the network
            for (let i = 0; i < block.count.length; ++i) {
                const count = block.count[i];
                if (ptr < count) {
                    if (block.leaf) {
                        return block.hashes[i];
                    } else {
                        cid = block.hashes[i];
                        continue search_loop;
                    }
                }
                ptr -= count;
            }
            return null;
        }
    }

    /**
     * Returns the element mapped to the given key
     * Time complexity: O(log_B n)
     * 
     * @param root The CID of the tree
     * @param key the key we are looking for
     * @returns The CID of the element at the index 
     */
    public async eq (root:CID, key:K) : Promise<CID|null> {
        let cid = root;
        while (true) {
            const block = await this.parseNode(cid);
            const idx = findPred(block.keys, key, this.compare);
            if (idx < 0) {
                return null;
            }
            if (block.leaf) {
                if (this.compare(block.keys[idx], key) === 0) {
                    return block.hashes[idx];
                }
                return null;
            } else {
                cid = block.hashes[idx];
            }
        }
    }

    /**
     * Returns the number of elements in the tree
     * Complexity: O(1)
     * 
     * @param root The root node of the array data structure
     * @returns the number of elements in the tree
     */
    public async size (root:CID) {
        const node = await this.parseNode(root);
        return sum(node.count, 0, node.count.length);
    }

    // /**
    //  * Async generator, scans the array from start to end
    //  * Complexity: O(k + log(n))  where k = end - start
    //  * 
    //  * @param root The root node of the array data structure
    //  * @param start (optional) start index of the region to scan (default is 0)
    //  * @param end (optional) end index of the region to scan (default is end of the array)
    //  * @yields A sequence of array elements in the tree in the range start to end
    //  */
    // public async* range(root:CID, options) {
    //     let count = typeof end === 'undefined' ? Infinity : (end - start);
    //     if (start < 0) {
    //         throw new Error('start index out of bounds');
    //     }
    //     if (count < 0) {
    //         return;
    //     }

    //     // first do a search on the start of the array to initialize the stack
    //     const stack:{
    //         index:number;
    //         hashes:CID[];
    //     }[] = [];
    //     let cid = root;
    //     let ptr = start;
    //     search_loop: while (true) {
    //         const block = await this.parseNode(cid);
    //         for (let i = 0; i < block.count.length; ++i) {
    //             const count = block.count[i];
    //             if (ptr < count) {
    //                 stack.push({
    //                     index: i,
    //                     hashes: block.hashes,
    //                 });
    //                 if (block.leaf) {
    //                     break search_loop;
    //                 } else {
    //                     cid = block.hashes[i];
    //                     continue search_loop;
    //                 }
    //             }
    //             ptr -= count;
    //         }
    //         throw new Error('start index out of bounds');
    //     }

    //     // next we start scanning the array
    //     while (count > 0) {            
    //         // scan leaf node items
    //         const top = stack.pop();
    //         const n = Math.min(count, top.hashes.length - top.index)
    //         for (let i = 0, ptr = top.index; i < n; ++i) {
    //             // TODO: would be more efficient to yield hashes in batches instead of one-by-one
    //             yield top.hashes[ptr++];
    //         }

    //         // decrement count and terminate if necessary
    //         count -= n;
    //         if (count <= 0) {
    //             return;
    //         }

    //         // walk to next node in stack
    //         while (true) {
    //             const top = stack[stack.length - 1];
    //             top.index += 1;
    //             if (top.index >= top.hashes.length) {
    //                 // if we are at the end of this node's sequence then pop it from the stack
    //                 stack.pop();
    //                 if (stack.length === 0) {
    //                     return;
    //                 }
    //             } else {
    //                 // otherwise we pop hashes off recursively
    //                 let cid = top.hashes[top.index];
    //                 while (true) {
    //                     const block = await this.parseNode(cid);
    //                     stack.push({
    //                         index: 0,
    //                         hashes: block.hashes,
    //                     });
    //                     if (block.leaf) {
    //                         break;
    //                     } else {
    //                         cid = block.hashes[0];
    //                     }
    //                 }
    //                 break;
    //             }
    //         }
    //     }
    // }

    // TODO:
    //  * upsert
    //  * remove
    //  * range scan
    //  * search (le, lt, ge, gt)
    //  * successor/predecessor scan
}
