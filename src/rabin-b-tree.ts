import { Hasher, Codec, Storage, CID, encode, Block, parseCID } from './multiformat';
import { nextChunk } from './chunk';
import { findPred, sum } from './helpers';
import { kMaxLength } from 'buffer';

type RabinBTreeNode<K> = {
    leaf:boolean;
    count:number[];
    keys:K[];
    hashes:CID[];
}

type RabinBTreeLevel<K> = {
    start:number;
    end:number;
    count:number[];
    keys:K[];
    hashes:CID[];
}

// range search options
export type RabinBTreeRangeSpec<K> = {
    // start index
    lo?:number;

    // alternatively, start of range
    lt?:K;
    le?:K;

    // end index
    hi?:number;

    // alternatively, end of range
    gt?:K;
    ge?:K;

    // limit
    limit?:number;
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
                counts.slice(),
                keys.slice(),
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
    public async at (root:CID, index:number) : Promise<{ key:K, value: CID }> {
        if (index < 0) {
            throw new Error('out of bounds');
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
                        return { key: block.keys[i], value: block.hashes[i] };
                    } else {
                        cid = block.hashes[i];
                        continue search_loop;
                    }
                }
                ptr -= count;
            }
            throw new Error('out of bounds');
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

    /**
     * Async generator, scans a continuous section of the tree, specified by options.
     * Complexity: O(k + log(n))  where k = number of rows visited
     * 
     * @param root The root node of the array data structure
     * @param options (optional) Configuration elements, determines which part of the array to scan
     * @yields A sequence of array elements in the tree in the range start to end
     */
    public async* scan(root:CID, _options?:RabinBTreeRangeSpec<K>) {
        const options = _options || {};

        // first do a search on the start of the array to initialize the stack
        const stack:{
            index:number;
            hashes:CID[];
            keys:K[];
        }[] = [];

        let count = 'hi' in options ? options.hi : Infinity;
        if ('lt' in options || 'le' in options) {
            let cid = root;
            const key = 'lt' in options ? options.lt : options.le;
            while (true) {
                const block = await this.parseNode(cid);
                const idx = Math.max(findPred(block.keys, key, this.compare), 0);
                count -= sum(block.count, 0, idx);
                stack.push({
                    index: idx,
                    keys: block.keys,
                    hashes: block.hashes,
                });
                if (block.leaf) {
                    break;
                }
                cid = block.hashes[idx];
            }
        } else {
            let cid = root;
            let ptr = Math.max(options.lo || 0, 0);
            count -= ptr;
            search_loop: while (true) {
                const block = await this.parseNode(cid);
                for (let i = 0; i < block.count.length; ++i) {
                    const count = block.count[i];
                    if (ptr < count) {
                        stack.push({
                            index: i,
                            keys: block.keys,
                            hashes: block.hashes,
                        });
                        if (block.leaf) {
                            break search_loop;
                        } else {
                            cid = block.hashes[i];
                            continue search_loop;
                        }
                    }
                    ptr -= count;
                }
                return;
            }
        }

        // handle empty tree
        if (stack[0].hashes.length === 0) {
            return;
        }

        // handle count limits
        if ('limit' in options) {
            count = Math.min(count, options.limit);
        }

        // handle boundary case for lt
        if ('lt' in options) {
            const top = stack[stack.length - 1];
            while (this.compare(top.keys[top.index], options.lt) === 0) {
                top.index += 1;
            }
        }

        // next we start scanning the array
        while (count > 0) {            
            // scan leaf node items
            const top = stack.pop();
            const n = Math.min(count, top.hashes.length - top.index)
            for (let i = 0, ptr = top.index; i < n; ++i) {
                const key = top.keys[ptr];
                if ('gt' in options) {
                    if (this.compare(options.gt, key) >= 0) {
                        break;
                    }
                } else if ('ge' in options) {
                    if (this.compare(options.ge, key) > 0) {
                        break;
                    }
                }
                yield { key, value: top.hashes[ptr] };
                ptr += 1;
            }

            // decrement count and terminate if necessary
            count -= n;
            if (count <= 0) {
                return;
            }

            // walk to next node in stack
            while (true) {
                const top = stack[stack.length - 1];
                top.index += 1;
                if (top.index >= top.hashes.length) {
                    // if we are at the end of this node's sequence then pop it from the stack
                    stack.pop();
                    if (stack.length === 0) {
                        return;
                    }
                } else {
                    // otherwise we pop hashes off recursively
                    let cid = top.hashes[top.index];
                    while (true) {
                        const block = await this.parseNode(cid);
                        stack.push({
                            index: 0,
                            keys: block.keys,
                            hashes: block.hashes,
                        });
                        if (block.leaf) {
                            break;
                        } else {
                            cid = block.hashes[0];
                        }
                    }
                    break;
                }
            }
        }
    }

    private async _levels (root:CID, key:K) : Promise<RabinBTreeLevel<K>[]> {
        // read in levels of the tree as we are splicing into the tree
        const levels:RabinBTreeLevel<K>[] = [];
        {   // scan down bottom of tree and build a stack of level
            let cid = root;
            while (true) {
                const block = await this.parseNode(cid);
                // special case: insert into empty tree
                if (cid === root && block.hashes.length === 0) {
                    return [];
                }
                const idx = findPred(block.keys, key, this.compare);
                const i = Math.max(idx, 0);

                if (block.leaf) {
                    let start = i;
                    if (idx >= 0) {
                        if (this.compare(block.keys[idx], key) === 0) {
                            start = idx;
                        } else {
                            start = idx + 1;
                        }
                    }
                    levels.push(
                        {
                            start,
                            end: idx + 1,
                            count: block.count.slice(),
                            keys: block.keys.slice(),
                            hashes: block.hashes.slice(),
                        },
                        {
                            start: 0,
                            end: 0,
                            count: [],
                            keys: [],
                            hashes: [],
                        });
                    break;
                } else {
                    levels.push({
                        start: i,
                        end: i + 1,
                        count: block.count.slice(),
                        keys: block.keys.slice(),
                        hashes: block.hashes.slice(),
                    });
                    cid = block.hashes[i];
                }
            }
            levels.reverse();
        }
        return levels;
    }

    private async _extend (levels:RabinBTreeLevel<K>[], level:number) : Promise<boolean> {
        if (level === levels.length - 1) {
            return false;
        }
        const parent = levels[level + 1];
        if (parent.end === parent.count.length) {
            if (!await this._extend(levels, level + 1)) {
                return false;
            }
        }
        const cid = parent.hashes[parent.end++];
        const node = await this.parseNode(cid);
        const l = levels[level];
        for (let i = 0; i < node.count.length; ++i) {
            l.count.push(node.count[i]);
            l.keys.push(node.keys[i]);
            l.hashes.push(node.hashes[i]);
        }
        return true;
    }

    // rebuilds a section of a b-tree
    private async _rebuild (levels:RabinBTreeLevel<K>[]) {
        for (let i = 0; i < levels.length; ++i) {
            // retrieve parent level
            let parent:RabinBTreeLevel<K>;
            if (i === levels.length - 1) {
                parent = {
                    start: 0,
                    end: 0,
                    count: [],
                    keys: [],
                    hashes: [],
                };
                levels.push(parent);
            } else {
                parent = levels[i + 1];
            }

            { // insert child nodes into parent hashes
                const start = parent.start;
                const deleteCount = parent.end - parent.start;
                parent.count.splice(start, deleteCount, ...levels[i].count);
                parent.keys.splice(start, deleteCount, ...levels[i].keys);
                parent.hashes.splice(start, deleteCount, ...levels[i].hashes);
            }

            { // recompute parent node hahes
                const nextCount:number[] = [];
                const nextKeys:K[] = [];
                const nextHashes:Promise<CID>[] = [];
    
                for(let lo = 0; lo < parent.count.length; ) {
                    let hi = nextChunk(parent.hashes, lo);
                    while (hi < 0) {
                        if (!await this._extend(levels, i + 1)) {
                            hi = parent.hashes.length;
                            break;
                        }
                        hi = nextChunk(parent.hashes, lo);
                    }
                    nextCount.push(sum(parent.count, lo, hi));
                    nextKeys.push(parent.keys[lo]);
                    nextHashes.push(this.serializeNode(
                        i === 0,
                        parent.count.slice(lo, hi),
                        parent.keys.slice(lo, hi),
                        parent.hashes.slice(lo, hi)));
                    lo = hi;
                }

                parent.count = nextCount;
                parent.keys = nextKeys;
                parent.hashes = await Promise.all(nextHashes);
            }

            // if we are at the top of the tree, terminate
            if (parent === levels[levels.length - 1] && parent.hashes.length <= 1) {
                break;
            }
        }

        // collapse tree levels which are singletons
        let head = levels.pop();
        if (head.hashes.length === 0) {
            return await this.serializeNode(true, [], [], []);
        }
        let result = head.hashes[0];
        while (true) {
            const block = await this.parseNode(result);
            if (block.hashes.length === 1) {
                result = block.hashes[0];
            } else {
                return result;
            }
        }
    }

    /**
     * Updates/inserts a new (key, value) into the given tree
     * Time & space complexity: O(log N)
     * 
     * @param root The root of the tree
     * @param key Key to upsert
     * @param value Value to upsert
     * @return the CID of the root of the new tree
     */
    public async upsert (root:CID, key:K, value:CID) : Promise<CID> {
        const levels = await this._levels(root, key);
        if (levels.length === 0) {
            return this.serializeNode(true, [1], [key], [value]);
        }
        levels[0].count.push(1);
        levels[0].keys.push(key);
        levels[0].hashes.push(value);
        return this._rebuild(levels);
    }

    /**
     * Removes the item with key from the tree
     * Time & space complexity: O(log N)
     * 
     * @param root The root of the tree
     * @param key Key to upsert
     * @return the CID of the root of the new tree
     */
    public async remove (root:CID, key:K) : Promise<CID> {
        const levels = await this._levels(root, key);
        if (levels.length <= 1) {
            return this.serializeNode(true, [], [], []);
        }
        const bottom = levels[1];
        if (bottom.start === bottom.end) {
            return root;
        }
        return this._rebuild(levels);
    }
}
