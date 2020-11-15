import { Hasher, Codec, Storage, CID, encode, Block, parseCID } from './multiformat';
import { nextChunk } from './chunk';
import { sum } from './helpers';

type RabinListNode = {
    leaf:boolean;
    count:number[];
    hashes:CID[];
}

type RabinListLevel = {
    start:number;
    end:number;
    count:number[];
    hashes:CID[];
}

type RabinListRangeSpec = {
    lo?:number;
    hi?:number;
    limit?:number;
}

export class RabinList {
    constructor (
        public hasher:Hasher,
        public codec:Codec,
        public storage:Storage,
    ) {}

    // TODO: replace this with a more efficient encoding
    public async serializeNode (leaf:boolean, counts:number[], hashes:CID[]) {
        const block = await encode({
            value: [
                leaf,
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
    public async parseNode (cid:CID) : Promise<RabinListNode> {
        const block:Block<any> = await this.storage.get(cid);
        const isLeaf = !!block.value[0];
        const count = block.value[1];
        const hashes = block.value[2];
        if (!Array.isArray(count) || !Array.isArray(hashes) || count.length !== hashes.length) {
            throw new Error('invalid RabinList node ' + cid.toString());
        }
        return {
            leaf: isLeaf,
            count: count.map((c) => c >>> 0),
            hashes: hashes.map(parseCID),
        };
    }

    /**
     * Creates a rabin list from an array of CIDs
     * Time & space complexity: O(n log(n))
     * 
     * @param hashes The ordered array of all CIDs which we are inserting into the tree
     * @returns The CID of the root of the new tree
     */
    public async create (hashes:CID[]) : Promise<CID> {
        if (hashes.length === 0) {
            return await this.serializeNode(true, [], []);
        }

        let prevCount = hashes.map(() => 1);
        let prevHashes = hashes;
        let leaf = true;

        do {
            const nextCount:number[] = [];
            const nextHashes:Promise<CID>[] = [];

            for(let lo = 0; lo < prevHashes.length; ) {
                let hi = nextChunk(prevHashes, lo);
                if (hi < 0) {
                    hi = prevHashes.length;
                }
                nextCount.push(sum(prevCount, lo, hi));
                nextHashes.push(this.serializeNode(leaf, prevCount.slice(lo, hi), prevHashes.slice(lo, hi)));
                lo = hi;
            }

            prevCount = nextCount;
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
    public async at (root:CID, index:number) : Promise<CID> {
        if (index < 0) {
            throw new Error('rabin-list: index out of bounds');
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
            throw new Error('rabin-array: index out of bounds');
        }
    }

    /**
     * Performs a splice() operation on a list, similiar to JavaScript's Array.splice()
     * With splice you can implement whatever updates you want (push/pop/set etc.)
     * 
     * Time & space complexity: O(k log_B (n + k)), where k = size of the splice in deletes + items
     * 
     * @param root The root of the array object
     * @param start The start index of the splice
     * @param deleteCount The number of items to delete from the array starting at index
     * @param items A list of CIDs to insert into the tree
     * @returns The CID of the root of the new tree
     */
    public async splice (root:CID, start:number, deleteCount:number=0, ...items:CID[]) : Promise<CID> {
        if (start < 0) {
            throw new Error('rabin array: index out of bounds');
        }

        // methods for rabin-array
        const rl = this;

        // read in levels of the tree as we are splicing into the tree
        const levels:RabinListLevel[] = [];
        {   // scan down bottom of tree and build a stack of level
            let cid = root;
            let ptr = start;
            search_loop: while (true) {
                const block = await rl.parseNode(cid);
                // special case: insert into empty tree
                if (cid === root && block.hashes.length === 0) {
                    return this.create(items);
                }
                for (let i = 0; i < block.count.length; ++i) {
                    const c = block.count[i];
                    if (ptr <= c) {
                        levels.push({
                            start: i + (ptr === c && block.leaf ? 1 : 0),
                            end: i + 1,
                            count: block.count.slice(),
                            hashes: block.hashes,
                        });
                        if (block.leaf) {
                            levels.push({
                                start: 0,
                                end: 0,
                                count: items.map(() => 1),
                                hashes: items,
                            });
                            break search_loop;
                        } else {
                            cid = block.hashes[i];
                            continue search_loop;
                        }
                    } else {
                        ptr -= c;
                    }
                }
                throw new Error('rabin array: index out of bounds');
            }
            levels.reverse();
        }

        // add an extra block full of hashes at the specified level
        async function extendLevel (level:number) {
            if (level === levels.length - 1) {
                return false;
            }
            const parent = levels[level + 1];
            if (parent.end === parent.count.length) {
                if (!await extendLevel(level + 1)) {
                    return false;
                }
            }
            const cid = parent.hashes[parent.end++];
            const node = await rl.parseNode(cid);
            const l = levels[level];
            for (let i = 0; i < node.count.length; ++i) {
                l.count.push(node.count[i]);
                l.hashes.push(node.hashes[i]);
            }
            return true;
        }

        { // shift in a bottom level for the main splice
            const bottom = levels[1];
            bottom.end = bottom.start + deleteCount;

            // extend bottom array
            while (bottom.count.length < bottom.end) {
                if (!await extendLevel(1)) {
                    bottom.end = bottom.count.length;
                    break;
                }
            }
        }

        // now rebuild tree, scanning from bottom up
        for (let i = 0; i < levels.length; ++i) {
            // retrieve parent level
            let parent:RabinListLevel;
            if (i === levels.length - 1) {
                parent = {
                    start: 0,
                    end: 0,
                    count: [],
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
                parent.hashes.splice(start, deleteCount, ...levels[i].hashes);
            }

            { // recompute parent node hahes
                const nextCount:number[] = [];
                const nextHashes:Promise<CID>[] = [];
    
                for(let lo = 0; lo < parent.count.length; ) {
                    let hi = nextChunk(parent.hashes, lo);
                    while (hi < 0) {
                        if (!await extendLevel(i + 1)) {
                            hi = parent.hashes.length;
                            break;
                        }
                        hi = nextChunk(parent.hashes, lo);
                    }
                    nextCount.push(sum(parent.count, lo, hi));
                    nextHashes.push(this.serializeNode(i === 0, parent.count.slice(lo, hi), parent.hashes.slice(lo, hi)));
                    lo = hi;
                }

                parent.hashes = await Promise.all(nextHashes);
                parent.count = nextCount;
            }

            // if we are at the top of the tree, terminate
            if (parent === levels[levels.length - 1] && parent.hashes.length <= 1) {
                break;
            }
        }

        // collapse tree levels which are singletons
        let head = levels.pop();
        if (head.hashes.length === 0) {
            return await this.serializeNode(true, [], []);
        }
        let result = head.hashes[0];
        while (true) {
            const b = await this.parseNode(result);
            if (b.hashes.length !== 1) {
                return result;
            }
            result = b.hashes[0];
        }
    }

    /**
     * Returns the length of the list
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
     * Async generator, scans the array from start to end
     * Complexity: O(k + log(n))  where k = end - start
     * 
     * @param root The root node of the array data structure
     * @param options (optional) start index of the region to scan (default is 0)
     * @yields A sequence of array elements in the tree in the range start to end
     */
    public async* scan(root:CID, options?:RabinListRangeSpec) {
        const start = Math.max('lo' in options ? options.lo : 0, 0);
        const end = 'hi' in options ? options.hi : Infinity;
        let count = Math.min(end - start, 'limit' in options ? options.limit : Infinity);
        if (count < 0) {
            return;
        }

        // first do a search on the start of the array to initialize the stack
        const stack:{
            index:number;
            hashes:CID[];
        }[] = [];
        let cid = root;
        let ptr = start;
        search_loop: while (true) {
            const block = await this.parseNode(cid);
            for (let i = 0; i < block.count.length; ++i) {
                const count = block.count[i];
                if (ptr < count) {
                    stack.push({
                        index: i,
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

        // next we start scanning the array
        while (count > 0) {            
            // scan leaf node items
            const top = stack.pop();
            const n = Math.min(count, top.hashes.length - top.index)
            for (let i = 0, ptr = top.index; i < n; ++i) {
                // TODO: would be more efficient to yield hashes in batches instead of one-by-one
                yield top.hashes[ptr++];
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
}
