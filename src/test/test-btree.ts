import tape = require('tape');
import { CID } from '../multiformat';
import { RabinBTree } from '../rabin-b-tree';
import { DEFAULT_FORMATS, encodeJSON, inspectBTree } from "./helpers";

const BTREE = new RabinBTree<string>(DEFAULT_FORMATS.hasher, DEFAULT_FORMATS.codec, DEFAULT_FORMATS.storage, (a, b) => {
    if (a < b) {
        return -1;
    } else if (a === b) {
        return 0;
    }
    return 1;
});

tape('empty tree', async (t) => {
    const empty = await BTREE.create(new Map<string, CID>());
    console.log(empty);

    const tree = await inspectBTree(BTREE, empty);
    // console.log(tree);

    t.same(tree.leaf, true, 'leaf ok');
    t.same(tree.count, [], 'count ok');
    t.same(tree.keys, [], 'keys ok');
    t.same(tree.children, [], 'children ok');

    t.end();
});

tape('query test', async (t) => {
    const N = 1e4;
    const T = 1e2;

    // first create a bunch of random strings
    const data:string[] = [];
    for (let i = 0; i < N; ++i) {
        let x = '';
        for (let j = 0; j < 100; ++j) {
            x += Math.random().toString(36).slice(2);
        }
        data.push(x);
    }

    // then store the CIDs
    const dataCIDs = await Promise.all(data.map((value) => encodeJSON({
        value,
        ...DEFAULT_FORMATS
    })));

    // finally create map
    const map = new Map<string,CID>();
    dataCIDs.forEach((cid, idx) => {
        map.set('key:' + idx, cid);
    })

    // create a tree
    const root = await BTREE.create(map);
    console.log(await inspectBTree(BTREE, root));

    t.equals(await BTREE.size(root), N, 'size is ok');

    // now run some random index tests
    for (let i = 0; i < T; ++i) {
        const index = Math.floor(N * Math.random());
        const key = 'key:' + index;
        const node = await BTREE.eq(root, key);
        t.equals(node.toString(), map.get(key).toString(), 'test map search: ' + key);
    }

    t.end();
});

tape('scan test', async (t) => {
    const N = 1e4;

    // first create a bunch of random strings
    const data:string[] = [];
    for (let i = 0; i < N; ++i) {
        data.push('ppp' + i);
    }
    data.sort();
    const dataCIDs = await Promise.all(data.map((value) => encodeJSON({
        value,
        ...DEFAULT_FORMATS
    })));
    const map = new Map<string, CID>();
    dataCIDs.forEach((cid, k) => map.set(data[k], cid));

    // create a tree
    const root = await BTREE.create(map);
    
    async function testScan (start:number, end:number) {
        let ptr = start;
        for await (const { key, value } of BTREE.scan(root, { lo: start, hi: end })) {
            t.equals(key, data[ptr], 'scan key: ' + ptr);
            t.equals(value.toString(), dataCIDs[ptr].toString(), 'scan value: ' + ptr)
            ptr += 1;
        }
        t.equals(ptr, Math.min(end, N), 'scan returned expected number of elements');
    }

    // scan full array
    await testScan(0, Infinity);
    await testScan(500, 3000);

    t.end();
});

tape('upsert/remove test', async (t) => {
    const N = 1e4;

    // first create a bunch of random strings
    const data:string[] = [];
    for (let i = 0; i < N; ++i) {
        data.push('ppp' + i);
    }
    data.sort();
    const dataCIDs = await Promise.all(data.map((value) => encodeJSON({
        value,
        ...DEFAULT_FORMATS
    })));
    const map = new Map<string, CID>();
    dataCIDs.forEach((cid, k) => map.set(data[k], cid));

    // create a tree
    const root = await BTREE.create(map);
    
    // test remove
    const partialData = data.slice();
    const partialMap = new Map<string, CID>();
    let partialRoot = root;
    dataCIDs.forEach((cid, k) => partialMap.set(data[k], cid));
    for (let i = 0; i < 100; ++i) {
        const idx = (Math.random() * partialData.length) | 0;
        const item = partialData[idx];
        partialData.splice(idx, 1);
        partialMap.delete(item);

        partialRoot = await BTREE.remove(partialRoot, item);
        const expected = await BTREE.create(partialMap);
        t.equals(partialRoot.toString(), expected.toString(), 'remove ' + item);
    }

    // test upsert
    const upsertMap = new Map<string, CID>();
    let upsertRoot = await BTREE.create(upsertMap);
    for (let i = 0; i < 100; ++i) {
        const idx = (Math.random() * data.length) | 0;
        const key = data[idx];
        const value = dataCIDs[idx];

        upsertRoot = await BTREE.upsert(upsertRoot, key, value);

        upsertMap.set(key, value);
        const expected = await BTREE.create(upsertMap);

        t.equals(upsertRoot.toString(), expected.toString(), 'upsert ' + key);
    }

    t.end();
});
