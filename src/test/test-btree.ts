import tape = require('tape');
import { CID } from '../multiformat';
import { RabinBTree } from '../rabin-b-tree';
import { DEFAULT_FORMATS, encodeJSON, inspectBTree, inspectList } from "./helpers";

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

// tape('scan test', async (t) => {
//     const N = 1e4;

//     // first create a bunch of random strings
//     const data:string[] = [];
//     for (let i = 0; i < N; ++i) {
//         data.push('ppp' + i);
//     }
//     const dataCIDs = await Promise.all(data.map((value) => encodeJSON({
//         value,
//         ...DEFAULT_FORMATS
//     })));

//     // create a tree
//     const root = await LIST.create(dataCIDs);
    
//     async function testScan (start:number, end:number) {
//         let ptr = start;
//         for await (const x of LIST.scan(root, start, end)) {
//             t.equals(x.toString(), dataCIDs[ptr].toString(), 'scan: ' + ptr)
//             ptr += 1;
//         }
//         t.equals(ptr, Math.min(end, N), 'scan returned expected number of elements');
//     }

//     // scan full array
//     await testScan(0, Infinity);
//     await testScan(500, 3000);

//     t.end();
// });
