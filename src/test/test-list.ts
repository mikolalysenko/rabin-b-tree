import tape = require('tape');
import { RabinList } from "../rabin-list";
import { DEFAULT_FORMATS, encodeJSON, inspectList } from "./helpers";

const LIST = new RabinList(DEFAULT_FORMATS.hasher, DEFAULT_FORMATS.codec, DEFAULT_FORMATS.storage);

tape('empty tree', async (t) => {
    const empty = await LIST.create([]);
    console.log(empty);

    const tree = await inspectList(LIST, empty);
    // console.log(tree);

    t.same(tree.leaf, true, 'leaf ok');
    t.same(tree.count, [], 'count ok');
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

    // create a tree
    const root = await LIST.create(dataCIDs);
    // console.log('root =', root.toString());

    // print tree
    const tree = await inspectList(LIST, root);
    // console.log(tree);

    t.equals(await LIST.size(root), N, 'size is ok');

    // now run some random index tests
    for (let i = 0; i < T; ++i) {
        const index = Math.floor(N * Math.random());
        const element = await LIST.at(root, index);
        t.equals(element.toString(), dataCIDs[index].toString(), 'test query at index ' + index);
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
    const dataCIDs = await Promise.all(data.map((value) => encodeJSON({
        value,
        ...DEFAULT_FORMATS
    })));

    // create a tree
    const root = await LIST.create(dataCIDs);
    
    async function testScan (start:number, end:number) {
        let ptr = start;
        for await (const x of LIST.scan(root, { lo: start, hi: end })) {
            t.equals(x.toString(), dataCIDs[ptr].toString(), 'scan: ' + ptr)
            ptr += 1;
        }
        t.equals(ptr, Math.min(end, N), 'scan returned expected number of elements');
    }

    // scan full array
    await testScan(0, Infinity);
    await testScan(500, 3000);

    t.end();
});

tape('splice', async (t) => {
    // first create a bunch of random strings
    const data:string[] = [];
    for (let i = 0; i < 1e4; ++i) {
        let x = '' + i;
        data.push(x);
    }
    const cids = await Promise.all(data.map((value) => encodeJSON({
        value,
        ...DEFAULT_FORMATS
    })));

    // make a pair of trees for empty and full
    const [ empty, one, full ] = await Promise.all([ LIST.create([]), LIST.create([ cids[0] ]), LIST.create(cids) ]);
    // console.log('empty =', await inspectArray(RA, empty));
    // console.log('one = ', await inspectArray(RA, one));
    // console.log('full = ', await inspectArray(RA, full));

    // check splice fully delete is consistent
    const removeAll = await LIST.splice(full, 0, cids.length);
    t.equals(removeAll.toString(), empty.toString(), 'removing elements canonically produces same result');

    // check splice full insert is consistent
    const insertAll = await LIST.splice(empty, 0, 0, ...cids);
    t.equals(insertAll.toString(), full.toString(), 'inserting all elements produces same result');

    // check singleton array
    const removeAllButOne = await LIST.splice(full, 1, cids.length - 1);
    t.equals(removeAllButOne.toString(), one.toString(), 'remove n - 1');
    const insertOne = await LIST.splice(empty, 0, 0, cids[0]);
    t.equals(insertOne.toString(), one.toString(), 'insert 1 into empty');
    const insertAllButOne = await LIST.splice(one, 1, 0, ...cids.slice(1));
    t.equals(insertAllButOne.toString(), full.toString(), 'insert n - 1');

    // create some extra data
    const exdata:string[] = [];
    for (let i = 0; i < 1e4; ++i) {
        let x = 'extra:' + i;
        exdata.push(x);
    }
    const excids = await Promise.all(exdata.map((value) => encodeJSON({
        value,
        ...DEFAULT_FORMATS
    })));

    async function testSplice (start:number, deleteCount:number, itemLength:number) {
        const expectedData = cids.slice();
        expectedData.splice(start, deleteCount, ...excids.slice(0, itemLength));
        const expectedTree = await LIST.create(expectedData);
        const actualTree = await LIST.splice(full, start, deleteCount, ...excids.slice(0, itemLength));

        // console.log('expected:', await inspectArray(RA, expectedTree));
        // console.log('actual:', await inspectArray(RA, actualTree));

        t.equals(actualTree.toString(), expectedTree.toString(), `test splice.  start=${start}, deleteCount=${deleteCount}, items.length=${itemLength}`);
    }

    // test a few special values
    await testSplice(2000, 100, 1000);
    await testSplice(7506, 208, 2682);
    await testSplice(228, 1435, 2251);

    // check a series of random splices
    for (let i = 0; i < 100; ++i) {
        const a = (Math.random() * cids.length) | 0;
        const b = (Math.random() * cids.length) | 0;
        const start = Math.min(a, b);
        const deleteCount = Math.max(a, b) - start;
        const itemLength = (Math.random() * excids.length) | 0;
        
        await testSplice(start, deleteCount, itemLength);
    }

    t.end();
});