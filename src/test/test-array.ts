import tape = require('tape');
import { RabinArray } from "../rabin-array";
import { DEFAULT_FORMATS, encodeJSON, inspectArray } from "./helpers";

const RA = new RabinArray(DEFAULT_FORMATS.hasher, DEFAULT_FORMATS.codec, DEFAULT_FORMATS.storage);

tape('index query test', async (t) => {
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
    const root = await RA.create(dataCIDs);
    // console.log('root =', root.toString());

    // print tree
    const tree = await inspectArray(RA, root);
    // console.log(tree);

    // now run some random index tests
    for (let i = 0; i < T; ++i) {
        const index = Math.floor(N * Math.random());
        const element = await RA.at(root, index);
        t.equals(element.toString(), dataCIDs[index].toString(), 'test query at index ' + index);
    }

    t.end();
});

tape('empty tree', async (t) => {
    const empty = await RA.create([]);
    console.log(empty);

    const tree = await inspectArray(RA, empty);
    // console.log(tree);

    t.same(tree.leaf, true, 'leaf ok');
    t.same(tree.count, [], 'count ok');
    t.same(tree.children, [], 'children ok');

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
    const [ empty, one, full ] = await Promise.all([ RA.create([]), RA.create([ cids[0] ]), RA.create(cids) ]);
    // console.log('empty =', await inspectArray(RA, empty));
    // console.log('one = ', await inspectArray(RA, one));
    // console.log('full = ', await inspectArray(RA, full));

    // check splice fully delete is consistent
    const removeAll = await RA.splice(full, 0, cids.length);
    t.equals(removeAll.toString(), empty.toString(), 'removing elements canonically produces same result');

    // check splice full insert is consistent
    const insertAll = await RA.splice(empty, 0, 0, ...cids);
    t.equals(insertAll.toString(), full.toString(), 'inserting all elements produces same result');

    // check singleton array
    const removeAllButOne = await RA.splice(full, 1, cids.length - 1);
    t.equals(removeAllButOne.toString(), one.toString(), 'remove n - 1');
    const insertOne = await RA.splice(empty, 0, 0, cids[0]);
    t.equals(insertOne.toString(), one.toString(), 'insert 1 into empty');
    const insertAllButOne = await RA.splice(one, 1, 0, ...cids.slice(1));
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
        const expectedTree = await RA.create(expectedData);
        const actualTree = await RA.splice(full, start, deleteCount, ...excids.slice(0, itemLength));

        // console.log('expected:', await inspectArray(RA, expectedTree));
        // console.log('actual:', await inspectArray(RA, actualTree));

        t.equals(actualTree.toString(), expectedTree.toString(), `test splice.  start=${start}, deleteCount=${deleteCount}, items.length=${itemLength}`);
    }

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