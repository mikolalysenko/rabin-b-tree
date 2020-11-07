import tape = require('tape');
import { RabinBTree } from "../rabin-b-tree";
import { DEFAULT_FORMATS, encodeJSON, inspectTree } from "./helpers";

const N = 1e4;
const T = 1e2;

tape('index query test', async (t) => {
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
    const rbt = new RabinBTree(DEFAULT_FORMATS.hasher, DEFAULT_FORMATS.codec, DEFAULT_FORMATS.storage);
    const root = await rbt.create(dataCIDs);

    console.log('root =', root.toString());

    // print tree
    const tree = await inspectTree(rbt, root);
    console.log(tree);

    // now run some random index tests
    for (let i = 0; i < T; ++i) {
        const index = Math.floor(N * Math.random());
        const element = await rbt.query(root, index);
        t.equals(element.toString(), dataCIDs[index].toString(), 'test query at index ' + index);
    }

    t.end();
});