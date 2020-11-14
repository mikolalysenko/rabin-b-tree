import { Block, CID, Codec, encode, Hasher, Storage } from '../multiformat';
import { RabinBTree } from '../rabin-b-tree';
import { RabinList } from '../rabin-list';

const _sha2 = (<any>require)('multiformats/hashes/sha2');
const _json = (<any>require)('multiformats/codecs/json');

export class MemoryStorage implements Storage {
    private _blocks = new Map<string, Block<any>>();

    public async put<T>(block:Block<T>) {
        this._blocks.set(block.cid.toString(), block);
    }

    public async get<T>(cid:CID) : Promise<Block<T>> {
        return this._blocks.get(cid.toString());
    }
}

export const sha256Hasher:Hasher = _sha2.sha256;
export const sha512Hasher:Hasher = _sha2.sha512;
export const jsonCodec:Codec = _json;

export const DEFAULT_FORMATS = {
    hasher: sha256Hasher,
    codec: jsonCodec,
    storage: new MemoryStorage(),
};

export async function encodeJSON (config:{
    hasher: Hasher,
    codec: Codec,
    storage: Storage,
    value: any,
}) {
    const block = await encode({
        value: config.value,
        hasher: config.hasher,
        codec: config.codec,
    });
    await config.storage.put(block);
    return block.cid;
}

export async function parseJSON (config:{
    hasher: Hasher,
    codec: Codec,
    storage: Storage,
    cid: CID,
}) {
    const block:Block<any> = await config.storage.get(config.cid);
    return block.value;
}

export async function inspectList(l:RabinList, root:CID) {
    const node = await l.parseNode(root);
    return {
        cid: root.toString(),
        count: node.count,
        leaf: node.leaf,
        children: await Promise.all(node.hashes.map((cid, i) => {
            if (node.count[i] === 1) {
                return cid.toString();
            }
            return inspectList(l, cid);
        })),
    };
}

export async function inspectBTree<K> (bt:RabinBTree<K>, root:CID) {
    const node = await bt.parseNode(root);
    return {
        cid: root.toString(),
        count: node.count,
        leaf: node.leaf,
        keys: node.keys,
        children: await Promise.all(node.hashes.map((cid, i) => {
            if (node.count[i] === 1) {
                return cid.toString();
            }
            return inspectBTree(bt, cid);
        })),
    };
}