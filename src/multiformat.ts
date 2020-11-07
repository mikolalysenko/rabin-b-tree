// pulls in the stuff we need from multiformats and wraps it in good-enough typescript interfaces
// did this to avoid fighting with multiformats/node15 more than was required.  was wasting too much time trying to figure stuff out
//
const _Block = (<any>require)('multiformats/block');
const _sha2 = (<any>require)('multiformats/hashes/sha2');
const _json = (<any>require)('multiformats/codecs/json');
const _CID = (<any>require)('multiformats/cid');

export interface Codec {
    name:string;
    code:number;
    encode<T>(object:T):Uint8Array;
    decode<T>(bytes:Uint8Array):T;
}

export interface Hasher {
    name:string;
    code:number;
    encode(input:Uint8Array):Promise<Uint8Array>;
}

export interface MultihashDigest {
    code: number;
    digest: Uint8Array;
    size: number;
    bytes: Uint8Array;
}

export interface CID {
    version:number;
    code:number;
    multihash:MultihashDigest;
    bytes:Uint8Array;

    equals(other:CID):boolean;
    toString():string;
    toJSON():{
        code:number,
        version:number,
        hash:Uint8Array,
    };
}

export interface Block<T> {
    cid:CID;
    bytes:Uint8Array;
    value:T;
}

export const sha256Hasher:Hasher = _sha2.sha256;
export const sha512Hasher:Hasher = _sha2.sha512;
export const jsonCodec:Codec = _json;

export function encode<T>(spec:{
    value:T,
    hasher:Hasher,
    codec:Codec,
}) : Promise<Block<T>> {
    return _Block.encode(spec);
}

export function decode<T>(spec:{
    bytes:Uint8Array,
    hasher:Hasher,
    codec:Codec,
}) : Promise<Block<T>> {
    return _Block.decode(spec);
}

export function parseCID (hash:string) {
    return _CID.parse(hash);
}