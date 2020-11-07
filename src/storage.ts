import { Block, CID } from "./multiformat";

export interface Storage {
    put<T>(block:Block<T>):Promise<void>;
    get<T>(cid:CID):Promise<Block<T>>;
}

export class MemoryStorage implements Storage {
    private _blocks = new Map<string, Block<any>>();

    public async put<T>(block:Block<T>) {
        this._blocks.set(block.cid.toString(), block);
    }

    public async get<T>(cid:CID) : Promise<Block<T>> {
        return this._blocks.get(cid.toString());
    }
}