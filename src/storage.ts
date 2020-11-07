import { Block, CID } from './multiformat';

export interface Storage {
    put<T>(block:Block<T>):Promise<void>;
    get<T>(cid:CID):Promise<Block<T>>;
}
