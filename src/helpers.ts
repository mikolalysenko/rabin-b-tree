// sum up all the elements in x
export function sum (x:number[], start:number, end:number) {
    let r = 0;
    for (let i = start; i < end; ++i) {
        r += x[i];
    }
    return r;
}

// find index of last element in array <= key
// TODO: replace with interpolation search or whatever
export function findPred<K> (items:K[], key:K, compare:(a:K, b:K) => number) {
    let pred = -1;
    for (let i = 0; i < items.length; ++i) {
        if (compare(items[i], key) <= 0) {
            pred = i;
        } else {
            break;
        }
    }
    return pred;
}