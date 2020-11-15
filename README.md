# rabin-b-tree

**This module is experimental**

Canonical functionally persistent data structures for lists and ordered indexes.

## RabinList

### `const rl = new RabinList(hasher, codec, storage)`

### `rl.create(items)`

### `rl.at(list, index)`

### `rl.scan(list[, options])`

### `rl.size(list)`

### `rl.splice(list, start, deleteCount, ...items)`

## RabinBtree

### `const rt = new RabinBTree(hasher, codec, storage, compare)`

### `rt.create(map)`

### `rt.at(tree, index)`

### `rt.eq(tree, index)`

### `rt.scan(tree[, options])`

### `rt.size(tree)`

### `rt.upsert(tree, key, value)`

### `rt.remove(tree, key)`
