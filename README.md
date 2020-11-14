# rabin-b-tree

**This module is experimental**

Canonical functionally persistent data structures for lists and ordered indexes.

## RabinList

### `const rl = new RabinList(hasher, codec, storage)`

### `rl.create(items)`

### `rl.at(list, index)`

### `rl.scan(list[, start, end])`

### `rl.size(list)`

### `rl.splice(list, start, deleteCount, ...items)`

## RabinBtree