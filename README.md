# NeoWorks Storage Mesh

An open storage network where anyone can run a node and nobody but the owner
can read what is stored. Devices encrypt everything before it leaves them, cut
it into erasure-coded pieces and spread those over independent nodes.

## Roles

- **Devices** hold every key. They encrypt, cut data into pieces, keep the
  index of what is stored where, and are the only place data is readable.
- **Nodes** store pieces addressed by their hash, return them, prove they still
  hold them, and delete them when their lease is not renewed. A piece is noise
  to them: they see its size, when it arrived and the address that sent it.
- **Coordinators** track the network, never anyone's data: the list of nodes
  and their reliability, the storage allowance of each account, the list of
  spent tokens, and sign-in. NeoWorks runs the first ones; the set of
  coordinators is a signed list that can grow into a federation.

## Status

Early. A file can be stored on six nodes and read back while any two of them
are down, have lost their pieces or return corrupted ones. Nodes keep each
piece for a lease and delete it once the lease ends unless it is renewed,
which is also how a file is deleted. An audit checks that the nodes still
hold every piece, from a few random segments with Merkle proofs. There are
no coordinators, tokens or repair yet; the issues track what comes next.

## Layout

- `crates/mesh-core`: types every role shares, such as piece ids.
- `crates/mesh-node`: the node, and the `mesh-node` binary.
- `crates/mesh-client`: encryption, erasure coding, upload and download, and
  the `mesh` binary.

## Try it

Start six nodes, each in its own terminal or in the background:

```sh
cargo build --release
for i in 1 2 3 4 5 6; do
  target/release/mesh-node --listen 127.0.0.1:700$i --data data/node-$i &
done
```

Store a file, stop two nodes and read it back:

```sh
target/release/mesh put photo.jpg --manifest photo.manifest.json \
  --node http://127.0.0.1:7001 --node http://127.0.0.1:7002 \
  --node http://127.0.0.1:7003 --node http://127.0.0.1:7004 \
  --node http://127.0.0.1:7005 --node http://127.0.0.1:7006
kill %2 %5
target/release/mesh get photo.manifest.json photo-again.jpg
```

The manifest holds the file's key, so anyone who has it can read the file.

`put` stores a file for 30 days by default (`--days` changes it). To keep it
longer, renew it before then:

```sh
target/release/mesh renew photo.manifest.json --days 30
```

To check that the nodes still hold every piece, without downloading them:

```sh
target/release/mesh audit photo.manifest.json
```

## License

Apache License 2.0, see [LICENSE](LICENSE).
