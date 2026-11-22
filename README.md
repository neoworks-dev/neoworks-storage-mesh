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

Early. A file is cut into chunks where its content says, each padded to a
size bucket and encrypted under a key derived from its bytes, so a chunk you
already stored is never stored again: an edited file only uploads the chunks
that changed. The chunks are spread over nodes chosen from a list, so that no
operator, network or subnet holds more pieces of a chunk than it can lose,
and a file can be read back while any two of a chunk's nodes are down, have
lost their pieces or return corrupted ones. Nodes keep each piece for a lease
and delete it once the lease ends unless it is renewed, which is also how a
file is deleted. An audit checks that the nodes still hold every piece, from
a few random segments with Merkle proofs, and repair rebuilds lost pieces
onto other nodes. There are no coordinators or tokens yet, so the node list
is a file you write; the issues track what comes next.

## Layout

- `crates/mesh-core`: types every role shares, such as piece ids.
- `crates/mesh-node`: the node, and the `mesh-node` binary.
- `crates/mesh-client`: chunking, encryption, erasure coding, placement,
  upload, download, audits and repair, and the `mesh` binary.

## Try it

Start seven nodes in the background:

```sh
cargo build --release
for i in 1 2 3 4 5 6 7; do
  target/release/mesh-node --listen 127.0.0.1:700$i --data data/node-$i &
done
```

Write a node list. On one machine every node needs its own `operator` and
`network`, or placement won't put more than two pieces of a chunk on them:

```sh
{
  echo '['
  for i in 1 2 3 4 5 6 7; do
    comma=$([ $i -lt 7 ] && echo ,)
    echo "  {\"url\": \"http://127.0.0.1:700$i\", \"operator\": \"op-$i\", \"network\": \"net-$i\"}$comma"
  done
  echo ']'
} > nodes.json
```

Store a file, stop two nodes and read it back:

```sh
target/release/mesh put photo.jpg --nodes nodes.json --manifest photo.manifest.json
kill %2 %5
target/release/mesh get photo.manifest.json photo-again.jpg
```

The manifest holds the chunk keys, so anyone who has it can read the file.
The first `put` also creates `mesh.key`, your dedup key: keep it private, and
keep using it, since it is what makes your chunks encrypt the same way every
time.

When you store a new version of a file, name the old manifest, and only the
chunks that changed are uploaded:

```sh
target/release/mesh put photo-edited.jpg --nodes nodes.json \
  --manifest photo-edited.manifest.json --earlier photo.manifest.json
```

`put` stores a file for 30 days by default (`--days` changes it). To keep it
longer, renew it before then:

```sh
target/release/mesh renew photo.manifest.json --days 30
```

To check that the nodes still hold every piece, without downloading them:

```sh
target/release/mesh audit photo.manifest.json
```

When nodes have lost pieces, rebuild them onto other nodes from the list.
Repair updates the manifest with where the new pieces are:

```sh
target/release/mesh repair photo.manifest.json --nodes nodes.json
```

## License

Apache License 2.0, see [LICENSE](LICENSE).
