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
  Nodes also keep signed pointers, small records under an owner's public key
  that say where the newest version of something is.
- **Coordinators** track the network, never anyone's data: the list of nodes
  with their reliability and speed, what storage costs on them, each
  account's token balance and free allowance, the list of spent tokens, and
  sign-in. NeoWorks runs the first ones; the set of coordinators is a signed
  list that can grow into a federation.

## Status

Early. A file is cut into chunks where its content says, each encrypted under
a key derived from its bytes, so a chunk you already stored is never stored
again: an edited file only uploads the chunks that changed. Each chunk is
cut into data and parity pieces, 4 + 2 by default, or the cheapest layout
that meets what an app says the file is worth: how much loss it tolerates,
how fast it must read and how many regions it must span. The pieces are
spread over nodes chosen from a list, so that no operator, network, subnet
or region holds more pieces of a chunk than it can lose, and a file can be
read back while as many of a chunk's nodes as it has parity pieces are down,
have lost their pieces or return corrupted ones. Nodes keep each piece for a lease
and delete it once the lease ends unless it is renewed, which is also how a
file is deleted. An audit checks that the nodes still hold every piece, from
a few random segments with Merkle proofs, and repair rebuilds lost pieces
onto other nodes. Each node has a key of its own and registers with a
coordinator, which checks that nodes are up, audits them with small pieces
of its own, measures how fast they are and publishes a signed list of them.
Clients register their chunks there by piece ids, without keys, and the
coordinator rebuilds the pieces of nodes that go down while the owner's
devices are off. With several coordinators, each chunk is registered with
two of them, and the second repairs it while the first is offline.

Storage is paid in tokens worth 0.1 euro cents each, signed blind so that
nodes and coordinators can't tell whose they are. Nodes never set prices:
coordinators put each node in a class by its uptime and speed and move each
class's price once a day by how full it is. Every account gets a free
allowance each month, which only the coordinator's own nodes take, and buys
more. A node's operator earns what users pay the node, over the month and
only while the node stays up and passes the coordinator's audits, and can
spend it on storage of their own.
Tokens can never be turned back into money. The issues track what comes
next.

## Layout

- `crates/mesh-core`: types every role shares, such as piece and node ids
  and the signed records.
- `crates/mesh-node`: the node, and the `mesh-node` binary.
- `crates/mesh-coordinator`: the coordinator, and the `mesh-coordinator`
  binary.
- `crates/mesh-client`: chunking, encryption, erasure coding, placement,
  upload, download, audits and repair, and the `mesh` binary.

## Try it

Start a coordinator that publishes its node list every 10 seconds instead of
every hour:

```sh
cargo build --release
target/release/mesh-coordinator --listen 127.0.0.1:7000 --data data/coordinator \
  --publish-seconds 10 &
```

It prints its key, which clients check its node list against. Copy it into
`coordinators.json`, next to where clients reach the coordinator:

```json
{
  "version": 1,
  "coordinators": [{ "key": "<the coordinator key>", "url": "http://127.0.0.1:7000" }]
}
```

Start seven nodes that register with it. On one machine every node needs its
own `operator` and `network`, or placement won't put more than two pieces of
a chunk on them:

```sh
for i in 1 2 3 4 5 6 7; do
  target/release/mesh-node --listen 127.0.0.1:700$i --data data/node-$i \
    --coordinator http://127.0.0.1:7000 --public-url http://127.0.0.1:700$i \
    --operator op-$i --network net-$i --region local --capacity-gb 10 &
done
```

Once the next list is out, store a file, stop two nodes and read it back:

```sh
sleep 10
target/release/mesh put photo.jpg --coordinator http://127.0.0.1:7000 \
  --coordinators coordinators.json --manifest photo.manifest.json
kill %3 %6
target/release/mesh get photo.manifest.json photo-again.jpg \
  --coordinator http://127.0.0.1:7000 --coordinators coordinators.json
```

The manifest names nodes by id, so every command, `get` too, looks up where
the nodes are now in the node list. Instead of a coordinator, `--nodes
nodes.json` takes a list you write, as `mesh_client::NodeList` describes, with
each node's id as it printed it.

The manifest holds the chunk keys, so anyone who has it can read the file.
The first `put` also creates `mesh.key`, your dedup key: keep it private, and
keep using it, since it is what makes your chunks encrypt the same way every
time.

When you store a new version of a file, name the old manifest, and only the
chunks that changed are uploaded:

```sh
target/release/mesh put photo-edited.jpg --coordinator http://127.0.0.1:7000 \
  --coordinators coordinators.json --manifest photo-edited.manifest.json \
  --earlier photo.manifest.json
```

`put` stores a file for 30 days by default (`--days` changes it). To keep it
longer, renew it before then:

```sh
target/release/mesh renew photo.manifest.json --coordinator http://127.0.0.1:7000 \
  --coordinators coordinators.json --days 30
```

To check that the nodes still hold every piece, without downloading them:

```sh
target/release/mesh audit photo.manifest.json --coordinator http://127.0.0.1:7000 \
  --coordinators coordinators.json
```

When nodes have lost pieces, rebuild them onto other nodes from the list.
Repair updates the manifest with where the new pieces are:

```sh
target/release/mesh repair photo.manifest.json --coordinator http://127.0.0.1:7000 \
  --coordinators coordinators.json
```

### With an account

Instead of keeping manifest files, an account keeps an index of its files in
the mesh, behind a signed pointer. Any device with the account's secret finds
every file:

```sh
target/release/mesh account-new
N="--coordinator http://127.0.0.1:7000 --coordinators coordinators.json"
target/release/mesh store photo.jpg $N
target/release/mesh ls $N --index other-device.json
target/release/mesh fetch photo.jpg photo-again.jpg $N --index other-device.json
```

To keep everything stored, run `mesh renew-all $N` from a timer, for
example weekly. It renews every file, the index and its pointer with one
request per node, and warns about files that lost too many pieces.

`account.key` holds the secret: keep it private, since it opens every file
of the account. To get it onto another device without copying the file,
sign up with an email and password once, then sign in on the new device; the
password is read from standard input:

```sh
target/release/mesh sign-up ana@example.org --coordinator http://127.0.0.1:7000
target/release/mesh sign-in ana@example.org --coordinator http://127.0.0.1:7000 \
  --account phone.key
```

The coordinator keeps the secret locked under a key derived from the
password, which it never sees. `mesh-index.json` is this device's copy of the index;
`--index` names another, as a second device would have.

### Locating nodes

A coordinator works out each node's network and country from the address
it reaches the node at, rather than believing the node. Download
[DB-IP's IP to City Lite](https://db-ip.com/db/download/ip-to-city-lite)
CSV and [iptoasn.com's](https://iptoasn.com) `ip2asn-combined.tsv`, unpack
them, and start the coordinator with
`--city-csv dbip-city-lite.csv --asn-tsv ip2asn-combined.tsv --location 50.11,8.68`,
the last being where the coordinator is. Without them, every node counts as
a network of its own.

## License

Apache License 2.0, see [LICENSE](LICENSE). Node locations come from IP
Geolocation by DB-IP (<https://db-ip.com>), licensed CC BY 4.0.
