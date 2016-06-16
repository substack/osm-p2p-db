# osm-p2p-db

[![Build Status](https://img.shields.io/travis/digidem/osm-p2p-db.svg)](https://travis-ci.org/digidem/osm-p2p-db)
[![npm](https://img.shields.io/npm/v/osm-p2p-db.svg?maxAge=2592000)](https://www.npmjs.com/package/osm-p2p-db)

> p2p database for open street map data

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [API](#api)
  - [var osm = osmdb(opts)](#var-osm--osmdbopts)
  - [osm.create(doc, opts={}, cb)](#osmcreatedoc-opts-cb)
  - [osm.put(id, doc, opts={}, cb)](#osmputid-doc-opts-cb)
  - [osm.del(id, opts={}, cb)](#osmdelid-opts-cb)
  - [osm.batch(rows, opts={}, cb)](#osmbatchrows-opts-cb)
  - [osm.get(id, opts={}, cb)](#osmgetid-opts-cb)
  - [osm.query(q, opts, cb)](#osmqueryq-opts-cb)
  - [var rstream = osm.queryStream(q, opts)](#var-rstream--osmquerystreamq-opts)
  - [var rstream = osm.getChanges(id, cb)](#var-rstream--osmgetchangesid-cb)
  - [osm.on('error', function (err) {})](#osmonerror-function-err-)
  - [osm.kv](#osmkv)
  - [osm.log](#osmlog)
- [Browser](#browser)
- [Replication](#replication)
- [Architecture](#architecture)
- [Contribute](#contribute)
- [License](#license)

## Install

```
npm install osm-p2p-db
```

## Usage

``` js
var hyperlog = require('hyperlog')

var level = require('level')
var db = {
  log: level('/tmp/osm-p2p/log'),
  index: level('/tmp/osm-p2p/index')
}
var fdstore = require('fd-chunk-store')
var storefile = '/tmp/osm-p2p/kdb'

var osmdb = require('osm-p2p-db')
var osm = osmdb({
  log: hyperlog(db.log, { valueEncoding: 'json' }),
  db: db.index,
  store: fdstore(4096, storefile)
})

if (process.argv[2] === 'create') {
  var value = JSON.parse(process.argv[3])
  osm.create(value, function (err, key, node) {
    if (err) console.error(err)
    else console.log(key)
  })
} else if (process.argv[2] === 'query') {
  var q = process.argv.slice(3).map(csplit)
  osm.query(q, function (err, pts) {
    if (err) console.error(err)
    else pts.forEach(function (pt) {
      console.log(pt)
    })
  })
}

function csplit (x) { return x.split(',').map(Number) }
```

Now we can create a few nodes and search with a bounding box query:

```
$ mkdir /tmp/osm-p2p
$ node db.js create '{"type":"node","lat":64.6,"lon":-147.8}'
11892499690884077339
$ node db.js create '{"type":"node","lat":64.3,"lon":-148.2}'
1982521011513780909
$ node db.js create '{"type":"node","lat":64.5,"lon":-147.3}'
14062704270722785878
$ node db.js query 64.1,64.6 -148,-147
{ type: 'node',
  lat: 64.5,
  lon: -147.3,
  id: '14062704270722785878',
  version: 'e635d07b9fc0a9d048cdd5d9e97a44a19ba3a0b2a51830d1e3e0fadcb80935fc' }
```

We can make a `way` document that refers to a list of `node` documents:

```
$ node db.js create '{"type":"way","refs":
["11892499690884077339","1982521011513780909","14062704270722785878"]}'
14666931246975765366
```

When we query, any `ways` that have one or more nodes within the bounding box
will turn up in the results:

```
$ node db.js query 64.1,64.6 -148,-147
{ type: 'node',
  lat: 64.5,
  lon: -147.3,
  id: '14062704270722785878',
  version: 'e635d07b9fc0a9d048cdd5d9e97a44a19ba3a0b2a51830d1e3e0fadcb80935fc' }
{ type: 'way',
  refs: [ '11892499690884077339', '1982521011513780909', '14062704270722785878' ],
  id: '14666931246975765366',
  version: 'f4fc0045e298ca4f9373fab78dee4f0561b4056dcd7975eb92f21d0a05e0eede' }
```

## API

``` js
var osmdb = require('osm-p2p-db')
```

### var osm = osmdb(opts)

Create a new `osm` instance with:

* `opts.log` - a [hyperlog][1] with a valueEncoding of `json`
* `opts.db` - a [levelup][2] instance to store index data
* `opts.store` - an [abstract-chunk-store][3] instance

You may safely delete the index database whenever you like. The index data is
automatically regenerated. This is very useful if there are breaking changes to
the index code or if the data becomes corrupted. The hyperlog contains the
source of truth.

[1]: https://npmjs.com/package/hyperlog
[2]: https://npmjs.com/package/levelup
[3]: https://npmjs.com/package/abstract-chunk-store

### osm.create(doc, opts={}, cb)

Store a new document from `doc`. `cb(err, id, node)` fires with the generated
OSM `id` and the `node` from the underlying hyperlog.

Elements are `node`, `way`, and `relation`. Each element should have a `type`
property that contains the element type as a string.

* Nodes should have `doc.lat` and `doc.lon` coordinates.
* Ways should have an array of OSM keys as `doc.refs`.
* Relations should have an array member objects as `doc.members`.
Each member object has a `member.type` of the document pointed at by
`member.ref` and optionally a [`member.role`][7].

Another type of document is a `changeset`.
Each element should have a `changeset` property that refers to the id of a
`changeset` document.

It is recommended to use `tags.comment` to store free-form text describing the
changeset.

[7]: http://wiki.openstreetmap.org/wiki/Relation#Roles

### osm.put(id, doc, opts={}, cb)

Replace a document at `id` with `doc`.

If the document didn't exist previously, it will be created.

The options `opts` are passed to the underlying [hyperkv][4] instance.

By default, hyperkv will merge the most recent known forks into a single fork.
To add modifications to a fork without merging the changes into other forks,
set `opts.links` to an array of only the single key you want to update.

[4]: https://npmjs.com/package/hyperkv

### osm.del(id, opts={}, cb)

Delete a document at `id`.

The options `opts` are passed to the underlying [hyperkv][4] instance.

`cb(err, node)` fires with the underlying `node` in the hyperlog.

### osm.batch(rows, opts={}, cb)

Atomically insert an array of documents `rows`.

Each `row` in `rows` should have:

* `row.type` - `'put'` or `'del'`
* `row.key` or `row.id` - the id of the document
* `row.links` - array of links to ancestor keys
* `row.value` - for puts, the value to store

### osm.get(id, opts={}, cb)

Get a document as `cb(err, docs)` by its OSM `id`.

`docs` is an object mapping hyperlog hashes to current document values.

The options `opts` are passed to the underlying [hyperkv][4] instance.

### osm.query(q, opts, cb)

Query for all nodes, ways, and relations in the query given by the array `q`.
Queries are arrays of `[[minLat,maxLat],[minLon,maxLon]]` specifying a bounding
box.

`cb(err, res)` fires with an array of results `res`. Each result is the
document augmented with an `id` property and a `version` property that is the
hash key from the underlying hyperlog.

### var rstream = osm.queryStream(q, opts)

Return a readable object stream `rstream` of query results contained in the
query `q`. Queries are arrays of `[[minLat,maxLat],[minLon,maxLon]]` specifying
a bounding box.

Each object in the stream is a document augmented with an `id` property and a
`version` property that is the hash key from the underlying hyperlog.

### var rstream = osm.getChanges(id, cb)

Get the list of document ids in a changeset by a changeset `id`.

The document ids are available as `cb(err, ids)` and as the objects in the
readable object stream `rstream`.

### osm.on('error', function (err) {})

Handle errors from the underlying indexes with the `'error'` event.

### osm.kv

You can get at the [hyperkv][4] instance directly to perform more operations
using `osm.kv`.

For example, you can use `osm.kv.createReadStream()` to list all the id/value
pairs in the database.

### osm.log

The [hyperlog][1] instance is available as the `opts.log` property if you need
to get at it directly later.


## Browser

To use this module in the browser, use [level-browserify][5] to provide the
`opts.db` instance and [idb-chunk-store][6] as the `opts.store`. Each of these
is backed by IndexedDB, a native browser storage interface.

[5]: https://npmjs.com/package/level-browserify
[6]: https://www.npmjs.com/package/idb-chunk-store

## Replication

If you have two hyperlogs `log0` and `log1`, pipe them together and back again
to replicate:

```
var r0 = log0.replicate()
var r1 = log1.replicate()
r0.pipe(r1).pipe(r0)
```

Insert additional streams as necessary for network transports if the logs live
in different processes or machines.

If both logs have made edits to the same IDs, multiple records will appear for
the same ID in the results. To merge these "conflicts" back into a single value,
use `osm.put(id, doc, cb)` to store the desired document value.

## Architecture

[Read about the internal architecture](doc/architecture.markdown).

## Contribute

If you would like to support our work, or if you have ideas about how to use and adapt osm-p2p for your own project, then please dive in. Open [an issue](https://github.com/digidem/osm-p2p-db/issues) with a bug report or feature request, or send us a [pull request](https://github.com/digidem/osm-p2p-db/pulls) with a bug-fix or new feature. 

We need help right now adding tests and fixing edge-cases with [osm-p2p-server](https://github.com/digidem/osm-p2p-server) and increasing compatibility with other OSM tools such as [JOSM](https://josm.openstreetmap.de/).

## License

BSD (c) 2016, Digital Democracy.
