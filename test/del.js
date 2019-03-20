var test = require('tape')
var collect = require('collect-stream')
var makeOsm = require('./create_db')

test('del', function (t) {
  t.plan(20)
  var osm = makeOsm()
  var docs = {
    A: { type: 'node', lat: 64.5, lon: -147.3 },
    B: { type: 'node', lat: 63.9, lon: -147.6 },
    C: { type: 'node', lat: 64.2, lon: -146.5 },
    D: { type: 'node', lat: 64.123, lon: -147.56 },
    E: { type: 'way', refs: [ 'A', 'B' ] },
    F: { type: 'way', refs: [ 'A', 'B', 'C' ] },
    G: { d: 'E' },
    H: { d: 'D' }
  }
  var names = {}
  var nodes = {}
  var versions = {}

  var keys = Object.keys(docs).sort()
  ;(function next () {
    if (keys.length === 0) return osm.ready(ready)
    var key = keys.shift()
    var doc = docs[key]
    if (doc.refs) {
      doc.refs = doc.refs.map(function (ref) { return names[ref] })
    }
    if (doc.d) {
      osm.del(names[doc.d], function (err, node) {
        t.ifError(err)
        versions[key] = node.key
        nodes[doc.d] = node
        next()
      })
    } else {
      osm.create(doc, function (err, k, node) {
        t.ifError(err)
        names[key] = k
        versions[key] = node.key
        nodes[k] = node
        next()
      })
    }
  })()

  function ready () {
    var q0 = [[63, 65], [-148, -146]]
    var ex0 = [
      { type: 'node',
        lat: 64.5,
        lon: -147.3,
        id: names.A,
        version: versions.A },
      { type: 'node',
        lat: 63.9,
        lon: -147.6,
        id: names.B,
        version: versions.B },
      { type: 'node',
        lat: 64.2,
        lon: -146.5,
        id: names.C,
        version: versions.C },
      { type: 'way',
        refs: [ names.A, names.B, names.C ],
        id: names.F,
        version: versions.F },
      { deleted: true, id: names.D, version: versions.H },
      { deleted: true, id: names.E, version: versions.G }
    ].sort(idcmp)
    osm.query(q0, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex0, 'full coverage query')
    })
    collect(osm.queryStream(q0), function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex0, 'full coverage stream')
    })
    var q1 = [[62, 64], [-149.5, -147.5]]
    var ex1 = [
      { type: 'node',
        lat: 64.5,
        lon: -147.3,
        id: names.A,
        version: versions.A },
      { type: 'node',
        lat: 63.9,
        lon: -147.6,
        id: names.B,
        version: versions.B },
      { type: 'node',
        lat: 64.2,
        lon: -146.5,
        id: names.C,
        version: versions.C },
      { type: 'way',
        refs: [ names.A, names.B, names.C ],
        id: names.F,
        version: versions.F },
      { deleted: true, id: names.E, version: versions.G }
    ].sort(idcmp)
    osm.query(q1, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex1, 'partial coverage query')
    })
    collect(osm.queryStream(q1), function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex1, 'partial coverage stream')
    })
    var q2 = [[62, 64], [-147, -145]]
    var ex2 = []
    osm.query(q2, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex2, 'empty coverage query')
    })
    collect(osm.queryStream(q2), function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex2, 'empty coverage stream')
    })
  }
})

test('del with value', function (t) {
  t.plan(5)

  var osm = makeOsm()

  var doc = { type: 'node', lat: 14, lon: -14, changeset: 'foobar' }

  osm.create(doc, function (err, id) {
    t.ifError(err)
    var v = {
      lat: doc.lat,
      lon: doc.lon,
      changeset: doc.changeset
    }
    osm.del(id, { value: v }, function (err, node) {
      t.ifError(err)
      doGet(id, node.key)
    })
  })

  function doGet (id, version) {
    osm.get(id, function (err, heads) {
      t.ifError(err)
      t.equals(Object.keys(heads).length, 1)
      var actual = heads[Object.keys(heads)[0]]
      var expected = {
        changeset: 'foobar',
        id: id,
        lat: 14,
        lon: -14,
        version: version,
        deleted: true
      }
      t.deepEqual(actual, expected, 'correct query /w value')
    })
  }
})

test('query deleted node with value', function (t) {
  t.plan(4)

  var osm = makeOsm()

  var doc = { type: 'node', lat: 14, lon: -14, changeset: 'foobar' }

  osm.create(doc, function (err, id2) {
    t.ifError(err)
    var v = {
      lat: doc.lat,
      lon: doc.lon,
      changeset: doc.changeset
    }
    osm.del(id2, { value: v }, function (err, node) {
      t.ifError(err)
      doQuery(id2, node.key)
    })
  })

  function doQuery (id, version) {
    var q = [[-90,90],[-180,180]]
    var expected = {
      changeset: 'foobar',
      id: id,
      lat: 14,
      lon: -14,
      version: version,
      deleted: true
    }
    osm.query(q, function (err, res) {
      t.ifError(err)
      t.deepEqual(res, [expected], 'full coverage query')
    })
  }
})

function idcmp (a, b) {
  return a.id < b.id ? -1 : 1
}
