var hyperkv = require('hyperkv')
var hyperkdb = require('hyperlog-kdb-index')
var kdbtree = require('kdb-tree-store')
var sub = require('subleveldown')
var randomBytes = require('randombytes')
var has = require('has')
var once = require('once')
var through = require('through2')
var to = require('to2')
var readonly = require('read-only-stream')
var xtend = require('xtend')
var join = require('hyperlog-join')
var inherits = require('inherits')
var EventEmitter = require('events').EventEmitter
var hex2dec = require('./lib/hex2dec.js')
var lock = require('mutexify')
var defined = require('defined')
var after = require('after-all')

module.exports = DB
inherits(DB, EventEmitter)

function DB (opts) {
  var self = this
  if (!(self instanceof DB)) return new DB(opts)
  self.log = opts.log
  self.db = opts.db
  self.kv = defined(opts.kv, hyperkv({
    log: self.log,
    db: sub(self.db, 'kv')
  }))
  self.kv.on('error', function (err) { self.emit('error', err) })
  self.lock = lock()
  self.kdb = hyperkdb({
    log: self.log,
    store: opts.store,
    db: sub(self.db, 'kdb'),
    kdbtree: kdbtree,
    types: [ 'float', 'float' ],
    map: function (row, next) {
      if (!row.value) return null
      var v = row.value.v, d = row.value.d
      if (v && v.lat !== undefined && v.lon !== undefined) {
        next(null, { type: 'put', point: ptf(v) })
      } else if (d && Array.isArray(row.value.points)) {
        var pts = row.value.points.map(ptf)
        next(null, { type: 'put', points: pts })
      } else next()
      function ptf (x) { return [ x.lat, x.lon ] }
    }
  })
  self.kdb.on('error', function (err) { self.emit('error', err) })
  self.refs = join({
    log: self.log,
    db: sub(self.db, 'r'),
    map: function (row, cb) {
      if (!row.value) return
      var k = row.value.k, v = row.value.v || {}
      var ops = []
      var next = after(function () {
        cb(null, ops)
      })

      // Update refs
      var refs = v.refs || row.value.refs || []
      var members = v.members || row.value.members || []
      row.links.forEach(function (link) {
        var done = next()
        self.log.get(link, function (err, node) {
          if (node.value.v.refs) {
            for (var i = 0; i < node.value.v.refs.length; i++) {
              var ref = node.value.v.refs[i]
              ops.push({ type: 'del', key: ref, rowKey: link })
            }
          }
          if (node.value.v.members) {
            for (var i = 0; i < node.value.v.members.length; i++) {
              var member = node.value.v.members[i]
              if (typeof member === 'string') member = { ref: member }
              if (typeof member.ref !== 'string') return
              ops.push({ type: 'del', key: member.ref, rowKey: link })
            }
          }
          done()
        })
      })
      if (k) {
        for (var i = 0; i < refs.length; i++) {
          ops.push({ type: 'put', key: refs[i], value: k })
        }
        for (var i = 0; i < members.length; i++) {
          ops.push({ type: 'put', key: members[i].ref || members[i], value: k })
        }
      }
    }
  })
  self.refs.on('error', function (err) { self.emit('error', err) })
  self.changeset = join({
    log: self.log,
    db: sub(self.db, 'c'),
    map: function (row, cb) {
      if (!row.value) return cb()
      var v = row.value.v
      if (!v || !v.changeset) return cb()
      return cb(null, { type: 'put', key: v.changeset, value: 0 })
    }
  })
  self.changeset.on('error', function (err) { self.emit('error', err) })
}

DB.prototype._links = function (link, cb) {
  var self = this
  self.log.get(link, function (err, doc) {
    if (err) return cb(err)
    self.refs.list(doc.value.k || doc.value.d, function (err, rows) {
      if (err) cb(err)
      else cb(null, rows.map(keyf))
    })
  })
  function keyf (row) { return row.key }
}

DB.prototype.ready = function (cb) {
  var pending = 3
  this.refs.dex.ready(ready)
  this.kdb.ready(ready)
  this.changeset.dex.ready(ready)
  function ready () { if (--pending === 0) cb() }
}

DB.prototype.create = function (value, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  if (!cb) cb = noop
  var key = hex2dec(randomBytes(8).toString('hex'))
  self.put(key, value, opts, function (err, node) {
    cb(err, key, node)
  })
}

DB.prototype.put = function (key, value, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  if (!cb) cb = noop
  self.lock(function (release) {
    self.kv.put(key, value, opts, function (err, node) {
      release(cb, err, node)
    })
  })
}

DB.prototype.del = function (key, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  cb = once(cb || noop)
  self._del(key, opts, function (err, rows) {
    if (err) return cb(err)
    self.batch(rows, opts, function (err, nodes) {
      if (err) cb(err)
      else cb(null, nodes[0])
    })
  })
}

DB.prototype._del = function (key, opts, cb) {
  var self = this
  self.kv.get(key, function (err, values) {
    if (err) return cb(err)
    // Filter deletions & map to old expected format.
    values = filterObj(values, function (key, value) {
      return !value.deleted
    })
    values = mapObj(values, function (key, value) {
      return value.value
    })

    var fields = {}
    var links = opts.keys || Object.keys(values)
    links.forEach(function (ln) {
      var v = values[ln] || {}
      if (v.lat !== undefined && v.lon !== undefined) {
        if (!fields.points) fields.points = []
        fields.points.push({ lat: v.lat, lon: v.lon })
      }
      if (Array.isArray(v.refs)) {
        if (!fields.refs) fields.refs = []
        fields.refs.push.apply(fields.refs, v.refs)
      }
    })
    cb(null, [ { type: 'del', key: key, links: links, fields: fields } ])
  })
}

DB.prototype.batch = function (rows, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  cb = once(cb || noop)

  var batch = []
  self.lock(function (release) {
    var pending = 1 + rows.length
    rows.forEach(function (row) {
      var key = defined(row.key, row.id)
      if (!key) {
        key = row.key = hex2dec(randomBytes(8).toString('hex'))
      }
      if (row.type === 'put') {
        batch.push(row)
        if (--pending === 0) done()
      } else if (row.type === 'del') {
        self._del(key, xtend(opts, row), function (err, xrows) {
          if (err) return release(cb, err)
          batch.push.apply(batch, xrows)
          if (--pending === 0) done()
        })
      } else {
        var err = new Error('unexpected row type: ' + row.type)
        process.nextTick(function () { release(cb, err) })
      }
    })
    if (--pending === 0) done()

    function done () {
      self.kv.batch(batch, opts, function (err, nodes) {
        release(cb, err, nodes)
      })
    }
  })
}

DB.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  this.kv.get(key, opts, function (err, docs) {
    if (err) return cb(err)
    // Filter deletions & map to old expected format.
    docs = filterObj(docs, function (key, value) {
      return !value.deleted
    })
    docs = mapObj(docs, function (key, value) {
      return value.value
    })

    cb(null, docs)
  })
}

DB.prototype.query = function (q, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  cb = once(cb || noop)
  var res = []
  self.ready(function () {
    self.kdb.query(q, opts, onquery)
  })
  function onquery (err, pts) {
    if (err) return cb(err)
    var pending = 1, seen = {}
    pts.forEach(function (pt) {
      pending++
      self._onpt(pt, seen, function (err, r) {
        if (r) res = res.concat(r)
        if (--pending === 0) done()
      })
    })
    if (--pending === 0) done()
  }
  function done () {
    if (opts.order === 'type') {
      res.sort(cmpType)
    }
    cb(null, res)
  }
}
var typeOrder = { node: 0, way: 1, relation: 2 }
function cmpType (a, b) {
  return typeOrder[a.type] - typeOrder[b.type]
}

DB.prototype._onpt = function (pt, seen, cb) {
  cb = once(cb || noop)
  var self = this
  var link = pt.value.toString('hex')
  if (has(seen, link)) return cb(null, [])
  seen[link] = true
  var res = [], added = {}, pending = 2
  self.log.get(link, function (err, doc) {
    if (doc && doc.value && doc.value.k && doc.value.v) {
      addDoc(doc.value.k, link, doc.value.v)
    } else if (doc && doc.value && doc.value.d && doc.value.points) {
      for (var i = 0; i < doc.value.points.length; i++) {
        var pt = doc.value.points[i]
        pt.deleted = true
        addDoc(doc.value.d, link, pt)
      }
    }
    if (--pending === 0) cb(null, res)
  })
  self._links(link, function onlinks (err, links) {
    if (!links) links = []
    links.forEach(function (link) {
      if (has(seen, link)) return
      seen[link] = true
      pending++
      self.log.get(link, function (err, doc) {
        if (doc && doc.value && doc.value.k && doc.value.v) {
          pending++
          self.get(doc.value.k, function (err, xdocs) {
            if (err) return cb(err)
            Object.keys(xdocs).forEach(function (key) {
              addDoc(doc.value.k, key, xdocs[key])
            })
            if (--pending === 0) cb(null, res)
          })
        }
        addDoc(doc.value.k, link, doc.value.v)
        if (--pending === 0) cb(null, res)
      })
      pending++
      self._links(link, function (err, links) {
        onlinks(err, links)
      })
    })
    if (--pending === 0) cb(null, res)
  })

  function addDoc (id, key, doc) {
    // For now, filter out deleted documents.
    if (doc.deleted) return

    if (!added.hasOwnProperty(key)) {
      res.push(xtend(doc, {
        id: id,
        version: key
      }))
      added[key] = true
    }
    if (doc && Array.isArray(doc.refs || doc.nodes)) {
      addWayNodes(doc.refs || doc.nodes)
    }
  }
  function addWayNodes (refs) {
    refs.forEach(function (ref) {
      if (has(seen, ref)) return
      seen[ref] = true
      pending++
      self.get(ref, function (err, docs) {
        Object.keys(docs || {}).forEach(function (key) {
          if (has(seen, key)) return
          seen[key] = true
          addDoc(ref, key, docs[key])
        })
        if (--pending === 0) cb(null, res)
      })
    })
  }
}

DB.prototype.queryStream = function (q, opts) {
  var self = this
  if (!opts) opts = {}
  var stream = opts.order === 'type'
    ? through.obj(writeType, endType)
    : through.obj(write)
  var seen = {}, queue = []
  self.ready(function () {
    var r = self.kdb.queryStream(q, opts)
    r.on('error', stream.emit.bind(stream, 'error'))
    r.pipe(stream)
  })
  return readonly(stream)

  function write (row, enc, next) {
    next = once(next)
    var tr = this
    self._onpt(row, seen, function (err, res) {
      if (res) res.forEach(function (r) {
        tr.push(r)
      })
      next()
    })
  }
  function writeType (row, enc, next) {
    next = once(next)
    var tr = this
    self._onpt(row, seen, function (err, res) {
      if (res) res.forEach(function (r) {
        if (r.type === 'node') tr.push(r)
        else queue.push(r)
      })
      next()
    })
  }
  function endType (next) {
    var tr = this
    queue.sort(cmpType).forEach(function (q) { tr.push(q) })
    next()
  }
}

DB.prototype.getChanges = function (key, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  var r = this.changeset.list(key, opts)
  var stream = r.pipe(through.obj(write))
  if (cb) collectObj(stream, cb)
  return readonly(stream)

  function write (row, enc, next) {
    this.push(row.key)
    next()
  }
}

function noop () {}

function collectObj (stream, cb) {
  cb = once(cb)
  var rows = []
  stream.on('error', cb)
  stream.pipe(to.obj(write, end))
  function write (x, enc, next) {
    rows.push(x)
    next()
  }
  function end () { cb(null, rows) }
}

function mapObj (obj, fn) {
  Object.keys(obj).forEach(function (key) {
    obj[key] = fn(key, obj[key])
  })
  return obj
}

function filterObj (obj, fn) {
  var res = {}
  Object.keys(obj).forEach(function (key) {
    if (fn(key, obj[key])) {
      res[key] = obj[key]
    }
  })
  return res
}
