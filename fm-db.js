/* Family Meals: GitHub-backed database layer.
   Implements the small slice of the db/user API the app uses, storing each collection
   as one JSON file (data/<collection>.json) in a PRIVATE data repo. No family data lives in this file or repo.
   The access token is entered once per device and kept only in this browser's localStorage. */
(function () {
  "use strict";
  var REPO = "Icekoph83/family-meals";
  var BASE = "https://api.github.com/repos/" + REPO + "/contents/data/";
  var K_TOKEN = "fm:token", K_WHO = "fm:who", K_CACHE = "fm:cache:";
  var POLL_MS = 30000;

  function lsGet(k) { try { return localStorage.getItem(k) } catch (e) { return null } }
  function lsSet(k, v) { try { localStorage.setItem(k, v) } catch (e) {} }
  function lsDel(k) { try { localStorage.removeItem(k) } catch (e) {} }

  function b64e(s) { return btoa(unescape(encodeURIComponent(s))) }
  function b64d(s) { return decodeURIComponent(escape(atob(String(s).replace(/\n/g, "")))) }
  function rid() { var a = "abcdefghijklmnopqrstuvwxyz0123456789", s = ""; for (var i = 0; i < 20; i++) s += a[Math.floor(Math.random() * a.length)]; return s }
  function clone(x) { return x === undefined ? undefined : JSON.parse(JSON.stringify(x)) }

  var cols = {};
  function C(name) {
    if (!cols[name]) {
      var cached = null;
      try { cached = JSON.parse(lsGet(K_CACHE + name) || "null") } catch (e) {}
      cols[name] = { name: name, data: cached || {}, sha: "", etag: "", subs: [], loaded: !!cached, queue: Promise.resolve() };
    }
    return cols[name];
  }
  function hdr(extra) {
    var h = { Authorization: "Bearer " + lsGet(K_TOKEN), Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28" };
    for (var k in extra) h[k] = extra[k];
    return h;
  }
  function fail(msg, code) { var e = new Error(msg); e.code = code; return e }
  function onAuthFail() { lsDel(K_TOKEN); window.dispatchEvent(new CustomEvent("fm-auth")); }

  // Fetch one collection file. Returns true if changed.
  function pull(c, force) {
    var h = hdr(force || !c.etag ? {} : { "If-None-Match": c.etag });
    return fetch(BASE + c.name + ".json?ref=main", { headers: h, cache: "no-store" }).then(function (r) {
      if (r.status === 304) return false;
      if (r.status === 404) { c.sha = "-"; c.etag = ""; c.loaded = true; if (Object.keys(c.data).length) { c.data = {}; return true } return false }
      if (r.status === 401 || r.status === 403) { if (r.status === 401) onAuthFail(); throw fail("Not allowed (" + r.status + ")", r.status) }
      if (!r.ok) throw fail("GitHub error " + r.status, r.status);
      var et = r.headers.get("ETag") || "";
      return r.json().then(function (j) {
        var obj = {};
        try { obj = JSON.parse(b64d(j.content)) } catch (e) { throw fail("Could not read " + c.name, 0) }
        var changed = JSON.stringify(obj) !== JSON.stringify(c.data);
        c.data = obj; c.sha = j.sha; c.etag = et; c.loaded = true;
        lsSet(K_CACHE + c.name, JSON.stringify(obj));
        return changed;
      });
    });
  }

  function emit(c) {
    c.subs.slice().forEach(function (s) { try { s() } catch (e) { console.error(e) } });
  }

  // Serialized read-modify-write with retry on conflict.
  function mutate(c, fn) {
    var run = function () {
      var attempt = 0;
      var go = function () {
        attempt++;
        return pull(c, true).then(function () {
          var before = JSON.stringify(c.data);
          var next = clone(c.data); fn(next);
          if (JSON.stringify(next) === before) return;
          var body = { message: "app: update " + c.name, content: b64e(JSON.stringify(next, null, 1)), branch: "main" };
          if (c.sha && c.sha !== "-") body.sha = c.sha;
          return fetch(BASE + c.name + ".json", { method: "PUT", headers: hdr({ "Content-Type": "application/json" }), body: JSON.stringify(body) }).then(function (r) {
            if (r.status === 409 || r.status === 422) { if (attempt < 4) return go(); throw fail("Save conflict, try again", r.status) }
            if (r.status === 401) { onAuthFail(); throw fail("Token rejected", 401) }
            if (r.status === 403 || r.status === 404) throw fail("Token can't write to the data repo (" + r.status + ")", r.status);
            if (!r.ok) throw fail("Save failed (" + r.status + ")", r.status);
            return r.json().then(function (j) {
              c.data = next; c.sha = j.content && j.content.sha || c.sha; c.etag = "";
              lsSet(K_CACHE + c.name, JSON.stringify(next));
            });
          });
        });
      };
      return go();
    };
    // optimistic local apply so the UI responds at once
    var opt = clone(c.data); try { fn(opt); c.data = opt; emit(c) } catch (e) {}
    var p = c.queue.then(run);
    c.queue = p.catch(function () {});
    return p.then(function () { emit(c) }, function (e) { return pull(c, true).catch(function () {}).then(function () { emit(c); throw e }) });
  }

  // ---- polling
  var pollTimer = null;
  function pollAll() {
    if (document.hidden || !lsGet(K_TOKEN)) return;
    Object.keys(cols).forEach(function (n) {
      var c = cols[n]; if (!c.subs.length) return;
      pull(c, false).then(function (ch) { if (ch) emit(c) }).catch(function () {});
    });
  }
  function startPoll() { if (!pollTimer) { pollTimer = setInterval(pollAll, POLL_MS); document.addEventListener("visibilitychange", function () { if (!document.hidden) pollAll() }) } }

  // ---- API objects
  function snapCol(c, ord, lim) {
    var rows = Object.keys(c.data).map(function (id) { return { id: id, d: c.data[id] } });
    if (ord) rows.sort(function (a, b) {
      var x = a.d[ord.f], y = b.d[ord.f]; if (x === y) return 0; if (x === undefined) return 1; if (y === undefined) return -1;
      var r = x < y ? -1 : 1; return ord.dir === "desc" ? -r : r;
    });
    if (lim) rows = rows.slice(0, lim);
    return { docs: rows.map(function (r) { return { id: r.id, data: function () { return clone(r.d) } } }) };
  }
  function subscribe(c, render, errCb) {
    var first = true;
    var fn = function () { render() };
    c.subs.push(fn);
    if (c.loaded) setTimeout(render, 0);
    pull(c, true).then(function () { render() }).catch(function (e) { if (!c.loaded) { c.loaded = true; if (errCb) errCb(e) } else render() });
    startPoll();
    return function () { c.subs = c.subs.filter(function (s) { return s !== fn }) };
  }
  function collection(name) {
    var c = C(name), ord = null, lim = 0;
    var q = {
      orderBy: function (f, dir) { ord = { f: f, dir: dir || "asc" }; return q },
      limit: function (n) { lim = n; return q },
      onSnapshot: function (cb, err) { return subscribe(c, function () { cb(snapCol(c, ord, lim)) }, err) },
      add: function (data) { var id = rid(); return mutate(c, function (d) { d[id] = clone(data) }).then(function () { return { id: id } }) },
      doc: function (id) { return docRef(name, id) },
      get: function () { return pull(c, true).then(function () { return snapCol(c, ord, lim) }) }
    };
    return q;
  }
  function docRef(col, id) {
    var c = C(col);
    return {
      onSnapshot: function (cb, err) { return subscribe(c, function () { cb({ id: id, exists: id in c.data, data: function () { return clone(c.data[id]) } }) }, err) },
      get: function () { return pull(c, true).then(function () { return { id: id, exists: id in c.data, data: function () { return clone(c.data[id]) } } }) },
      set: function (data) { return mutate(c, function (d) { d[id] = clone(data) }) },
      update: function (patch) { return mutate(c, function (d) { var cur = d[id] || {}; for (var k in patch) { if (patch[k] && patch[k].__delete__) delete cur[k]; else cur[k] = clone(patch[k]) } d[id] = cur }) },
      delete: function () { return mutate(c, function (d) { delete d[id] }) }
    };
  }
  var db = {
    collection: collection,
    doc: function (path) { var p = String(path).split("/"); return docRef(p[0], p.slice(1).join("/")) }
  };

  var NAMES = { jake: "Jake", maggie: "Maggie" };
  var OLD = { "u_irV88F_eZxxkMEAKWAKRcQ": "jake" };  // ids from the earlier Claude-hosted version
  function me() { return lsGet(K_WHO) || "jake" }
  var user = {
    id: function () { return Promise.resolve(me()) },
    me: function () { return { id: me(), name: NAMES[me()] || me() } },
    isOwner: function () { return true },
    canEdit: function () { return true },
    can: function () { return true },
    profiles: function (ids) { var o = {}; (ids || []).forEach(function (i) { var k = OLD[i] || i; o[i] = { name: NAMES[k] || "" } }); return Promise.resolve(o) }
  };

  // ---- setup screen
  var ready = null;
  function check(tok) {
    return fetch("https://api.github.com/repos/" + REPO, { headers: { Authorization: "Bearer " + tok, Accept: "application/vnd.github+json" }, cache: "no-store" }).then(function (r) {
      if (r.status === 401) throw fail("That token isn't valid. Check you copied all of it.", 401);
      if (r.status === 404) throw fail("That token can't see the data repo. It needs access to " + REPO + ".", 404);
      if (!r.ok) throw fail("GitHub said " + r.status, r.status);
      return r.json();
    }).then(function (j) {
      if (j.permissions && j.permissions.push === false) throw fail("That token can read but not write. Give it Contents: Read and write.", 403);
    });
  }
  function setupScreen(note) {
    return new Promise(function (resolve) {
      var o = document.createElement("div");
      o.style.cssText = "position:fixed;inset:0;z-index:99;background:var(--bg,#faf6ef);color:var(--ink,#2b2620);overflow:auto;font:16px/1.45 system-ui,sans-serif";
      var box = document.createElement("div");
      box.style.cssText = "max-width:440px;margin:0 auto;padding:36px 20px";
      box.innerHTML = '<h1 style="font:700 26px Georgia,serif;margin:0 0 6px">Family Meals</h1>' +
        '<p style="color:var(--mute,#6f665a);margin:0 0 18px">One-time setup on this device. Paste the access token Jake made, and say who you are.</p>' +
        '<label style="display:block;font-size:14px;margin:0 0 4px">Access token</label>' +
        '<input id="fm-t" type="password" autocomplete="off" autocapitalize="off" spellcheck="false" style="width:100%;box-sizing:border-box;padding:12px;font:inherit;border-radius:10px;border:1px solid var(--line,#e6dccb);background:var(--card,#fff);color:inherit">' +
        '<label style="display:block;font-size:14px;margin:14px 0 4px">I am</label>' +
        '<select id="fm-w" style="width:100%;padding:12px;font:inherit;border-radius:10px;border:1px solid var(--line,#e6dccb);background:var(--card,#fff);color:inherit"><option value="jake">Jake</option><option value="maggie">Maggie</option></select>' +
        '<p id="fm-e" style="color:#b4491f;min-height:20px;font-size:14px;margin:12px 0 0"></p>' +
        '<button id="fm-b" style="width:100%;padding:13px;font:inherit;font-weight:600;border-radius:10px;border:0;background:var(--accent,#b4491f);color:var(--accent-ink,#fff);cursor:pointer">Connect</button>' +
        '<p style="color:var(--mute,#6f665a);font-size:13px;margin:16px 0 0">The token stays in this browser only. Your family data is stored in a private repo, never in this page.</p>';
      o.appendChild(box); document.body.appendChild(o);
      var e = box.querySelector("#fm-e"); if (note) e.textContent = note;
      var b = box.querySelector("#fm-b");
      var w = lsGet(K_WHO); if (w) box.querySelector("#fm-w").value = w;
      b.onclick = function () {
        var t = box.querySelector("#fm-t").value.trim(); if (!t) { e.textContent = "Paste the token first."; return }
        b.disabled = true; b.textContent = "Checking…"; e.textContent = "";
        check(t).then(function () {
          lsSet(K_TOKEN, t); lsSet(K_WHO, box.querySelector("#fm-w").value);
          o.remove(); resolve();
        }).catch(function (er) { b.disabled = false; b.textContent = "Connect"; e.textContent = er.message || "Could not connect." });
      };
    });
  }
  function ensure() {
    if (lsGet(K_TOKEN)) return Promise.resolve();
    if (!ready) ready = setupScreen().then(function () { ready = null });
    return ready;
  }
  window.addEventListener("fm-auth", function () { if (!ready) { ready = setupScreen("That token stopped working. Paste a new one.").then(function () { ready = null; location.reload() }) } });

  window.FM = { signOut: function () { lsDel(K_TOKEN); location.reload() }, who: me, setWho: function (w) { lsSet(K_WHO, w) } };
  window.claude = {
    use: function (name) {
      if (name === "db") return ensure().then(function () { return db });
      if (name === "user") return ensure().then(function () { return user });
      return Promise.resolve(null);   // no in-page Claude here; requests go through the queue instead
    }
  };
})();
