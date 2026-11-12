/* Family Meals: Supabase backend. Same db/user surface as fm-db.js, but data lives in a per-household
   Postgres table with row level security. Sign in with email and password, then create or join a household. */
(function () {
  "use strict";
  var SB_URL = "https://llxfgkxooclkqaazjqbg.supabase.co";
  var SB_KEY = "sb_publishable_Rb-YmYPM9OgRDu5mSLoyTQ_BoFdW6mT";   // publishable key; safe in the browser, access is enforced by row level security
  var LIB = "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.45.4/dist/umd/supabase.js";

  function clone(x) { return x === undefined ? undefined : JSON.parse(JSON.stringify(x)) }
  function rid() { var a = "abcdefghijklmnopqrstuvwxyz0123456789", s = ""; for (var i = 0; i < 20; i++) s += a[Math.floor(Math.random() * a.length)]; return s }
  function fail(msg, code) { var e = new Error(msg); e.code = code; return e }

  var sb = null, uid = "", email = "", hh = "", hinfo = { name: "", code: "", role: "member" }, members = {};
  var cols = {}, channel = null, pollTimer = null;

  function loadLib() {
    if (window.supabase && window.supabase.createClient) return Promise.resolve();
    return new Promise(function (res, rej) {
      var s = document.createElement("script"); s.src = LIB; s.onload = res; s.onerror = function () { rej(fail("Could not load the sign-in library. Check your connection.", 0)) };
      document.head.appendChild(s);
    });
  }

  // ---------- collections ----------
  function C(name) { if (!cols[name]) cols[name] = { name: name, data: {}, loaded: false, subs: [], queue: Promise.resolve() }; return cols[name] }
  function emit(c) { c.subs.slice().forEach(function (s) { try { s() } catch (e) { console.error(e) } }) }
  function pull(c) {
    return sb.from("docs").select("id,data").eq("household_id", hh).eq("collection", c.name).then(function (r) {
      if (r.error) throw fail(r.error.message, r.error.code || 0);
      var o = {}; (r.data || []).forEach(function (x) { o[x.id] = x.data });
      var changed = JSON.stringify(o) !== JSON.stringify(c.data); c.data = o; c.loaded = true; return changed;
    });
  }
  function applyChange(p) {
    var row = p.eventType === "DELETE" ? p.old : p.new; if (!row || row.household_id !== hh) return;
    var c = cols[row.collection]; if (!c) return;
    if (p.eventType === "DELETE") delete c.data[row.id]; else c.data[row.id] = row.data;
    emit(c);
  }
  function startRealtime() {
    if (channel) return;
    channel = sb.channel("docs-" + hh).on("postgres_changes", { event: "*", schema: "public", table: "docs", filter: "household_id=eq." + hh }, applyChange).subscribe();
    if (!pollTimer) {   // safety net in case the socket drops
      pollTimer = setInterval(function () { if (!document.hidden) Object.keys(cols).forEach(function (n) { var c = cols[n]; if (c.subs.length) pull(c).then(function (ch) { if (ch) emit(c) }).catch(function () {}) }) }, 60000);
      document.addEventListener("visibilitychange", function () { if (!document.hidden) Object.keys(cols).forEach(function (n) { var c = cols[n]; if (c.subs.length) pull(c).then(function (ch) { if (ch) emit(c) }).catch(function () {}) }) });
    }
  }
  function mutate(c, optimistic, remote) {
    try { optimistic(c.data); emit(c) } catch (e) {}
    var p = c.queue.then(remote);
    c.queue = p.catch(function () {});
    return p.then(function () {}, function (e) { return pull(c).catch(function () {}).then(function () { emit(c); throw e }) });
  }
  function must(r) { if (r && r.error) throw fail(r.error.message, r.error.code || 0); return r }
  function snapCol(c, ord, lim) {
    var rows = Object.keys(c.data).map(function (id) { return { id: id, d: c.data[id] } });
    if (ord) rows.sort(function (a, b) { var x = a.d[ord.f], y = b.d[ord.f]; if (x === y) return 0; if (x === undefined) return 1; if (y === undefined) return -1; var r = x < y ? -1 : 1; return ord.dir === "desc" ? -r : r });
    if (lim) rows = rows.slice(0, lim);
    return { docs: rows.map(function (r) { return { id: r.id, data: function () { return clone(r.d) } } }) };
  }
  function subscribe(c, render, errCb) {
    var fn = function () { render() }; c.subs.push(fn);
    if (c.loaded) setTimeout(render, 0);
    pull(c).then(function () { render() }).catch(function (e) { if (!c.loaded) { c.loaded = true; if (errCb) errCb(e) } else render() });
    return function () { c.subs = c.subs.filter(function (s) { return s !== fn }) };
  }
  function collection(name) {
    var c = C(name), ord = null, lim = 0;
    var q = {
      orderBy: function (f, dir) { ord = { f: f, dir: dir || "asc" }; return q },
      limit: function (n) { lim = n; return q },
      onSnapshot: function (cb, err) { return subscribe(c, function () { cb(snapCol(c, ord, lim)) }, err) },
      add: function (data) { var id = rid(); return docRef(name, id).set(data).then(function () { return { id: id } }) },
      doc: function (id) { return docRef(name, id) },
      get: function () { return pull(c).then(function () { return snapCol(c, ord, lim) }) }
    };
    return q;
  }
  function docRef(col, id) {
    var c = C(col);
    return {
      onSnapshot: function (cb, err) { return subscribe(c, function () { cb({ id: id, exists: id in c.data, data: function () { return clone(c.data[id]) } }) }, err) },
      get: function () { return pull(c).then(function () { return { id: id, exists: id in c.data, data: function () { return clone(c.data[id]) } } }) },
      set: function (data) { var d = clone(data); return mutate(c, function (m) { m[id] = clone(d) }, function () { return sb.from("docs").upsert({ household_id: hh, collection: col, id: id, data: d }).then(must) }) },
      update: function (patch) {
        var set = {}, del = []; Object.keys(patch).forEach(function (k) { if (patch[k] && patch[k].__delete__) del.push(k); else set[k] = clone(patch[k]) });
        return mutate(c, function (m) { var cur = m[id] || {}; del.forEach(function (k) { delete cur[k] }); Object.keys(set).forEach(function (k) { cur[k] = clone(set[k]) }); m[id] = cur },
          function () { return sb.rpc("patch_doc", { hid: hh, col: col, did: id, patch: set, deletes: del }).then(must) });
      },
      delete: function () { return mutate(c, function (m) { delete m[id] }, function () { return sb.from("docs").delete().eq("household_id", hh).eq("collection", col).eq("id", id).then(must) }) }
    };
  }
  var db = { collection: collection, doc: function (path) { var p = String(path).split("/"); return docRef(p[0], p.slice(1).join("/")) } };

  // ---------- user ----------
  var user = {
    id: function () { return Promise.resolve(uid) },
    me: function () { return { id: uid, name: members[uid] || email.split("@")[0] } },
    isOwner: function () { return hinfo.role === "owner" },
    canEdit: function () { return true }, can: function () { return true },
    profiles: function (ids) { var o = {}; (ids || []).forEach(function (i) { o[i] = { name: members[i] || "" } }); return Promise.resolve(o) }
  };

  // ---------- photos (private storage bucket, one folder per household) ----------
  var FMPH = {
    put: function (key, b64) {
      var path = hh + "/" + String(key).replace(/[^A-Za-z0-9_-]/g, "_") + ".jpg";
      var bin = atob(b64), arr = new Uint8Array(bin.length); for (var i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
      return sb.storage.from("photos").upload(path, new Blob([arr], { type: "image/jpeg" }), { upsert: true, contentType: "image/jpeg" }).then(function (r) { if (r.error) throw fail(r.error.message, 0); return path });
    },
    get: function (path) { return sb.storage.from("photos").download(path).then(function (r) { if (r.error || !r.data) throw fail("photo", 0); return r.data }) }
  };

  // ---------- screens ----------
  function overlay(build) {
    return new Promise(function (resolve) {
      var o = document.createElement("div");
      o.style.cssText = "position:fixed;inset:0;z-index:99;background:var(--bg,#faf6ef);color:var(--ink,#2b2620);overflow:auto;font:16px/1.45 system-ui,sans-serif";
      var box = document.createElement("div"); box.style.cssText = "max-width:440px;margin:0 auto;padding:36px 20px";
      o.appendChild(box); document.body.appendChild(o);
      build(box, function () { o.remove(); resolve() });
    });
  }
  var INP = "width:100%;box-sizing:border-box;padding:12px;font:inherit;border-radius:10px;border:1px solid var(--line,#e6dccb);background:var(--card,#fff);color:inherit";
  var BTN = "width:100%;padding:13px;font:inherit;font-weight:600;border-radius:10px;border:0;background:var(--accent,#b4491f);color:var(--accent-ink,#fff);cursor:pointer";
  var BTN2 = "width:100%;padding:13px;font:inherit;font-weight:600;border-radius:10px;border:1px solid var(--line,#e6dccb);background:transparent;color:inherit;cursor:pointer;margin-top:8px";
  function field(box, label, id, type, ph) {
    var l = document.createElement("label"); l.textContent = label; l.style.cssText = "display:block;font-size:14px;margin:14px 0 4px"; box.appendChild(l);
    var i = document.createElement("input"); i.id = id; i.type = type || "text"; i.placeholder = ph || ""; i.style.cssText = INP; i.autocapitalize = "off"; i.spellcheck = false; box.appendChild(i); return i;
  }
  function msg(box) { var p = document.createElement("p"); p.style.cssText = "color:#b4491f;min-height:20px;font-size:14px;margin:12px 0 0"; box.appendChild(p); return p }
  function btn(box, text, style) { var b = document.createElement("button"); b.textContent = text; b.style.cssText = style || BTN; box.appendChild(b); return b }
  function head(box, h, sub) { var t = document.createElement("h1"); t.textContent = h; t.style.cssText = "font:700 26px Georgia,serif;margin:0 0 6px"; box.appendChild(t); var p = document.createElement("p"); p.textContent = sub; p.style.cssText = "color:var(--mute,#6f665a);margin:0 0 8px"; box.appendChild(p) }

  function authScreen(note) {
    return overlay(function (box, done) {
      var mode = "in";
      head(box, "Family Meals", "Sign in to your household, or create an account.");
      var e = field(box, "Email", "fm-e", "email"); e.autocomplete = "email";
      var p = field(box, "Password", "fm-p", "password"); p.autocomplete = "current-password";
      var m = msg(box); if (note) m.textContent = note;
      var go = btn(box, "Sign in"); var sw = btn(box, "I need to create an account", BTN2);
      sw.onclick = function () { mode = mode === "in" ? "up" : "in"; go.textContent = mode === "in" ? "Sign in" : "Create account"; sw.textContent = mode === "in" ? "I need to create an account" : "I already have an account"; p.autocomplete = mode === "in" ? "current-password" : "new-password"; m.textContent = "" };
      go.onclick = function () {
        var em = e.value.trim(), pw = p.value; if (!em || !pw) { m.textContent = "Enter your email and password."; return }
        if (mode === "up" && pw.length < 8) { m.textContent = "Use at least 8 characters for the password."; return }
        go.disabled = true; m.textContent = "";
        var call = mode === "in" ? sb.auth.signInWithPassword({ email: em, password: pw }) : sb.auth.signUp({ email: em, password: pw });
        call.then(function (r) {
          go.disabled = false;
          if (r.error) { m.textContent = r.error.message; return }
          if (!r.data || !r.data.session) { m.style.color = "var(--mute,#6f665a)"; m.textContent = "Check your email to confirm your address, then sign in."; mode = "in"; go.textContent = "Sign in"; return }
          done();
        }).catch(function (er) { go.disabled = false; m.textContent = er.message || "Could not sign in." });
      };
    });
  }
  function householdScreen() {
    return overlay(function (box, done) {
      head(box, "Set up your household", "A household is the group that shares one meal plan, pantry and grocery list.");
      var h1 = document.createElement("h2"); h1.textContent = "Start a new household"; h1.style.cssText = "font-size:18px;margin:22px 0 0"; box.appendChild(h1);
      var hn = field(box, "Household name", "fm-hn", "text", "The Smiths"); var yn = field(box, "Your name", "fm-yn", "text", "Alex");
      var m1 = msg(box); var b1 = btn(box, "Create household");
      var h2 = document.createElement("h2"); h2.textContent = "Or join one"; h2.style.cssText = "font-size:18px;margin:26px 0 0"; box.appendChild(h2);
      var ic = field(box, "Invite code", "fm-ic", "text", "12 characters"); var yn2 = field(box, "Your name", "fm-yn2", "text", "Sam");
      var m2 = msg(box); var b2 = btn(box, "Join household", BTN2);
      b1.onclick = function () { if (!hn.value.trim() || !yn.value.trim()) { m1.textContent = "Enter a household name and your name."; return }
        b1.disabled = true; sb.rpc("create_household", { hname: hn.value.trim(), dname: yn.value.trim() }).then(function (r) { b1.disabled = false; if (r.error) { m1.textContent = r.error.message; return } done() }) };
      b2.onclick = function () { if (!ic.value.trim() || !yn2.value.trim()) { m2.textContent = "Enter the invite code and your name."; return }
        b2.disabled = true; sb.rpc("join_household", { code: ic.value.trim(), dname: yn2.value.trim() }).then(function (r) { b2.disabled = false; if (r.error) { m2.textContent = /too many/i.test(r.error.message) ? "Too many tries. Wait an hour and check the code with whoever invited you." : r.error.message; return } if (!r.data) { m2.textContent = "That invite code didn't match a household."; return } done() }) };
    });
  }

  // ---------- startup ----------
  function loadHousehold() {
    return sb.from("household_members").select("household_id,role,display_name,user_id").then(function (r) {
      if (r.error) throw fail(r.error.message, 0);
      var mine = (r.data || []).filter(function (x) { return x.user_id === uid });
      if (!mine.length) return false;
      hh = mine[0].household_id; hinfo.role = mine[0].role;
      members = {}; (r.data || []).filter(function (x) { return x.household_id === hh }).forEach(function (x) { members[x.user_id] = x.display_name });
      return sb.from("households").select("name,invite_code").eq("id", hh).single().then(function (h) { if (h.data) { hinfo.name = h.data.name; hinfo.code = h.data.invite_code } return true });
    });
  }
  var ready = null;
  function ensure() {
    if (ready) return ready;
    ready = loadLib().then(function () {
      sb = window.supabase.createClient(SB_URL, SB_KEY, { auth: { persistSession: true, autoRefreshToken: true, storage: window.localStorage } });
      return sb.auth.getSession();
    }).then(function (r) {
      var s = r && r.data && r.data.session; if (s) return s;
      return authScreen().then(function () { return sb.auth.getSession().then(function (x) { return x.data.session }) });
    }).then(function (s) {
      uid = s.user.id; email = s.user.email || "";
      sb.auth.onAuthStateChange(function (ev) { if (ev === "SIGNED_OUT") location.reload() });
      return loadHousehold();
    }).then(function (has) {
      if (has) return;
      return householdScreen().then(loadHousehold);
    }).then(function () { startRealtime() });
    return ready;
  }

  window.FM = {
    signOut: function () { (sb ? sb.auth.signOut() : Promise.resolve()).then(function () { location.reload() }) },
    who: function () { return uid },
    info: function () { return ensure().then(function () { return { household: hinfo.name, code: hinfo.code, role: hinfo.role, email: email, members: Object.keys(members).map(function (k) { return members[k] }) } }) }
  };
  Object.defineProperty(window, "FMPH", { get: function () { return sb && hh ? FMPH : null } });
  window.claude = {
    use: function (name) {
      if (name === "db") return ensure().then(function () { return db });
      if (name === "user") return ensure().then(function () { return user });
      return Promise.resolve(null);
    }
  };
})();
