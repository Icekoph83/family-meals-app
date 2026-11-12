const V="fm-shell-v5";
const SHELL=["./","index.html","fm-db.js","fm-sb.js","manifest.webmanifest","icon-192.png","icon-512.png"];
self.addEventListener("install",e=>{e.waitUntil(caches.open(V).then(c=>c.addAll(SHELL)).then(()=>self.skipWaiting()))});
self.addEventListener("activate",e=>{e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==V).map(k=>caches.delete(k)))).then(()=>self.clients.claim()))});
self.addEventListener("fetch",e=>{
  const u=new URL(e.request.url);
  if(e.request.method!=="GET"||u.origin!==location.origin)return;   // never touch GitHub or Supabase calls
  if(u.searchParams.has("v"))return;                                 // the app's update check must reach the network
  const page=e.request.mode==="navigate"||/\/(index\.html)?$/.test(u.pathname);
  e.respondWith(caches.open(V).then(c=>{
    const net=fetch(e.request).then(r=>{if(r.ok)c.put(e.request,r.clone());return r});
    if(page)return net.catch(()=>c.match(e.request,{ignoreSearch:true}));   // the page itself: newest first, saved copy when offline
    return c.match(e.request,{ignoreSearch:true}).then(hit=>hit?(net.catch(()=>{}),hit):net)}))});
