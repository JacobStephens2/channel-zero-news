// Kill-switch service worker for the cutover from the old PHP PWA.
// The previous app registered /sw.js; returning visitors still have it
// controlling their page and could be served stale cached PHP assets. This
// replacement clears all caches and unregisters itself so the new realtime
// client loads cleanly. New visitors never register a service worker at all.
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.map((k) => caches.delete(k)));
    await self.registration.unregister();
    const clients = await self.clients.matchAll();
    clients.forEach((c) => c.navigate(c.url));
  })());
});
