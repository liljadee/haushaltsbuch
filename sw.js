/*
 * Service Worker des Haushaltsbuchs (Bauplan 8.2), beim Build aus scripts/sw-template.js erzeugt.
 * Lädt alle Programmdateien bei der Installation vorab; danach läuft die App ohne Netz.
 * Eine neue Version wartet, bis die Nutzerin „Jetzt aktualisieren“ antippt (Bauplan 8.3).
 */
const VERSION = '0.6.8-3e9d6a907960';
const FILES = ["index.html","app.js","manifest.webmanifest","icon.png","neu.png"];
const CACHE = `haushaltsbuch-${VERSION}`;

self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE).then((cache) => cache.addAll(FILES.map((f) => new Request(f, { cache: 'reload' })))));
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    (async () => {
      for (const key of await caches.keys()) if (key.startsWith('haushaltsbuch-') && key !== CACHE) await caches.delete(key);
      await self.clients.claim();
    })(),
  );
});

self.addEventListener('message', (event) => {
  if (event.data === 'SKIP_WAITING') self.skipWaiting();
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET' || new URL(req.url).origin !== self.location.origin) return;
  event.respondWith(
    (async () => {
      const cache = await caches.open(CACHE);
      // Jede Seitenadresse (auch ./?start=neue-buchung) bekommt index.html; die App wertet die Adresse aus.
      if (req.mode === 'navigate') return (await cache.match('index.html')) ?? fetch(req);
      return (await cache.match(req, { ignoreSearch: true })) ?? fetch(req);
    })(),
  );
});
