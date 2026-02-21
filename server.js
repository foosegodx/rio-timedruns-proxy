import express from "express";
import fetch from "node-fetch";
import { chromium } from "playwright";

const app = express();
const PORT = process.env.PORT || 3000;

// ✅ Нужно для POST JSON
app.use(express.json({ limit: "1mb" }));

const cache = new Map();
const TTL_MS = 6 * 60 * 60 * 1000; // 6 часов
const regions = ["eu", "us", "kr", "tw", "cn"];

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

function now() {
  return Date.now();
}
function getC(k) {
  const v = cache.get(k);
  if (!v) return null;
  if (now() > v.exp) {
    cache.delete(k);
    return null;
  }
  return v.val;
}
function setC(k, val) {
  cache.set(k, { val, exp: now() + TTL_MS });
}

function normalizeRealm(realmRaw) {
  if (!realmRaw) return "";
  let s = "";
  if (typeof realmRaw === "string") s = realmRaw;
  else if (typeof realmRaw === "object") s = realmRaw.slug || realmRaw.name || "";
  else s = String(realmRaw);

  s = String(s).toLowerCase().trim();
  s = s.replace(/['’]/g, "");
  s = s.replace(/\s+/g, "-");
  s = s.replace(/[^a-z0-9-]/g, "-");
  s = s.replace(/-+/g, "-");
  s = s.replace(/^-|-$/g, "");
  return s;
}

function normalizeRegion(regionRaw) {
  if (!regionRaw) return "";
  const s = String(
    typeof regionRaw === "object" ? (regionRaw.slug || regionRaw.name || "") : regionRaw
  )
    .toLowerCase()
    .trim();
  if (regions.includes(s)) return s;
  return "";
}

// base64 websafe (Apps Script) -> utf8
function decodeBase64WebSafeToUtf8(enc) {
  const s = String(enc || "");
  let b64 = s.replace(/-/g, "+").replace(/_/g, "/");
  while (b64.length % 4) b64 += "=";
  return Buffer.from(b64, "base64").toString("utf8");
}

// устойчивый парсер 5+ Timed Runs
function parseTimed5(text) {
  const t = String(text || "").replace(/\u00a0/g, " ");

  let m = t.match(/(\d[\d,]*)\s+5\+\s+Keystone\s+Timed\s+Runs/i);
  if (m) return Number(String(m[1]).replace(/,/g, ""));

  m = t.match(/(\d[\d,]*)\s+5\+\s+Timed\s+Runs/i);
  if (m) return Number(String(m[1]).replace(/,/g, ""));

  const idx = t.toLowerCase().indexOf("timed runs");
  if (idx !== -1) {
    const slice = t.slice(Math.max(0, idx - 250), idx + 250);
    m = slice.match(/(\d[\d,]*)\s+5\+/i);
    if (m) return Number(String(m[1]).replace(/,/g, ""));
  }

  m = t.match(/(\d[\d,]*)\s+5\+\s+.*?\s+Runs/i);
  if (m) return Number(String(m[1]).replace(/,/g, ""));

  return null;
}

// --- простой лимитер параллелизма без зависимостей ---
async function mapLimit(items, limit, fn) {
  const arr = Array.from(items);
  const out = new Array(arr.length);
  let cursor = 0;

  const workers = new Array(Math.min(limit, arr.length)).fill(0).map(async () => {
    while (true) {
      const i = cursor++;
      if (i >= arr.length) break;
      out[i] = await fn(arr[i], i);
    }
  });

  await Promise.all(workers);
  return out;
}

// --- Playwright singleton (browser + context) ---
let browserPromise = null;
let contextPromise = null;

async function getContext() {
  if (contextPromise) return contextPromise;

  contextPromise = (async () => {
    browserPromise =
      browserPromise ||
      chromium.launch({
        headless: true,
        args: ["--no-sandbox", "--disable-dev-shm-usage"],
      });

    const browser = await browserPromise;
    const context = await browser.newContext({ userAgent: UA });

    await context.route("**/*", (route) => {
      const rt = route.request().resourceType();
      if (!["document", "script", "xhr", "fetch"].includes(rt)) return route.abort();
      return route.continue();
    });

    return context;
  })();

  return contextPromise;
}

async function shutdown() {
  try {
    if (browserPromise) {
      const b = await browserPromise;
      await b.close();
    }
  } catch {}
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

// --- Raider.IO run-details roster ---
async function fetchRunRoster(runUrl) {
  const m = String(runUrl).match(/\/mythic-plus-runs\/(season-[^/]+)\/(\d+)-/i);
  if (!m) throw new Error("Bad URL");
  const season = m[1];
  const runId = m[2];

  const apiUrl =
    "https://raider.io/api/v1/mythic-plus/run-details" +
    `?season=${encodeURIComponent(season)}` +
    `&id=${encodeURIComponent(runId)}`;

  const resp = await fetch(apiUrl, { headers: { "user-agent": UA } });
  if (!resp.ok) throw new Error(`run-details ${resp.status}`);
  const data = await resp.json();

  const rosterRaw = data.roster || data?.run?.roster || [];
  if (!rosterRaw.length) throw new Error("No roster");

  const roster = rosterRaw
    .map((row) => row.character || row)
    .filter(Boolean)
    .map((c) => {
      const name = c.name;

      const realmRaw =
        c.realmSlug ||
        c.realm ||
        c.realm_name ||
        c.server ||
        (c.realm && (c.realm.slug || c.realm.name)) ||
        null;

      const regionRaw = c.region || c.regionSlug || c.region_name || null;

      const realm = normalizeRealm(realmRaw);
      const region = normalizeRegion(regionRaw);

      if (!name || !realm) return null;
      return { name: String(name), realm, region };
    })
    .filter(Boolean);

  if (!roster.length) throw new Error("No usable roster");
  return roster;
}

function charKey(c) {
  return `${(c.region || "").toLowerCase()}:${c.realm.toLowerCase()}:${c.name.toLowerCase()}`;
}

// --- Получение timed5 по персонажу ---
async function fetchTimed5(context, realm, name, regionHint) {
  const ck = `t5:${(regionHint || "").toLowerCase()}:${realm}:${name}`.toLowerCase();
  const cached = getC(ck);
  if (cached !== null) return cached;

  const tryRegions = [];
  if (regionHint && regions.includes(regionHint)) tryRegions.push(regionHint);
  for (const r of regions) if (!tryRegions.includes(r)) tryRegions.push(r);

  for (const region of tryRegions) {
    const page = await context.newPage();
    page.setDefaultTimeout(12000);
    page.setDefaultNavigationTimeout(12000);

    const url = `https://raider.io/characters/${region}/${encodeURIComponent(realm)}/${encodeURIComponent(
      name
    )}`;

    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 12000 });

      let txt = null;
      try {
        const loc = page.locator("text=/5\\+\\s+Keystone\\s+Timed\\s+Runs/i").first();
        await loc.waitFor({ timeout: 5000 });
        txt = await loc.textContent({ timeout: 2000 });
      } catch {}

      let v = txt ? parseTimed5(txt) : null;

      if (v === null) {
        const bodyText = await page.evaluate(() => document.body?.innerText || "");
        v = parseTimed5(bodyText);
      }

      if (typeof v === "number" && Number.isFinite(v)) {
        setC(ck, v);
        return v;
      }
    } catch {
      // ignore
    } finally {
      await page.close();
    }
  }

  setC(ck, null);
  return null;
}

// --- Lowest для одного run ---
async function lowestFromRun(context, runUrl, precomputedT5Map = null) {
  const runCacheKey = `run:${runUrl}`;
  const cached = getC(runCacheKey);
  if (cached) return cached;

  const roster = await fetchRunRoster(runUrl);

  let best = null;
  for (const p of roster) {
    const k = charKey(p);
    const t5 =
      precomputedT5Map && precomputedT5Map.has(k)
        ? precomputedT5Map.get(k)
        : await fetchTimed5(context, p.realm, p.name, p.region);

    if (t5 === null) continue;
    if (!best || t5 < best.t5) best = { name: p.name, realm: p.realm, t5 };
  }

  const out = best
    ? { ok: true, lowest: `${best.name}-${best.realm}`.toLowerCase(), timed5: best.t5 }
    : { ok: false, error: "no timed5 found" };

  setC(runCacheKey, out);
  return out;
}

// --- Полный batch (может быть долгим) ---
async function processBatchFull(urls) {
  const clean = urls.map((u) => (u ? String(u).trim() : "")).slice(0, 300);
  const context = await getContext();

  const results = new Array(clean.length).fill(null);
  const todo = [];

  for (let i = 0; i < clean.length; i++) {
    const u = clean[i];
    if (!u) {
      results[i] = { ok: false, error: "empty url" };
      continue;
    }
    const cached = getC(`run:${u}`);
    if (cached) results[i] = cached;
    else todo.push({ i, u });
  }

  if (!todo.length) return results;

  const rosterPacked = await mapLimit(todo, 10, async ({ i, u }) => {
    try {
      const roster = await fetchRunRoster(u);
      return { i, u, ok: true, roster };
    } catch (e) {
      return { i, u, ok: false, error: String(e.message || e) };
    }
  });

  const uniqueChars = new Map();
  for (const r of rosterPacked) {
    if (!r.ok) continue;
    for (const c of r.roster) uniqueChars.set(charKey(c), c);
  }

  const t5Map = new Map();
  await mapLimit(uniqueChars.values(), 4, async (c) => {
    const v = await fetchTimed5(context, c.realm, c.name, c.region);
    t5Map.set(charKey(c), v);
  });

  for (const r of rosterPacked) {
    if (!r.ok) {
      results[r.i] = { ok: false, error: r.error };
      continue;
    }
    try {
      const out = await lowestFromRun(context, r.u, t5Map);
      results[r.i] = out;
    } catch (e) {
      results[r.i] = { ok: false, error: String(e.message || e) };
    }
  }

  return results;
}

// --- Быстрый batch: отдаём cache/PENDING и греем фон ---
const inflightWarm = new Set();

function warmInBackground(missingUrls) {
  const uniq = [];
  for (const u of missingUrls) {
    if (!u) continue;
    if (inflightWarm.has(u)) continue;
    inflightWarm.add(u);
    uniq.push(u);
  }
  if (!uniq.length) return;

  (async () => {
    try {
      await processBatchFull(uniq);
    } catch (e) {
      console.error("warm error:", e);
    } finally {
      for (const u of uniq) inflightWarm.delete(u);
    }
  })();
}

function processBatchFast(urls) {
  const clean = urls.map((u) => (u ? String(u).trim() : "")).slice(0, 300);

  const results = new Array(clean.length);
  const missing = [];

  for (let i = 0; i < clean.length; i++) {
    const u = clean[i];
    if (!u) {
      results[i] = { ok: false, error: "empty url" };
      continue;
    }
    const cached = getC(`run:${u}`);
    if (cached) results[i] = cached;
    else {
      results[i] = { ok: false, error: "PENDING" };
      missing.push(u);
    }
  }

  // стартуем прогрев и сразу отдаём ответ
  if (missing.length) warmInBackground(missing);

  return results;
}

app.get("/health", (_, res) => res.json({ ok: true }));

// одиночный GET (для ручной проверки; может быть долгим)
app.get("/lowest-from-run", async (req, res) => {
  const runUrl = req.query.url;
  if (!runUrl) return res.status(400).json({ ok: false, error: "missing url" });

  try {
    const context = await getContext();
    const out = await lowestFromRun(context, String(runUrl).trim());
    return res.json(out);
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

// batch GET (старый; может упираться в длину URL)
app.get("/lowest-from-runs-get", async (req, res) => {
  try {
    const enc = req.query.urls;
    if (!enc) return res.status(400).json({ ok: false, error: "missing urls param" });

    let urls;
    try {
      const json = decodeBase64WebSafeToUtf8(enc);
      urls = JSON.parse(json);
    } catch {
      return res.status(400).json({ ok: false, error: "bad base64/json" });
    }

    if (!Array.isArray(urls)) {
      return res.status(400).json({ ok: false, error: "urls must be array" });
    }

    // ⚠️ GET оставляем как full (может быть долгим)
    const results = await processBatchFull(urls);
    return res.json({ ok: true, results });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

// ✅ batch POST: по умолчанию FAST (cache/PENDING), но можно ?full=1
app.post("/lowest-from-runs", async (req, res) => {
  try {
    const full = String(req.query.full || "") === "1";
    const urls = Array.isArray(req.body) ? req.body : req.body?.urls;

    if (!Array.isArray(urls)) {
      return res.status(400).json({ ok: false, error: "body must be array or { urls: array }" });
    }

    const results = full ? await processBatchFull(urls) : processBatchFast(urls);
    return res.json({ ok: true, results });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

app.listen(PORT, () => console.log("listening on", PORT));
