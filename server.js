import express from "express";
import fetch from "node-fetch";
import { chromium } from "playwright";

const app = express();
const PORT = process.env.PORT || 3000;

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

// устойчивый парсер 5+ Timed Runs
function parseTimed5(text) {
  const t = String(text || "").replace(/\u00a0/g, " ");

  // число слева
  let m = t.match(/(\d[\d,]*)\s+5\+\s+Keystone\s+Timed\s+Runs/i);
  if (m) return Number(String(m[1]).replace(/,/g, ""));

  // число справа
  m = t.match(/5\+\s+Keystone\s+Timed\s+Runs\s+(\d[\d,]*)/i);
  if (m) return Number(String(m[1]).replace(/,/g, ""));

  m = t.match(/(\d[\d,]*)\s+5\+\s+Timed\s+Runs/i);
  if (m) return Number(String(m[1]).replace(/,/g, ""));

  m = t.match(/5\+\s+Timed\s+Runs\s+(\d[\d,]*)/i);
  if (m) return Number(String(m[1]).replace(/,/g, ""));

  const idx = t.toLowerCase().indexOf("timed runs");
  if (idx !== -1) {
    const slice = t.slice(Math.max(0, idx - 250), idx + 250);
    m = slice.match(/(\d[\d,]*)\s+5\+/i);
    if (m) return Number(String(m[1]).replace(/,/g, ""));
  }

  return null;
}

// --- лимитер параллелизма ---
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

// --- Playwright singleton ---
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

// --- run-details roster ---
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

// --- timed5 по персонажу ---
async function fetchTimed5(context, realm, name, regionHint) {
  const ck = `t5:${(regionHint || "").toLowerCase()}:${realm}:${name}`.toLowerCase();
  const cached = getC(ck);
  if (cached !== null) return cached;

  const tryRegions = [];
  if (regionHint && regions.includes(regionHint)) tryRegions.push(regionHint);
  for (const r of regions) if (!tryRegions.includes(r)) tryRegions.push(r);

  for (const region of tryRegions) {
    const page = await context.newPage();
    page.setDefaultTimeout(15000);
    page.setDefaultNavigationTimeout(15000);

    const url = `https://raider.io/characters/${region}/${encodeURIComponent(realm)}/${encodeURIComponent(
      name
    )}`;

    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });

      let txt = null;
      try {
        const loc = page.locator("text=/5\\+\\s+Keystone\\s+Timed\\s+Runs/i").first();
        await loc.waitFor({ timeout: 4000 });
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

// ✅ Lowest для одного run (null считаем как 0)
async function lowestFromRun(context, runUrl) {
  const runCacheKey = `run:${runUrl}`;
  const cached = getC(runCacheKey);
  if (cached) return cached;

  const roster = await fetchRunRoster(runUrl);

  // параллелим timed5 внутри 1 run, но аккуратно (чтобы не убить память)
  const t5List = await mapLimit(roster, 3, async (p) => {
    const v = await fetchTimed5(context, p.realm, p.name, p.region);
    return { p, t5: v === null ? 0 : v };
  });

  let best = null;
  for (const it of t5List) {
    if (!best || it.t5 < best.t5) best = it;
  }

  const out = {
    ok: true,
    lowest: `${best.p.name}-${best.p.realm}`.toLowerCase(),
    timed5: best.t5,
  };

  setC(runCacheKey, out);
  return out;
}

/* =========================================================
   ✅ Очередь прогрева (чтобы 100 ссылок не стояли PENDING вечность)
   ========================================================= */
const runQueue = [];
const queued = new Set();
const inflight = new Set();
let workerRunning = false;

function enqueueRuns(urls) {
  for (const u0 of urls) {
    const u = String(u0 || "").trim();
    if (!u) continue;
    if (getC(`run:${u}`)) continue;        // уже готово
    if (queued.has(u) || inflight.has(u)) continue;
    queued.add(u);
    runQueue.push(u);
  }
  startWorker();
}

async function startWorker() {
  if (workerRunning) return;
  workerRunning = true;

  (async () => {
    const context = await getContext();

    while (runQueue.length) {
      // берем небольшими пачками
      const batch = runQueue.splice(0, 6);
      for (const u of batch) {
        queued.delete(u);
        inflight.add(u);
      }

      // считаем ограниченной параллельностью
      await mapLimit(batch, 2, async (u) => {
        try {
          await lowestFromRun(context, u);
        } catch (e) {
          setC(`run:${u}`, { ok: false, error: String(e.message || e) });
        } finally {
          inflight.delete(u);
        }
      });
    }
  })()
    .catch((e) => console.error("worker fatal:", e))
    .finally(() => {
      workerRunning = false;
      // если пока работали добавили ещё — запустимся снова
      if (runQueue.length) startWorker();
    });
}

/* =========================================================
   Batch FAST / FULL
   ========================================================= */
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

  // ✅ добавляем в очередь и сразу отдаём ответ
  if (missing.length) enqueueRuns(missing);

  return results;
}

async function processBatchFull(urls) {
  const clean = urls.map((u) => (u ? String(u).trim() : "")).slice(0, 300);
  const context = await getContext();

  // считаем всё до конца (для ручной отладки, не для Sheets)
  const out = await mapLimit(clean, 2, async (u) => {
    if (!u) return { ok: false, error: "empty url" };
    try {
      return await lowestFromRun(context, u);
    } catch (e) {
      return { ok: false, error: String(e.message || e) };
    }
  });

  return out;
}

/* =========================================================
   Endpoints
   ========================================================= */
app.get("/health", (_, res) => res.json({ ok: true }));

// ✅ смотреть прогресс
app.get("/debug", (_, res) => {
  res.json({
    ok: true,
    cacheSize: cache.size,
    queueLen: runQueue.length,
    queued: queued.size,
    inflight: inflight.size,
    workerRunning,
  });
});

// одиночный GET (для проверки)
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

// batch GET (legacy)
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

    const results = await processBatchFull(urls);
    return res.json({ ok: true, results });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

// ✅ batch POST: по умолчанию FAST, можно ?full=1
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
