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

// ---- НАСТРОЙКИ СТАБИЛЬНОСТИ ----
const RUN_TIMEOUT_MS = 120_000;        // бюджет на 1 run (внутри вернём partial, если не успели)
const INFLIGHT_STALE_MS = 3 * 60_000; // если "в работе" > 3 мин — считаем зависшим
const WORKER_BATCH = 6;               // сколько run-ов брать за раз из очереди

// ✅ ВАЖНО: ставим 1 для стабильности. Потом можно попробовать 2.
const WORKER_PARALLEL = 2;

// в lowestFromRun сейчас последовательный обход участников, PER_RUN_PARALLEL не используется,
// но оставим на будущее
const PER_RUN_PARALLEL = 2;

// если partial — попробуем дорефайнить позже (пару раз)
const PARTIAL_RETRY_MAX = 2;
const PARTIAL_RETRY_DELAY_MS = 30_000;
const partialAttempts = new Map(); // runUrl -> attempts

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

function setC(k, val, ttlMs = TTL_MS) {
  cache.set(k, { val, exp: now() + ttlMs });
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
    typeof regionRaw === "object" ? (regionRaw.slug || realmRaw?.name || "") : regionRaw
  )
    .toLowerCase()
    .trim();
  if (regions.includes(s)) return s;
  return "";
}

// base64 websafe (Apps Script) -> utf8 (для legacy GET)
function decodeBase64WebSafeToUtf8(enc) {
  const s = String(enc || "");
  let b64 = s.replace(/-/g, "+").replace(/_/g, "/");
  while (b64.length % 4) b64 += "=";
  return Buffer.from(b64, "base64").toString("utf8");
}

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

function isBrowserClosedError(msg) {
  const s = String(msg || "");
  return /Target page, context or browser has been closed|Target closed|browser has disconnected|Protocol error/i.test(
    s
  );
}

// --- Playwright singleton ---
let browserPromise = null;
let contextPromise = null;

// ✅ lock, чтобы reset не запускался одновременно
let resetLock = null;

async function resetBrowser() {
  try {
    if (browserPromise) {
      const b = await browserPromise;
      await b.close();
    }
  } catch {}
  browserPromise = null;
  contextPromise = null;
}

async function ensureResetBrowser() {
  if (resetLock) return resetLock;
  resetLock = (async () => {
    await resetBrowser();
  })();
  try {
    await resetLock;
  } finally {
    resetLock = null;
  }
}

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

/**
 * Парсим все "X+ Keystone Timed Runs" и суммируем.
 */
function parseTimedRunsAll(text) {
  const t = String(text || "").replace(/\u00a0/g, " ");
  const by = {};

  const p1 = /(\d[\d,]*)\s+(\d{1,2}\+)\s+Keystone\s+Timed\s+Runs/gi;
  const p2 = /(\d{1,2}\+)\s+Keystone\s+Timed\s+Runs\s+(\d[\d,]*)/gi;

  let m;
  while ((m = p1.exec(t)) !== null) {
    const count = Number(String(m[1]).replace(/,/g, ""));
    const level = m[2];
    if (Number.isFinite(count)) by[level] = Math.max(by[level] || 0, count);
  }
  while ((m = p2.exec(t)) !== null) {
    const level = m[1];
    const count = Number(String(m[2]).replace(/,/g, ""));
    if (Number.isFinite(count)) by[level] = Math.max(by[level] || 0, count);
  }

  const keys = Object.keys(by);
  if (!keys.length) return null;

  const total = keys.reduce((s, k) => s + (by[k] || 0), 0);
  return { total, by };
}

async function fetchTimedTotal(context, realm, name, regionHint) {
  const ck = `tt:${(regionHint || "").toLowerCase()}:${realm}:${name}`.toLowerCase();
  const cached = getC(ck);
  if (cached !== null) return cached; // объект или null

  const tryRegions = [];
  if (regionHint && regions.includes(regionHint)) tryRegions.push(regionHint);
  for (const r of regions) if (!tryRegions.includes(r)) tryRegions.push(r);

  for (const region of tryRegions) {
    // если контекст умер — этот вызов упадёт и улетит в воркер catch
    const page = await context.newPage();
    page.setDefaultTimeout(15000);
    page.setDefaultNavigationTimeout(15000);

    const url = `https://raider.io/characters/${region}/${encodeURIComponent(realm)}/${encodeURIComponent(
      name
    )}`;

    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });

      try {
        await page.waitForSelector('text=/Keystone\\s+Timed\\s+Runs/i', { timeout: 6000 });
      } catch (_) {}

      const bodyText = await page.evaluate(() => document.body?.innerText || "");
      const parsed = parseTimedRunsAll(bodyText);

      if (parsed) {
        setC(ck, parsed);
        return parsed;
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

/**
 * partial вместо run_timeout:
 * - есть дедлайн
 * - успели часть -> partial:true
 * - успели всех -> partial:false
 */
async function lowestFromRun(context, runUrl) {
  const runCacheKey = `run:${runUrl}`;
  const cached = getC(runCacheKey);
  if (cached) return cached;

  const roster = await fetchRunRoster(runUrl);

  const deadline = Date.now() + (RUN_TIMEOUT_MS - 3000);
  let best = null;
  let done = 0;

  for (const p of roster) {
    if (Date.now() > deadline) break;

    const stat = await fetchTimedTotal(context, p.realm, p.name, p.region);
    done++;

    if (!stat) continue;
    if (!best || stat.total < best.stat.total) best = { p, stat };
  }

  const partial = done < roster.length;

  const out = best
    ? {
        ok: true,
        lowest: `${best.p.name}-${best.p.realm}`.toLowerCase(),
        timed_total: best.stat.total,
        timed_breakdown: best.stat.by,
        partial,
      }
    : { ok: false, error: "no timed totals found", partial: true };

  setC(runCacheKey, out, partial ? 20 * 60 * 1000 : TTL_MS);
  return out;
}

/* =======================
   Очередь + воркер
   ======================= */
const runQueue = [];
const queued = new Set();
const inflight = new Map(); // url -> startedAt
let workerRunning = false;
let lastProgressAt = now();

function enqueueRuns(urls, force = false) {
  const t = now();
  for (const u0 of urls) {
    const u = String(u0 || "").trim();
    if (!u) continue;

    const cached = getC(`run:${u}`);
    if (cached && cached.ok && !cached.partial && !force) continue;

    const startedAt = inflight.get(u);
    if (startedAt && t - startedAt > INFLIGHT_STALE_MS) {
      inflight.delete(u);
    }

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
    let context = await getContext();

    while (runQueue.length) {
      const batch = runQueue.splice(0, WORKER_BATCH);
      for (const u of batch) {
        queued.delete(u);
        inflight.set(u, now());
      }

      await mapLimit(batch, WORKER_PARALLEL, async (u) => {
        try {
          const out = await lowestFromRun(context, u);
          setC(`run:${u}`, out, out.partial ? 20 * 60 * 1000 : TTL_MS);
          lastProgressAt = now();

          if (out.ok && out.partial) {
            const n = partialAttempts.get(u) || 0;
            if (n < PARTIAL_RETRY_MAX) {
              partialAttempts.set(u, n + 1);
              setTimeout(() => enqueueRuns([u], true), PARTIAL_RETRY_DELAY_MS);
            }
          }
        } catch (e) {
          const msg = String(e?.message || e);

          // ✅ если браузер/контекст закрыт — НЕ кэшируем это как результат
          // просто сбрасываем браузер и переочередим этот run
          if (isBrowserClosedError(msg)) {
            await ensureResetBrowser();
            context = await getContext();
            // переочередим чуть позже
            setTimeout(() => enqueueRuns([u], true), 2000);
          } else {
            setC(`run:${u}`, { ok: false, error: msg, partial: true }, 10 * 60 * 1000);
          }

          lastProgressAt = now();
        } finally {
          inflight.delete(u);
        }
      });
    }
  })()
    .catch(async (e) => {
      console.error("worker fatal:", e);
      for (const [u] of inflight.entries()) {
        inflight.delete(u);
        if (!queued.has(u)) {
          queued.add(u);
          runQueue.push(u);
        }
      }
      await ensureResetBrowser();
    })
    .finally(() => {
      workerRunning = false;
      if (runQueue.length) startWorker();
    });
}

// watchdog: если воркер завис — перезапустим
setInterval(() => {
  const t = now();
  if (workerRunning && t - lastProgressAt > INFLIGHT_STALE_MS) {
    console.log("watchdog: no progress, restarting worker");
    for (const [u] of inflight.entries()) {
      inflight.delete(u);
      if (!queued.has(u)) {
        queued.add(u);
        runQueue.push(u);
      }
    }
    workerRunning = false;
    ensureResetBrowser().finally(() => startWorker());
  }
}, 30_000);

/* =======================
   Batch FAST / FULL
   ======================= */
function processBatchFast(urls) {
  const clean = urls.map((u) => (u ? String(u).trim() : "")).slice(0, 300);

  const results = new Array(clean.length);
  const missing = [];
  const needRefine = [];

  for (let i = 0; i < clean.length; i++) {
    const u = clean[i];
    if (!u) {
      results[i] = { ok: false, error: "empty url", partial: true };
      continue;
    }

    const cached = getC(`run:${u}`);

    if (cached) {
      // ✅ если в кэше лежит "browser closed" — удаляем и считаем как PENDING
      if (!cached.ok && isBrowserClosedError(cached.error)) {
        cache.delete(`run:${u}`);
        results[i] = { ok: false, error: "PENDING", partial: true };
        missing.push(u);
      } else {
        results[i] = cached;
        if (cached.ok && cached.partial) needRefine.push(u);
      }
    } else {
      results[i] = { ok: false, error: "PENDING", partial: true };
      missing.push(u);
    }
  }

  if (missing.length) enqueueRuns(missing);
  if (needRefine.length) enqueueRuns(needRefine, true);

  return results;
}

async function processBatchFull(urls) {
  const clean = urls.map((u) => (u ? String(u).trim() : "")).slice(0, 300);
  const context = await getContext();

  const out = await mapLimit(clean, 2, async (u) => {
    if (!u) return { ok: false, error: "empty url", partial: true };
    try {
      return await lowestFromRun(context, u);
    } catch (e) {
      return { ok: false, error: String(e.message || e), partial: true };
    }
  });

  return out;
}

/* =======================
   Endpoints
   ======================= */
app.get("/health", (_, res) => res.json({ ok: true }));

app.get("/debug", (_, res) => {
  res.json({
    ok: true,
    cacheSize: cache.size,
    queueLen: runQueue.length,
    queued: queued.size,
    inflight: inflight.size,
    workerRunning,
    lastProgressSecAgo: Math.round((now() - lastProgressAt) / 1000),
  });
});

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
