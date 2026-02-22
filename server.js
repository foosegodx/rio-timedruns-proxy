// server.js
import express from "express";
import fetch from "node-fetch";
import { chromium } from "playwright";

const app = express();
const PORT = process.env.PORT || 3000;

// ✅ чтобы спокойно принимать 1500 ссылок
app.use(express.json({ limit: "5mb" }));

// ✅ bump версии кэша (чтобы обновились discord/warband)
// (можешь оставить v14, но при изменениях удобнее bump)
const CACHE_VER = "v15";

const cache = new Map();
const TTL_MS = 6 * 60 * 60 * 1000; // 6 часов
const regions = ["eu", "us", "kr", "tw", "cn"];

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

// ---- НАСТРОЙКИ ----
const RUN_TIMEOUT_MS = 140_000;
const INFLIGHT_STALE_MS = 3 * 70_000;

// скорость
const WORKER_BATCH = 6;
const WORKER_PARALLEL = 2;

// partial — попробуем дорефайнить позже
const PARTIAL_RETRY_MAX = 2;
const PARTIAL_RETRY_DELAY_MS = 30_000;
const partialAttempts = new Map(); // runUrl -> attempts

// warband попытка (чтобы не зависать)
const WAR_BAND_TIMEOUT_MS = 30_000;

// ✅ лимит количества URL в одном батче
const MAX_URLS = 1500;

const runKey = (u) => `run:${CACHE_VER}:${u}`;
const ttKey = (regionHint, realm, name) =>
  `tt:${CACHE_VER}:${(regionHint || "").toLowerCase()}:${String(realm)}:${String(name)}`.toLowerCase();
const wbKey = (region, realm, name) =>
  `wb:${CACHE_VER}:${region}:${String(realm)}:${String(name)}`.toLowerCase();

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
    typeof regionRaw === "object" ? (regionRaw.slug || regionRaw.name || "") : regionRaw
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

function withTimeout(promiseFactory, ms, label = "timeout") {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(label)), ms);
    Promise.resolve()
      .then(promiseFactory)
      .then((v) => {
        clearTimeout(t);
        resolve(v);
      })
      .catch((e) => {
        clearTimeout(t);
        reject(e);
      });
  });
}

// --- Playwright singleton ---
let browserPromise = null;
let contextPromise = null;
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

    // ✅ режем лишнее, но stylesheet оставляем
    await context.route("**/*", (route) => {
      const rt = route.request().resourceType();
      if (!["document", "script", "xhr", "fetch", "stylesheet"].includes(rt)) return route.abort();
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

// --- Timed Runs total ---
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

/**
 * ✅ DOM-версия: достаёт Discord строго из строки с иконкой Discord в Contact Info.
 * Возвращает:
 * - "name#1234" (старый формат) или
 * - "name" (новый формат), если именно в discord-строке.
 */
async function extractDiscordFromDom(page) {
  return await page
    .evaluate(() => {
      const norm = (s) =>
        String(s || "")
          .replace(/\u00a0/g, " ")
          .replace(/\s+/g, " ")
          .trim();

      const low = (s) => norm(s).toLowerCase();

      const oldRe = /^[\p{L}\p{N}_.-]{2,32}#\d{4}$/u;
      const newRe = /^[\p{L}\p{N}_.-]{2,32}$/u;

      const bad = new Set([
        "contact info",
        "discord",
        "loadout",
        "twitch",
        "twitter",
        "youtube",
        "kick",
        "tiktok",
        "instagram",
        "facebook",
      ]);

      // находим корень Contact Info
      const all = Array.from(document.querySelectorAll("h1,h2,h3,h4,div,span"));
      const head = all.find((el) => /contact info/i.test(el.textContent || ""));
      const root = (head && (head.closest("section,aside,div") || head.parentElement)) || document.body;

      // ищем элементы, которые с большой вероятностью являются иконкой Discord
      const iconSelectors = [
        'svg[data-icon="discord"]',
        '[data-icon="discord"]',
        'i[class*="discord"]',
        'svg[aria-label*="discord" i]',
        'img[alt*="discord" i]',
        'a[href*="discord" i]',
        '[data-testid*="discord" i]',
        '[title*="discord" i]',
        '[aria-label*="discord" i]',
      ].join(",");

      // если на странице нет явных атрибутов, пробуем найти строку по HTML-сигнатуре
      const icons = Array.from(root.querySelectorAll(iconSelectors));

      const candidateRows = [];

      for (const ic of icons) {
        const row = ic.closest("a,li,div,button,span") || ic.parentElement || ic;
        if (row) candidateRows.push(row);
      }

      // fallback: иногда иконки не размечены — тогда ищем любые "строки" в contact info,
      // где в innerHTML встречается слово discord.
      if (!candidateRows.length) {
        const rows = Array.from(root.querySelectorAll("a,li,div,button"))
          .filter((el) => (el.innerText || "").trim().length > 0)
          .slice(0, 400);

        for (const r of rows) {
          const html = (r.innerHTML || "").toLowerCase();
          if (html.includes("discord")) candidateRows.push(r);
        }
      }

      const uniq = [];
      const seen = new Set();
      for (const r of candidateRows) {
        const key = r && r.outerHTML ? r.outerHTML.slice(0, 200) : String(r);
        if (!seen.has(key)) {
          seen.add(key);
          uniq.push(r);
        }
      }

      for (const row of uniq) {
        let txt = norm(row.innerText || row.textContent || "");
        if (!txt) continue;

        const lines = txt
          .split("\n")
          .map((x) => norm(x))
          .filter(Boolean)
          .filter((x) => !bad.has(low(x)));

        // выкидываем URL и платформенные слова
        const cleaned = lines.filter((x) => !/^https?:\/\//i.test(x) && !bad.has(low(x)));

        // сначала строго старый формат
        const hitOld = cleaned.find((x) => oldRe.test(x));
        if (hitOld) return hitOld;

        // затем новый формат, но только если он действительно строка контакта (а не "label")
        const hitNew = cleaned.find((x) => newRe.test(x) && x.length >= 3 && !bad.has(low(x)));
        if (hitNew) return hitNew;
      }

      return null;
    })
    .catch(() => null);
}

/**
 * ✅ Fallback по тексту (безопасный):
 * берём ТОЛЬКО "name#1234". Новый формат без # не берём, чтобы не ловить Twitch.
 */
function parseDiscordFromText(text) {
  const t = String(text || "").replace(/\u00a0/g, " ");
  const idx = t.toLowerCase().indexOf("contact info");
  if (idx === -1) return null;

  let slice = t.slice(idx, idx + 2600);
  const cut = slice.search(
    /\n\s*(mythic\+|raid progression|gear|talents|external links|recent runs|top runs)\b/i
  );
  if (cut > 0) slice = slice.slice(0, cut);

  const lines = slice
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean);

  const discordOldRe = /^[\p{L}\p{N}_.-]{2,32}#\d{4}$/u;

  const badWords = new Set([
    "contact info",
    "loadout",
    "gear",
    "talents",
    "external links",
    "recent runs",
    "twitch",
    "twitter",
    "youtube",
    "kick",
    "tiktok",
    "instagram",
    "facebook",
  ]);

  const isBad = (s) => badWords.has(String(s || "").toLowerCase());

  for (let i = 0; i < lines.length; i++) {
    const cand = lines[i];
    if (!cand) continue;
    if (/^https?:\/\//i.test(cand)) continue;
    if (isBad(cand)) continue;

    const prev = (lines[i - 1] || "").toLowerCase();
    const next = (lines[i + 1] || "").toLowerCase();
    if (badWords.has(prev) || badWords.has(next)) continue;

    if (discordOldRe.test(cand)) return cand;
  }

  return null;
}

// ----- WAR BAND helpers -----
async function findWarbandUrlFromPage(page) {
  const href = await page
    .evaluate(() => {
      const abs = (raw) => {
        if (!raw) return null;
        if (/^https?:\/\//i.test(raw)) return raw;
        if (raw.startsWith("/")) return new URL(raw, location.origin).toString();
        return null;
      };

      const a1 = Array.from(document.querySelectorAll("a[href]")).find((a) =>
        /warband/i.test(a.getAttribute("href") || "")
      );
      if (a1) return abs(a1.getAttribute("href"));

      const norm = (s) => (s || "").replace(/\s+/g, " ").trim().toLowerCase();
      const els = Array.from(
        document.querySelectorAll("a,button,[role='tab'],[role='button'],[tabindex]")
      );
      const hit = els.find((el) => {
        const t = norm(el.textContent);
        const al = norm(el.getAttribute?.("aria-label"));
        const ti = norm(el.getAttribute?.("title"));
        const href = norm(el.getAttribute?.("href"));
        const dt = norm(el.getAttribute?.("data-testid"));
        return [t, al, ti, href, dt].some((x) => x && x.includes("warband"));
      });
      if (!hit) return null;

      const a = hit.closest?.("a");
      const raw =
        (a && a.getAttribute("href")) ||
        hit.getAttribute?.("href") ||
        hit.getAttribute?.("to") ||
        (hit.dataset && (hit.dataset.href || hit.dataset.to || hit.dataset.url)) ||
        null;

      return abs(raw);
    })
    .catch(() => null);

  if (href) return href;

  const fromScripts = await page
    .evaluate(() => {
      const abs = (raw) => {
        if (!raw) return null;
        if (/^https?:\/\//i.test(raw)) return raw;
        if (raw.startsWith("/")) return new URL(raw, location.origin).toString();
        return null;
      };

      const re = /"((?:\/)[^"\\]*warband[^"\\]*)"/gi;
      const candidates = new Set();

      const scripts = Array.from(document.querySelectorAll("script"));
      for (const s of scripts) {
        const txt = s.textContent || "";
        if (!txt || txt.length > 2_000_000) continue;
        if (!/warband/i.test(txt)) continue;

        let m;
        while ((m = re.exec(txt)) !== null) candidates.add(m[1]);

        if (s.id === "__NEXT_DATA__" || s.type === "application/json") {
          try {
            const json = JSON.parse(txt);
            const stack = [json];
            while (stack.length) {
              const cur = stack.pop();
              if (!cur) continue;
              if (typeof cur === "string") {
                if (cur.includes("warband") && cur.startsWith("/")) candidates.add(cur);
                continue;
              }
              if (Array.isArray(cur)) {
                for (const x of cur) stack.push(x);
                continue;
              }
              if (typeof cur === "object") {
                for (const k of Object.keys(cur)) stack.push(cur[k]);
              }
            }
          } catch {}
        }
      }

      const canon = document.querySelector('link[rel="canonical"]')?.href || null;
      const og = document.querySelector('meta[property="og:url"]')?.content || null;
      if (canon && /warband/i.test(canon)) return canon;
      if (og && /warband/i.test(og)) return og;

      for (const c of candidates) {
        const u = abs(c);
        if (u) return u;
      }
      return null;
    })
    .catch(() => null);

  return fromScripts || null;
}

async function detectHasWarband(page) {
  return await page
    .evaluate(() => {
      const norm = (s) => (s || "").replace(/\s+/g, " ").trim().toLowerCase();
      const els = Array.from(
        document.querySelectorAll(
          "a,button,[role='tab'],[role='button'],[tabindex],[aria-label],[title]"
        )
      );

      return els.some((el) => {
        const t = norm(el.textContent);
        const al = norm(el.getAttribute?.("aria-label"));
        const ti = norm(el.getAttribute?.("title"));
        const href = norm(el.getAttribute?.("href"));
        const to = norm(el.getAttribute?.("to"));
        const dt = norm(el.getAttribute?.("data-testid"));
        const du = norm(el.getAttribute?.("data-url"));
        const dh = norm(el.getAttribute?.("data-href"));
        return [t, al, ti, href, to, dt, du, dh].some((x) => x && x.includes("warband"));
      });
    })
    .catch(() => false);
}

async function waitForWarbandEvidence(page, totalMs = 12_000, stepMs = 400) {
  const end = Date.now() + totalMs;
  while (Date.now() < end) {
    const ok = await detectHasWarband(page);
    if (ok) return true;
    await page.waitForTimeout(stepMs);
  }
  return false;
}

async function fetchWarbandUrl(context, region, realm, name) {
  const ck = wbKey(region, realm, name);
  const cached = getC(ck);
  if (cached !== null) return cached; // string|null

  const profileUrl = `https://raider.io/characters/${region}/${encodeURIComponent(realm)}/${encodeURIComponent(
    name
  )}`;

  const page = await context.newPage();
  page.setDefaultTimeout(15000);
  page.setDefaultNavigationTimeout(15000);

  try {
    await page.goto(profileUrl, { waitUntil: "domcontentloaded", timeout: 15000 });
    await page.waitForTimeout(800);

    const hasWar = await waitForWarbandEvidence(page, 12_000, 400);
    if (!hasWar) {
      setC(ck, null);
      return null;
    }

    let url = await findWarbandUrlFromPage(page);
    if (url) {
      setC(ck, url);
      return url;
    }

    await page
      .locator(
        'a:has-text("Warband"), button:has-text("Warband"), [role="tab"]:has-text("Warband"), [aria-label*="warband" i], [title*="warband" i]'
      )
      .first()
      .click({ timeout: 6000 })
      .catch(() => {});
    await page.waitForTimeout(900);

    url = await findWarbandUrlFromPage(page);
    if (url) {
      setC(ck, url);
      return url;
    }

    const fallback = profileUrl + "?tab=warband";
    setC(ck, fallback);
    return fallback;
  } catch {
    setC(ck, null);
    return null;
  } finally {
    await page.close();
  }
}

/**
 * ✅ Возвращает { total, by, regionUsed, discord } или null
 */
async function fetchTimedTotal(context, realm, name, regionHint) {
  const ck = ttKey(regionHint, realm, name);
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

      try {
        await page.waitForSelector('text=/Keystone\\s+Timed\\s+Runs/i', { timeout: 6000 });
      } catch (_) {}
      try {
        await page.waitForSelector('text=/Contact\\s+Info/i', { timeout: 5000 });
      } catch (_) {}

      // ✅ Discord сначала из DOM (по иконке Discord)
      const domDiscord = await extractDiscordFromDom(page);

      const bodyText = await page.evaluate(() => document.body?.innerText || "");
      const parsed = parseTimedRunsAll(bodyText);

      if (parsed) {
        // ✅ fallback по тексту только если DOM не нашёл
        const discord = domDiscord || parseDiscordFromText(bodyText);

        const out = { ...parsed, regionUsed: region, discord: discord || null };
        setC(ck, out);
        return out;
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

// --- lowest for run ---
async function lowestFromRun(context, runUrl) {
  const rk = runKey(runUrl);
  const cached = getC(rk);
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

  if (!best) {
    const outFail = { ok: false, error: "no timed totals found", partial: true };
    setC(rk, outFail, 10 * 60 * 1000);
    return outFail;
  }

  const label = `${best.p.name}-${best.p.realm}`.toLowerCase();
  const regionForUrl = best.stat.regionUsed || best.p.region || "eu";
  const profile_url = `https://raider.io/characters/${regionForUrl}/${encodeURIComponent(
    best.p.realm
  )}/${encodeURIComponent(best.p.name)}`;

  let warband_url = null;
  try {
    warband_url = await withTimeout(
      () => fetchWarbandUrl(context, regionForUrl, best.p.realm, best.p.name),
      WAR_BAND_TIMEOUT_MS,
      "warband_timeout"
    );
  } catch {
    warband_url = null;
  }

  const out = {
    ok: true,
    lowest: label,
    profile_url,
    discord: best.stat.discord || null,
    warband_url: warband_url || null,
    timed_total: best.stat.total,
    timed_breakdown: best.stat.by,
    partial,
  };

  setC(rk, out, partial ? 20 * 60 * 1000 : TTL_MS);
  return out;
}

/* =======================
   Queue + worker
   ======================= */
const runQueue = [];
const queued = new Set();
const inflight = new Map();
let workerRunning = false;
let lastProgressAt = now();

function enqueueRuns(urls, force = false) {
  const t = now();
  for (const u0 of urls) {
    const u = String(u0 || "").trim();
    if (!u) continue;

    const cached = getC(runKey(u));
    if (cached && cached.ok && !cached.partial && !force) continue;

    const startedAt = inflight.get(u);
    if (startedAt && t - startedAt > INFLIGHT_STALE_MS) inflight.delete(u);

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
          setC(runKey(u), out, out.partial ? 20 * 60 * 1000 : TTL_MS);
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

          if (isBrowserClosedError(msg)) {
            await ensureResetBrowser();
            context = await getContext();
            setTimeout(() => enqueueRuns([u], true), 2000);
          } else {
            setC(runKey(u), { ok: false, error: msg, partial: true }, 10 * 60 * 1000);
          }
          lastProgressAt = now();
        } finally {
          inflight.delete(u);
        }
      });
    }
  })()
    .catch(async () => {
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

setInterval(() => {
  const t = now();
  if (workerRunning && t - lastProgressAt > INFLIGHT_STALE_MS) {
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
   Batch endpoints
   ======================= */
function processBatchFast(urls) {
  const clean = urls.map((u) => (u ? String(u).trim() : "")).slice(0, MAX_URLS);

  const results = new Array(clean.length);
  const missing = [];
  const needRefine = [];

  for (let i = 0; i < clean.length; i++) {
    const u = clean[i];
    if (!u) {
      results[i] = { ok: false, error: "empty url", partial: true };
      continue;
    }

    const cached = getC(runKey(u));

    if (cached) {
      if (!cached.ok && isBrowserClosedError(cached.error)) {
        cache.delete(runKey(u));
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
  const clean = urls.map((u) => (u ? String(u).trim() : "")).slice(0, MAX_URLS);
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

// ✅ компактный ответ для Sheets (ускоряет JSON.parse/запись в Apps Script)
function compactForSheet(r) {
  if (!r) return { ok: false, error: "No data", partial: true };
  if (!r.ok) return { ok: false, error: r.error || "ERR", partial: !!r.partial };
  return {
    ok: true,
    profile_url: r.profile_url || "",
    lowest: r.lowest || "",
    partial: !!r.partial,
    discord: r.discord || "",
    warband_url: r.warband_url || "",
  };
}

/* =======================
   Routes
   ======================= */
app.get("/health", (_, res) => res.json({ ok: true, ver: CACHE_VER, max: MAX_URLS }));

app.get("/debug", (_, res) => {
  res.json({
    ok: true,
    ver: CACHE_VER,
    max: MAX_URLS,
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

// ✅ основной endpoint для Sheets
app.post("/lowest-from-runs", async (req, res) => {
  try {
    const full = String(req.query.full || "") === "1";
    const compact = String(req.query.compact || "") !== "0"; // default ON
    const urls = Array.isArray(req.body) ? req.body : req.body?.urls;

    if (!Array.isArray(urls)) {
      return res.status(400).json({ ok: false, error: "body must be array or { urls: array }" });
    }

    const resultsRaw = full ? await processBatchFull(urls) : processBatchFast(urls);
    const results = (!full && compact) ? resultsRaw.map(compactForSheet) : resultsRaw;

    return res.json({ ok: true, results });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

app.listen(PORT, () => console.log("listening on", PORT));
