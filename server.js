import express from "express";
import fetch from "node-fetch";
import { chromium } from "playwright";

const app = express();
app.use(express.json({ limit: "1mb" })); // <-- нужно для batch (POST JSON)
const PORT = process.env.PORT || 3000;

const cache = new Map();
const TTL_MS = 6 * 60 * 60 * 1000;
const regions = ["eu", "us", "kr", "tw", "cn"];

function getC(k) {
  const v = cache.get(k);
  if (!v) return null;
  if (Date.now() > v.exp) {
    cache.delete(k);
    return null;
  }
  return v.val;
}
function setC(k, val) {
  cache.set(k, { val, exp: Date.now() + TTL_MS });
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

async function fetchRunRoster(runUrl) {
  const m = String(runUrl).match(/\/mythic-plus-runs\/(season-[^/]+)\/(\d+)-/i);
  if (!m) throw new Error("Bad URL");
  const season = m[1];
  const runId = m[2];

  const apiUrl =
    "https://raider.io/api/v1/mythic-plus/run-details" +
    `?season=${encodeURIComponent(season)}` +
    `&id=${encodeURIComponent(runId)}`;

  const resp = await fetch(apiUrl);
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

      const realm = normalizeRealm(realmRaw);
      if (!name || !realm) return null;
      return { name: String(name), realm };
    })
    .filter(Boolean);

  if (!roster.length) throw new Error("No usable roster");
  return roster;
}

function parseTimed5(text) {
  const t = String(text || "").replace(/\u00a0/g, " ");

  let m = t.match(/(\d[\d,]*)\s+5\+\s+Keystone\s+Timed\s+Runs/i);
  if (m) return Number(String(m[1]).replace(/,/g, ""));

  m = t.match(/(\d[\d,]*)\s+5\+\s+Timed\s+Runs/i);
  if (m) return Number(String(m[1]).replace(/,/g, ""));

  const idx = t.toLowerCase().indexOf("timed runs");
  if (idx !== -1) {
    const slice = t.slice(Math.max(0, idx - 300), idx + 300);
    m = slice.match(/(\d[\d,]*)\s+5\+/i);
    if (m) return Number(String(m[1]).replace(/,/g, ""));
  }

  m = t.match(/(\d[\d,]*)\s+5\+\s+.*?\s+Runs/i);
  if (m) return Number(String(m[1]).replace(/,/g, ""));

  return null;
}

async function fetchTimed5(browser, realm, name) {
  const ck = `t5:${realm}:${name}`;
  const cached = getC(ck);
  if (cached !== null) return cached;

  for (const region of regions) {
    const page = await browser.newPage({
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    });

    await page.route("**/*", (route) => {
      const rt = route.request().resourceType();
      if (["image", "font", "media"].includes(rt)) return route.abort();
      return route.continue();
    });

    const url = `https://raider.io/characters/${region}/${encodeURIComponent(realm)}/${encodeURIComponent(name)}`;

    try {
      await page.goto(url, { waitUntil: "networkidle", timeout: 45000 });

      try {
        await page.waitForSelector('text=/Timed Runs/i', { timeout: 8000 });
      } catch (_) {}

      await page.waitForTimeout(500);

      const text = await page.evaluate(() => document.body?.innerText || "");
      const v = parseTimed5(text);
      if (typeof v === "number" && Number.isFinite(v)) {
        setC(ck, v);
        return v;
      }
    } catch (_) {
    } finally {
      await page.close();
    }
  }

  setC(ck, null);
  return null;
}

async function lowestFromRunUrl(browser, runUrl) {
  const runCacheKey = `run:${runUrl}`;
  const cached = getC(runCacheKey);
  if (cached) return cached;

  const roster = await fetchRunRoster(runUrl);

  let best = null;
  for (const p of roster) {
    const t5 = await fetchTimed5(browser, p.realm, p.name);
    if (t5 === null) continue;
    if (!best || t5 < best.t5) best = { name: p.name, realm: p.realm, t5: t5 };
  }

  const out = best
    ? { ok: true, lowest: `${best.name}-${best.realm}`.toLowerCase(), timed5: best.t5 }
    : { ok: false, error: "no timed5 found" };

  setC(runCacheKey, out);
  return out;
}

app.get("/health", (_, res) => res.json({ ok: true }));

// Старый (одиночный) endpoint оставляем
app.get("/lowest-from-run", async (req, res) => {
  const runUrl = req.query.url;
  if (!runUrl) return res.status(400).json({ ok: false, error: "missing url" });

  try {
    const browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-dev-shm-usage"],
    });

    try {
      const out = await lowestFromRunUrl(browser, String(runUrl).trim());
      res.json(out);
    } finally {
      await browser.close();
    }
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

/**
 * ✅ Новый batch endpoint:
 * POST /lowest-from-runs
 * Body: { "urls": ["https://...", "..."] }
 * Returns: { ok:true, results:[ {ok:true,lowest:...} or {ok:false,error:...}, ... ] }
 */
app.post("/lowest-from-runs", async (req, res) => {
  try {
    const urls = req.body && req.body.urls;
    if (!urls || !Array.isArray(urls)) {
      return res.status(400).json({ ok: false, error: "Body must be { urls: [...] }" });
    }

    // фильтруем пустые и ограничим размер запроса
    const clean = urls.map((u) => (u ? String(u).trim() : "")).slice(0, 300);

    const browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-dev-shm-usage"],
    });

    try {
      const results = [];
      for (let i = 0; i < clean.length; i++) {
        const u = clean[i];
        if (!u) {
          results.push({ ok: false, error: "empty url" });
          continue;
        }
        try {
          const out = await lowestFromRunUrl(browser, u);
          results.push(out);
        } catch (e) {
          results.push({ ok: false, error: String(e.message || e) });
        }
      }

      return res.json({ ok: true, results: results });
    } finally {
      await browser.close();
    }
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

app.listen(PORT, () => console.log("listening on", PORT));
