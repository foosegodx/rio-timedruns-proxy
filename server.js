import express from "express";
import fetch from "node-fetch";
import { chromium } from "playwright";

const app = express();
const PORT = process.env.PORT || 3000;

// --- простейший кэш, чтобы не долбить Raider.IO ---
const cache = new Map(); // key -> { value, exp }
const TTL_MS = 6 * 60 * 60 * 1000; // 6 часов

function getCache(key) {
  const v = cache.get(key);
  if (!v) return null;
  if (Date.now() > v.exp) {
    cache.delete(key);
    return null;
  }
  return v.value;
}

function setCache(key, value) {
  cache.set(key, { value, exp: Date.now() + TTL_MS });
}

// ----------- Raider.IO run-details -----------
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

  // realm в run-details иногда объект
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
      return { name: String(name), realm }; // realm = slug
    })
    .filter(Boolean);

  if (!roster.length) throw new Error("No usable roster");
  return { season, roster };
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

// ----------- Парсинг timed runs со страницы персонажа -----------
function parseTimedRunsFromText(text) {
  // Ищем числа перед строками:
  // "5+ Keystone Timed Runs" / "10+ ..." / "15+ ..."
  // Важно: берём именно timed runs, как на сайте.
  const t = text.replace(/\u00a0/g, " "); // nbsp
  const get = (lvl) => {
    const re = new RegExp(String.raw`(\d[\d,]*)\s+${lvl}\+\s+Keystone\s+Timed\s+Runs`, "i");
    const m = t.match(re);
    if (!m) return null;
    return Number(String(m[1]).replace(/,/g, ""));
  };

  return {
    timed5: get(5),
    timed10: get(10),
    timed15: get(15),
  };
}

async function fetchTimedRunsViaBrowser(region, realm, name, browser) {
  const cacheKey = `timed:${region}:${realm}:${name}`;
  const cached = getCache(cacheKey);
  if (cached) return cached;

  const page = await browser.newPage({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  });

  // чтобы меньше палиться — можно отключить картинки/шрифты
  await page.route("**/*", (route) => {
    const rt = route.request().resourceType();
    if (["image", "font", "media"].includes(rt)) return route.abort();
    return route.continue();
  });

  const url = `https://raider.io/characters/${region}/${encodeURIComponent(realm)}/${encodeURIComponent(name)}`;

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
    // ждём чуть-чуть, чтобы контент дорендерился
    await page.waitForTimeout(1200);

    const text = await page.evaluate(() => document.body?.innerText || "");
    const parsed = parseTimedRunsFromText(text);

    // если ничего не нашли — вернём nullы
    setCache(cacheKey, parsed);
    return parsed;
  } finally {
    await page.close();
  }
}

async function fetchTimed5AnyRegion(realm, name, browser) {
  const regions = ["eu", "us", "kr", "tw", "cn"];
  for (const region of regions) {
    const data = await fetchTimedRunsViaBrowser(region, realm, name, browser);
    if (data && typeof data.timed5 === "number") return { region, ...data };
  }
  return null;
}

// ----------- API endpoints -----------
app.get("/health", (_, res) => res.json({ ok: true }));

// 1 запрос на одну ссылку из таблицы:
app.get("/lowest-from-run", async (req, res) => {
  const runUrl = req.query.url;
  if (!runUrl) return res.status(400).json({ error: "missing url" });

  try {
    // кэш результата по ссылке
    const cacheKey = `runlowest:${runUrl}`;
    const cached = getCache(cacheKey);
    if (cached) return res.json(cached);

    const { roster } = await fetchRunRoster(runUrl);

    const browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-dev-shm-usage"],
    });

    try {
      let best = null;

      for (const p of roster) {
        const timed = await fetchTimed5AnyRegion(p.realm, p.name, browser);
        if (!timed || typeof timed.timed5 !== "number") continue;

        if (!best || timed.timed5 < best.timed5) {
          best = { name: p.name, realm: p.realm, timed5: timed.timed5 };
        }
      }

      const out = best
        ? { ok: true, lowest: `${best.name}-${best.realm}`.toLowerCase(), timed5: best.timed5 }
        : { ok: false, error: "no timed5 found" };

      setCache(cacheKey, out);
      return res.json(out);
    } finally {
      await browser.close();
    }
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

app.listen(PORT, () => {
  console.log(`rio proxy listening on :${PORT}`);
});
