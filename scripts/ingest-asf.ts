#!/usr/bin/env tsx
/**
 * ASF (Autoritatea de Supraveghere Financiara) ingestion crawler.
 *
 * Crawls asfromania.ro for:
 *   Phase 1 — Regulation listing pages (norme, regulamente, instructiuni)
 *   Phase 2 — Individual regulation article pages (extract title, text, metadata)
 *   Phase 3 — Enforcement / sanctions pages (monthly council decisions)
 *
 * Writes directly to the SQLite database used by the MCP server.
 *
 * Usage:
 *   npx tsx scripts/ingest-asf.ts
 *   npx tsx scripts/ingest-asf.ts --dry-run
 *   npx tsx scripts/ingest-asf.ts --resume
 *   npx tsx scripts/ingest-asf.ts --force
 *   npx tsx scripts/ingest-asf.ts --phase regulations
 *   npx tsx scripts/ingest-asf.ts --phase enforcement
 *   npx tsx scripts/ingest-asf.ts --limit 20
 */

import Database from "better-sqlite3";
import * as cheerio from "cheerio";
import { existsSync, mkdirSync, readFileSync, writeFileSync, unlinkSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { SCHEMA_SQL } from "../src/db.js";

// ─── Constants ────────────────────────────────────────────────────────────────

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const BASE_URL = "https://asfromania.ro";
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 3000;

const DB_PATH = process.env["ASF_DB_PATH"] ?? "data/asf.db";
const STATE_PATH = resolve(__dirname, "../data/ingest-state.json");

/**
 * Regulation category pages on asfromania.ro.
 *
 * Each entry points to a listing page under /ro/c/{id}/{slug} that
 * contains links to individual regulation articles (/ro/a/{id}/{slug}).
 * The sourcebook_id maps to the sourcebooks table in the DB.
 */
const REGULATION_CATEGORIES: CategoryDef[] = [
  // Integrated regulations (common to all three markets)
  {
    sourcebook_id: "ASF_REGLEMENTARI_INTEGRATE",
    name: "Reglementari integrate emise de ASF",
    path: "/ro/c/361/reglementari-integrate-emise-de-asf",
    description:
      "Acte normative comune aplicabile celor trei piete reglementate si supravegheate de ASF (piata de capital, asigurari, pensii private).",
  },
  // Capital market — SSIF legislation
  {
    sourcebook_id: "ASF_CAPITAL_SSIF",
    name: "Legislatie SSIF - piata de capital",
    path: "/ro/a/932/legislatie-ssif",
    description:
      "Norme si regulamente privind societatile de servicii de investitii financiare, inclusiv cerinte MiFID II/MiFIR.",
  },
  // Capital market — emitenti (issuers)
  {
    sourcebook_id: "ASF_CAPITAL_EMITENTI",
    name: "Legislatie emitenti - piata de capital",
    path: "/ro/a/944/legislatie-emitenti",
    description:
      "Reglementari privind emitentii de valori mobiliare, oferte publice si obligatii de raportare.",
  },
  // Capital market — general regulations
  {
    sourcebook_id: "ASF_CAPITAL_GENERAL",
    name: "Reglementari generale SIIF - piata de capital",
    path: "/ro/a/946/reglementari-generale-siif",
    description:
      "Reglementari generale privind piata de capital, organisme de plasament colectiv si administratori de fonduri.",
  },
  // Insurance — secondary legislation
  {
    sourcebook_id: "ASF_ASIGURARI",
    name: "Legislatie secundara asigurari",
    path: "/ro/a/2890/legislatie-secundara-asigurari",
    description:
      "Norme ASF privind piata de asigurari-reasigurari: autorizare, Solvabilitate II, distributie, raportare si guvernanta.",
  },
  // Insurance — IDD (Insurance Distribution Directive)
  {
    sourcebook_id: "ASF_ASIGURARI_IDD",
    name: "Legislatie IDD - distributie in asigurari",
    path: "/ro/c/100/idd",
    description:
      "Reglementari privind distributia in asigurari conform Directivei (UE) 2016/97 (IDD).",
  },
  // Insurance — Solvency II
  {
    sourcebook_id: "ASF_SOLVENCY_II",
    name: "Reglementari nationale Solvabilitate II",
    path: "/ro/c/101/solvabilitate-ii",
    description:
      "Reglementari nationale de implementare a Directivei Solvabilitate II (2009/138/CE) pentru piata de asigurari.",
  },
  // Pensions — Pillar II
  {
    sourcebook_id: "ASF_PENSII_PILON_II",
    name: "Legislatie pensii private Pilon II",
    path: "/ro/c/114/pilon-ii",
    description:
      "Reglementari privind fondurile de pensii administrate privat (Pilon II) — autorizare, investitii, raportare.",
  },
  // Pensions — Pillar III
  {
    sourcebook_id: "ASF_PENSII_PILON_III",
    name: "Legislatie pensii private Pilon III",
    path: "/ro/c/115/pilon-iii",
    description:
      "Reglementari privind fondurile de pensii facultative (Pilon III) — autorizare, contributii, raportare.",
  },
  // DORA
  {
    sourcebook_id: "ASF_DORA",
    name: "DORA - Regulamentul (UE) 2022/2554",
    path: "/ro/c/388/dora---regulamentul-(ue)-2022/2554",
    description:
      "Reglementari privind rezilienta operationala digitala a sectorului financiar (DORA).",
  },
];

/**
 * Sanctions category pages, each containing yearly sub-pages with
 * links to individual enforcement decision PDFs or monthly summaries.
 */
const SANCTIONS_CATEGORIES: SanctionCategoryDef[] = [
  {
    market: "piata_de_capital",
    name: "Sanctiuni piata de capital",
    path: "/ro/c/139/piata-de-capital",
  },
  {
    market: "piata_asigurari",
    name: "Sanctiuni piata asigurari-reasigurari",
    path: "/ro/c/138/piata-asigurari---reasigurari",
  },
  {
    market: "pensii_private",
    name: "Sanctiuni pensii private",
    path: "/ro/c/140/pensii-private",
  },
];

/**
 * Monthly sanctions summary listing page — these pages contain
 * inline text descriptions of enforcement decisions adopted by
 * the ASF Council each month. The path is a category page.
 */
const MONTHLY_SANCTIONS_CATEGORY = "/ro/c/48/sanctiuni";

// ─── Types ────────────────────────────────────────────────────────────────────

interface CategoryDef {
  sourcebook_id: string;
  name: string;
  path: string;
  description: string;
}

interface SanctionCategoryDef {
  market: string;
  name: string;
  path: string;
}

interface DiscoveredArticle {
  url: string;
  title: string;
  sourcebook_id: string;
}

interface ParsedProvision {
  reference: string;
  title: string;
  text: string;
  type: string;
  effective_date: string | null;
  chapter: string | null;
  section: string | null;
}

interface ParsedEnforcement {
  firm_name: string;
  reference_number: string | null;
  action_type: string;
  amount: number | null;
  date: string | null;
  summary: string;
  sourcebook_references: string | null;
  market: string;
}

interface IngestState {
  crawled_urls: string[];
  last_phase: string;
  last_run: string;
}

interface CliOptions {
  dryRun: boolean;
  resume: boolean;
  force: boolean;
  phase: "all" | "regulations" | "enforcement";
  limit: number;
}

// ─── CLI argument parsing ─────────────────────────────────────────────────────

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  const options: CliOptions = {
    dryRun: false,
    resume: false,
    force: false,
    phase: "all",
    limit: 0,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--dry-run") {
      options.dryRun = true;
    } else if (arg === "--resume") {
      options.resume = true;
    } else if (arg === "--force") {
      options.force = true;
    } else if (arg === "--phase" && args[i + 1]) {
      options.phase = args[i + 1] as CliOptions["phase"];
      i++;
    } else if (arg === "--limit" && args[i + 1]) {
      const parsed = Number.parseInt(args[i + 1]!, 10);
      if (!Number.isNaN(parsed) && parsed > 0) {
        options.limit = parsed;
      }
      i++;
    }
  }

  return options;
}

// ─── State persistence (for --resume) ─────────────────────────────────────────

function loadState(): IngestState {
  if (existsSync(STATE_PATH)) {
    try {
      return JSON.parse(readFileSync(STATE_PATH, "utf-8")) as IngestState;
    } catch {
      // Corrupted state file — start fresh.
    }
  }
  return { crawled_urls: [], last_phase: "", last_run: "" };
}

function saveState(state: IngestState): void {
  const dir = dirname(STATE_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  writeFileSync(STATE_PATH, JSON.stringify(state, null, 2));
}

// ─── HTTP fetch with retry and rate limit ─────────────────────────────────────

let lastFetchTime = 0;

async function rateLimitedFetch(url: string): Promise<string> {
  const now = Date.now();
  const elapsed = now - lastFetchTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastFetchTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": "AnsvarMCP/1.0 (legal-data-research; contact@ansvar.eu)",
          "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.5",
        },
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status} ${response.statusText} for ${url}`);
      }

      return await response.text();
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      if (attempt < MAX_RETRIES) {
        const backoff = RETRY_BACKOFF_MS * attempt;
        console.warn(`  Retry ${attempt}/${MAX_RETRIES} for ${url} (waiting ${backoff}ms): ${lastError.message}`);
        await sleep(backoff);
      }
    }
  }

  throw lastError ?? new Error(`Failed to fetch ${url} after ${MAX_RETRIES} attempts`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── HTML parsing helpers ─────────────────────────────────────────────────────

/**
 * Extract article links from a category listing page.
 *
 * ASF listing pages use <ul><li><a href="/ro/a/{id}/{slug}">Title</a></li></ul>.
 * Some pages use tables or direct PDF links — those are also captured.
 */
function extractArticleLinks(html: string): Array<{ url: string; title: string }> {
  const $ = cheerio.load(html);
  const links: Array<{ url: string; title: string }> = [];
  const seen = new Set<string>();

  // Find all links pointing to article pages (/ro/a/...)
  $('a[href*="/ro/a/"]').each((_i, el) => {
    const href = $(el).attr("href");
    if (!href) return;

    const fullUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
    // Normalise to drop query params and fragments for dedup
    const normalised = fullUrl.split("?")[0]!.split("#")[0]!;
    if (seen.has(normalised)) return;
    seen.add(normalised);

    const title = $(el).text().trim();
    if (title.length > 0) {
      links.push({ url: normalised, title });
    }
  });

  // Also pick up direct PDF attachment links as references
  $('a[href$=".pdf"]').each((_i, el) => {
    const href = $(el).attr("href");
    if (!href) return;
    const fullUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
    if (seen.has(fullUrl)) return;
    seen.add(fullUrl);

    const title = $(el).text().trim();
    if (title.length > 0) {
      links.push({ url: fullUrl, title });
    }
  });

  return links;
}

/**
 * Detect pagination on a category listing page and return all page URLs.
 *
 * Pagination uses ?page=N query parameters.
 */
function extractPaginationUrls(html: string, basePageUrl: string): string[] {
  const $ = cheerio.load(html);
  const pages = new Set<string>();

  $("a[href]").each((_i, el) => {
    const href = $(el).attr("href") ?? "";
    const match = href.match(/[?&]page=(\d+)/);
    if (match) {
      const pageNum = match[1];
      // Reconstruct full URL
      const baseNoPage = basePageUrl.split("?")[0]!;
      pages.add(`${BASE_URL}${baseNoPage}?page=${pageNum}`);
    }
  });

  return Array.from(pages).sort();
}

/**
 * Parse an individual regulation article page.
 *
 * ASF article pages typically have:
 *  - The regulation title in the <h1> or main heading
 *  - The body text as paragraphs (inline content) or a PDF download link
 *  - Metadata extractable from the title (type, number, year, subject)
 */
function parseArticlePage(html: string, url: string): ParsedProvision | null {
  const $ = cheerio.load(html);

  // Extract title — look for the main heading
  let title = "";
  const headingSelectors = [
    "h1.article-title",
    "h1.page-title",
    "h1",
    ".article-header h1",
    ".page-header h1",
    ".content h1",
  ];
  for (const selector of headingSelectors) {
    const heading = $(selector).first().text().trim();
    if (heading.length > 10) {
      title = heading;
      break;
    }
  }

  if (!title) {
    // Fallback: try <title> tag
    title = $("title").text().replace(/\s*-\s*Autoritatea.*$/i, "").trim();
  }

  if (!title || title.length < 5) return null;

  // Extract the body content
  // ASF article pages put the regulation text inside the main content area
  const contentSelectors = [
    ".article-content",
    ".article-body",
    ".page-content",
    ".content-area",
    "article",
    ".field-item",
    "#content",
    "main",
  ];

  let bodyText = "";
  for (const selector of contentSelectors) {
    const el = $(selector).first();
    if (el.length > 0) {
      // Remove navigation, sidebars, scripts
      el.find("nav, .sidebar, script, style, .breadcrumb, .menu, .pagination").remove();
      bodyText = el.text().trim();
      if (bodyText.length > 50) break;
    }
  }

  if (!bodyText || bodyText.length < 20) {
    // Many ASF pages only have a PDF link, no inline text.
    // Extract PDF links as the provision text.
    const pdfLinks: string[] = [];
    $('a[href$=".pdf"]').each((_i, el) => {
      const href = $(el).attr("href") ?? "";
      const linkText = $(el).text().trim();
      const pdfUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
      pdfLinks.push(`${linkText}: ${pdfUrl}`);
    });

    if (pdfLinks.length > 0) {
      bodyText = `Document disponibil in format PDF:\n${pdfLinks.join("\n")}`;
    } else {
      return null;
    }
  }

  // Extract metadata from title
  const meta = extractRegulationMeta(title);

  return {
    reference: meta.reference || generateReference(title, url),
    title: cleanText(title),
    text: cleanText(bodyText),
    type: meta.type,
    effective_date: meta.effectiveDate,
    chapter: null,
    section: null,
  };
}

/**
 * Extract regulation type, number and year from the title string.
 *
 * Patterns:
 *   "Norma nr. 4/2018 privind ..."
 *   "Regulamentul nr. 13/2019 privind ..."
 *   "INSTRUCTIUNE Nr. 3/2022 ..."
 *   "REGULAMENT NR.18/2022 ..."
 *   "Regulamentul Delegat (UE) 2023/2486 ..."
 */
function extractRegulationMeta(title: string): {
  type: string;
  reference: string;
  effectiveDate: string | null;
} {
  let type = "reglementare";
  if (/\bnorm[aă]\b/i.test(title)) {
    type = "norma";
  } else if (/\bregulament/i.test(title)) {
    type = "regulament";
  } else if (/\binstruc[tț]iun/i.test(title)) {
    type = "instructiune";
  } else if (/\bdecizie?\b/i.test(title)) {
    type = "decizie";
  } else if (/\bordin\b/i.test(title)) {
    type = "ordin";
  }

  // Extract number/year: "nr. 4/2018", "nr.18/2022", "NR. 3/2022", "13/2019"
  let reference = "";
  const numMatch = title.match(/nr\.?\s*(\d+)\s*\/\s*(\d{4})/i);
  if (numMatch) {
    reference = `${type}_${numMatch[1]}/${numMatch[2]}`;
  }

  // Try to extract effective date from title — often not present
  let effectiveDate: string | null = null;
  const dateMatch = title.match(/(\d{2})\.(\d{2})\.(\d{4})/);
  if (dateMatch) {
    effectiveDate = `${dateMatch[3]}-${dateMatch[2]}-${dateMatch[1]}`;
  } else if (numMatch) {
    // Use the year from the regulation number as approximate date
    effectiveDate = `${numMatch[2]}-01-01`;
  }

  return { type, reference, effectiveDate };
}

/**
 * Generate a stable reference string from the title and URL when
 * no number/year pattern is found in the title.
 */
function generateReference(title: string, url: string): string {
  // Use the article ID from the URL as fallback
  const idMatch = url.match(/\/ro\/a\/(\d+)\//);
  const articleId = idMatch ? idMatch[1] : "unknown";

  const slug = title
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, "")
    .replace(/\s+/g, "-")
    .substring(0, 60);

  return `asf_art_${articleId}_${slug}`;
}

/**
 * Clean text by collapsing whitespace and trimming.
 */
function cleanText(text: string): string {
  return text
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

// ─── Enforcement / sanctions parsing ──────────────────────────────────────────

/**
 * Parse a yearly sanctions index page (e.g. "2024 - Sanctiuni/masuri piata de capital").
 *
 * These pages contain links to individual decision PDFs, each with a
 * decision number and date. The actual sanction details (firm name, amount)
 * are inside the PDFs, but we extract what metadata we can from the link text.
 */
function parseSanctionsIndexPage(
  html: string,
  market: string,
  year: string,
): ParsedEnforcement[] {
  const $ = cheerio.load(html);
  const results: ParsedEnforcement[] = [];

  // ASF sanctions pages list decisions as links with text like:
  // "DECIZIA NR. 1309/18.12.2024" or "DECIZIE nr. 1265/24.12.2025"
  const contentSelectors = [
    ".article-content",
    ".article-body",
    ".page-content",
    "article",
    "#content",
    "main",
    "body",
  ];

  let fullText = "";
  for (const selector of contentSelectors) {
    const el = $(selector).first();
    if (el.length > 0 && el.text().trim().length > 50) {
      fullText = el.text();
      break;
    }
  }

  if (!fullText) return results;

  // Match decision patterns in the text
  // Pattern: "DECIZIA NR. {number}/{dd.mm.yyyy}" or "DECIZIE nr. {number}/{dd.mm.yyyy}"
  const decisionRegex = /DECIZI[AE]\s+(?:NR\.?|nr\.?)\s*(\d+)\s*\/\s*(\d{2}\.\d{2}\.\d{4})/gi;
  let match: RegExpExecArray | null;

  while ((match = decisionRegex.exec(fullText)) !== null) {
    const decisionNum = match[1]!;
    const decisionDateRaw = match[2]!;
    const parts = decisionDateRaw.split(".");
    const isoDate = parts.length === 3 ? `${parts[2]}-${parts[1]}-${parts[0]}` : null;

    results.push({
      firm_name: `Decizia nr. ${decisionNum}/${decisionDateRaw}`,
      reference_number: `ASF-${market}-${decisionNum}/${year}`,
      action_type: "decizie",
      amount: null,
      date: isoDate,
      summary: `Decizie de sanctionare adoptata de Consiliul ASF — ${market.replace(/_/g, " ")}. Document complet disponibil pe asfromania.ro.`,
      sourcebook_references: null,
      market,
    });
  }

  return results;
}

/**
 * Parse a monthly sanctions summary page (e.g. "Decizii de sanctionare ...
 * in luna martie 2024"). These pages have inline text with firm names,
 * violation descriptions, and fine amounts.
 */
function parseMonthlySanctionsSummary(
  html: string,
  market: string,
): ParsedEnforcement[] {
  const $ = cheerio.load(html);
  const results: ParsedEnforcement[] = [];

  const contentSelectors = [
    ".article-content",
    ".article-body",
    ".page-content",
    "article",
    "#content",
    "main",
    "body",
  ];

  let bodyText = "";
  for (const selector of contentSelectors) {
    const el = $(selector).first();
    if (el.length > 0) {
      el.find("nav, .sidebar, script, style, .breadcrumb, .menu, .pagination").remove();
      bodyText = el.text().trim();
      if (bodyText.length > 100) break;
    }
  }

  if (!bodyText || bodyText.length < 50) return results;

  // Split by "Sanctionarea" or "Aplicarea" which typically starts each entry
  const entryPatterns = bodyText.split(/(?=Sanc[țt]ionarea\s|Aplicarea\s|Retragerea\s|Suspendarea\s|Radierea\s)/i);

  for (const entry of entryPatterns) {
    const trimmed = entry.trim();
    if (trimmed.length < 30) continue;

    // Extract firm name — often follows "societatii" or "societății"
    const firmMatch = trimmed.match(
      /societ[aă][tț]ii\s+(.+?)(?:\s+pentru\s|\s+din\s|\s+ca\s|\s*,)/i,
    );
    const firmName = firmMatch
      ? firmMatch[1]!.trim()
      : extractFirstEntityName(trimmed);

    if (!firmName || firmName.length < 3) continue;

    // Extract action type
    let actionType = "sanctiune";
    if (/\bamend[aă]\b/i.test(trimmed)) {
      actionType = "amenda";
    } else if (/\bavertisment\b/i.test(trimmed)) {
      actionType = "avertisment";
    } else if (/\bretragere\b/i.test(trimmed)) {
      actionType = "retragere_autorizatie";
    } else if (/\bsuspendare\b/i.test(trimmed)) {
      actionType = "suspendare";
    } else if (/\bradiere\b/i.test(trimmed)) {
      actionType = "radiere";
    }

    // Extract fine amount in RON (lei)
    let amount: number | null = null;
    const amountMatch = trimmed.match(
      /(?:amend[aă]|cuantum(?:ul)?)\s+(?:de\s+)?(?:in\s+cuantum\s+de\s+)?([\d.,]+)\s*lei/i,
    );
    if (amountMatch) {
      amount = parseFloat(amountMatch[1]!.replace(/\./g, "").replace(",", "."));
    } else {
      // Alternative pattern: "X.XXX lei"
      const altMatch = trimmed.match(/([\d.]+)\s*lei/i);
      if (altMatch) {
        amount = parseFloat(altMatch[1]!.replace(/\./g, "").replace(",", "."));
      }
    }

    // Extract regulatory references
    const refMatches = trimmed.match(
      /(?:art\.\s*\d+|Legea\s+nr\.\s*\d+\/\d{4}|Norma\s+(?:ASF\s+)?nr\.\s*\d+\/\d{4}|Regulament(?:ul)?\s+(?:ASF\s+)?nr\.\s*\d+\/\d{4})/gi,
    );
    const sourcebookRefs = refMatches ? refMatches.join("; ") : null;

    results.push({
      firm_name: cleanText(firmName),
      reference_number: null,
      action_type: actionType,
      amount,
      date: null,
      summary: cleanText(trimmed.substring(0, 1000)),
      sourcebook_references: sourcebookRefs,
      market,
    });
  }

  return results;
}

/**
 * Extract the first plausible entity name from a sanctions text block.
 * Falls back to the first capitalised multi-word phrase.
 */
function extractFirstEntityName(text: string): string {
  // Try common patterns for Romanian entity names
  const patterns = [
    /([A-Z][A-Za-z\s\-\.]+(?:S\.A\.|S\.R\.L\.|S\.A|SRL|SA))/,
    /(?:societatea|entitatea|brokerul)\s+([A-Z][A-Za-z\s\-\.]+)/i,
    /(?:domnul|doamna|dl\.|d-na)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/,
  ];

  for (const pattern of patterns) {
    const m = text.match(pattern);
    if (m && m[1] && m[1].trim().length >= 3) {
      return m[1].trim();
    }
  }

  return text.substring(0, 80).trim();
}

// ─── Database operations ──────────────────────────────────────────────────────

function initDb(force: boolean): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function ensureSourcebooks(
  db: Database.Database,
  categories: CategoryDef[],
): void {
  const insert = db.prepare(
    "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
  );

  // Keep the original seed sourcebooks
  const seedSourcebooks = [
    {
      id: "ASF_NORME",
      name: "ASF Norme",
      description:
        "Norme emise de Autoritatea de Supraveghere Financiara privind piata de capital, asigurarile si pensiile private.",
    },
    {
      id: "ASF_INSTRUCTIUNI",
      name: "ASF Instructiuni",
      description:
        "Instructiuni ASF privind procedurile de autorizare, raportare si conformitate pentru participantii la piata.",
    },
    {
      id: "ASF_REGULAMENTE",
      name: "ASF Regulamente",
      description:
        "Regulamente emise de ASF privind organizarea si functionarea pietelor financiare non-bancare.",
    },
    {
      id: "BNR_REGULAMENTE",
      name: "BNR Regulamente",
      description:
        "Regulamente emise de Banca Nationala a Romaniei privind cerintele prudentiale pentru institutiile de credit.",
    },
    {
      id: "BNR_NORME",
      name: "BNR Norme",
      description:
        "Norme BNR privind supravegherea bancara, managementul riscului si cerintele de capital.",
    },
  ];

  for (const sb of seedSourcebooks) {
    insert.run(sb.id, sb.name, sb.description);
  }

  // Add sourcebooks from crawl categories
  for (const cat of categories) {
    insert.run(cat.sourcebook_id, cat.name, cat.description);
  }
}

function insertProvision(
  db: Database.Database,
  sourcebookId: string,
  provision: ParsedProvision,
): boolean {
  // Check for duplicate by reference
  const existing = db
    .prepare("SELECT id FROM provisions WHERE reference = ? LIMIT 1")
    .get(provision.reference) as { id: number } | undefined;

  if (existing) return false;

  db.prepare(
    `INSERT INTO provisions
       (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
     VALUES (?, ?, ?, ?, ?, 'in_force', ?, ?, ?)`,
  ).run(
    sourcebookId,
    provision.reference,
    provision.title,
    provision.text,
    provision.type,
    provision.effective_date,
    provision.chapter,
    provision.section,
  );

  return true;
}

function insertEnforcement(
  db: Database.Database,
  enforcement: ParsedEnforcement,
): boolean {
  // Simple dedup: check by reference_number if present, else by firm_name + date
  if (enforcement.reference_number) {
    const existing = db
      .prepare(
        "SELECT id FROM enforcement_actions WHERE reference_number = ? LIMIT 1",
      )
      .get(enforcement.reference_number) as { id: number } | undefined;
    if (existing) return false;
  } else {
    const existing = db
      .prepare(
        "SELECT id FROM enforcement_actions WHERE firm_name = ? AND summary = ? LIMIT 1",
      )
      .get(enforcement.firm_name, enforcement.summary) as
      | { id: number }
      | undefined;
    if (existing) return false;
  }

  db.prepare(
    `INSERT INTO enforcement_actions
       (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    enforcement.firm_name,
    enforcement.reference_number,
    enforcement.action_type,
    enforcement.amount,
    enforcement.date,
    enforcement.summary,
    enforcement.sourcebook_references,
  );

  return true;
}

// ─── Phase 1: Crawl regulation listing pages ─────────────────────────────────

async function discoverRegulations(
  state: IngestState,
  options: CliOptions,
): Promise<DiscoveredArticle[]> {
  console.log("\n=== Phase 1: Discover regulations ===\n");

  const allArticles: DiscoveredArticle[] = [];

  for (const category of REGULATION_CATEGORIES) {
    console.log(`\n  Category: ${category.name}`);
    console.log(`  Path:     ${category.path}`);

    const pageUrl = `${BASE_URL}${category.path}`;
    let html: string;

    try {
      html = await rateLimitedFetch(pageUrl);
    } catch (err) {
      console.error(`  FAILED to fetch category page: ${(err as Error).message}`);
      continue;
    }

    // Extract article links from first page
    const articles = extractArticleLinks(html);
    for (const art of articles) {
      allArticles.push({
        url: art.url,
        title: art.title,
        sourcebook_id: category.sourcebook_id,
      });
    }
    console.log(`  Page 1: ${articles.length} links found`);

    // Check for pagination
    const paginationUrls = extractPaginationUrls(html, category.path);
    for (const pageLink of paginationUrls) {
      try {
        const pageHtml = await rateLimitedFetch(pageLink);
        const pageArticles = extractArticleLinks(pageHtml);
        for (const art of pageArticles) {
          allArticles.push({
            url: art.url,
            title: art.title,
            sourcebook_id: category.sourcebook_id,
          });
        }
        console.log(`  ${pageLink.split("?")[1]}: ${pageArticles.length} links found`);
      } catch (err) {
        console.error(`  FAILED page ${pageLink}: ${(err as Error).message}`);
      }
    }
  }

  // Deduplicate by URL
  const seen = new Set<string>();
  const unique = allArticles.filter((a) => {
    if (seen.has(a.url)) return false;
    seen.add(a.url);
    return true;
  });

  // Filter out already-crawled URLs if resuming
  const filtered = options.resume
    ? unique.filter((a) => !state.crawled_urls.includes(a.url))
    : unique;

  // Apply limit
  const limited = options.limit > 0 ? filtered.slice(0, options.limit) : filtered;

  console.log(`\n  Total discovered: ${allArticles.length}`);
  console.log(`  Unique articles:  ${unique.length}`);
  if (options.resume) {
    console.log(`  Already crawled:  ${unique.length - filtered.length}`);
  }
  console.log(`  To process:       ${limited.length}`);

  return limited;
}

// ─── Phase 2: Fetch and parse individual regulation pages ────────────────────

async function crawlRegulations(
  db: Database.Database,
  articles: DiscoveredArticle[],
  state: IngestState,
  options: CliOptions,
): Promise<void> {
  console.log("\n=== Phase 2: Crawl regulation pages ===\n");

  let processed = 0;
  let inserted = 0;
  let skipped = 0;
  let failed = 0;

  for (const article of articles) {
    // Skip PDFs — we only parse HTML article pages
    if (article.url.endsWith(".pdf")) {
      skipped++;
      continue;
    }

    try {
      const html = await rateLimitedFetch(article.url);
      const provision = parseArticlePage(html, article.url);

      if (!provision) {
        skipped++;
        console.log(`  SKIP (no content): ${article.title.substring(0, 70)}`);
        state.crawled_urls.push(article.url);
        continue;
      }

      if (options.dryRun) {
        console.log(`  DRY-RUN: ${provision.reference} — ${provision.title.substring(0, 60)}`);
      } else {
        const wasInserted = insertProvision(db, article.sourcebook_id, provision);
        if (wasInserted) {
          inserted++;
          console.log(`  [${inserted}] ${provision.reference} — ${provision.title.substring(0, 60)}`);
        } else {
          skipped++;
          console.log(`  DUP: ${provision.reference}`);
        }
      }

      state.crawled_urls.push(article.url);
      processed++;

      // Save state periodically
      if (processed % 10 === 0) {
        saveState(state);
      }
    } catch (err) {
      failed++;
      console.error(`  FAILED: ${article.url} — ${(err as Error).message}`);
    }
  }

  saveState(state);

  console.log(`\n  Processed: ${processed}`);
  console.log(`  Inserted:  ${inserted}`);
  console.log(`  Skipped:   ${skipped}`);
  console.log(`  Failed:    ${failed}`);
}

// ─── Phase 3: Crawl sanctions / enforcement ──────────────────────────────────

async function crawlEnforcement(
  db: Database.Database,
  state: IngestState,
  options: CliOptions,
): Promise<void> {
  console.log("\n=== Phase 3: Crawl enforcement actions ===\n");

  let totalInserted = 0;
  let totalSkipped = 0;

  // 3a. Crawl yearly sanctions index pages per market
  for (const cat of SANCTIONS_CATEGORIES) {
    console.log(`\n  Market: ${cat.name}`);

    let html: string;
    try {
      html = await rateLimitedFetch(`${BASE_URL}${cat.path}`);
    } catch (err) {
      console.error(`  FAILED to fetch: ${(err as Error).message}`);
      continue;
    }

    // Extract links to yearly pages
    const yearLinks = extractArticleLinks(html);
    console.log(`  Found ${yearLinks.length} yearly pages`);

    for (const yearLink of yearLinks) {
      if (options.resume && state.crawled_urls.includes(yearLink.url)) {
        console.log(`  SKIP (already crawled): ${yearLink.title}`);
        continue;
      }

      // Skip non-sanctions pages (e.g. "Prevederi legale specifice")
      if (!/\bsanc[tț]iuni\b/i.test(yearLink.title) && !/\bm[aă]suri\b/i.test(yearLink.title)) {
        continue;
      }

      try {
        const yearHtml = await rateLimitedFetch(yearLink.url);
        const yearMatch = yearLink.title.match(/(\d{4})/);
        const year = yearMatch ? yearMatch[1]! : "unknown";

        const enforcements = parseSanctionsIndexPage(yearHtml, cat.market, year);

        for (const enf of enforcements) {
          if (options.dryRun) {
            console.log(`    DRY-RUN: ${enf.reference_number} — ${enf.firm_name}`);
          } else {
            const wasInserted = insertEnforcement(db, enf);
            if (wasInserted) {
              totalInserted++;
            } else {
              totalSkipped++;
            }
          }
        }

        console.log(`    ${yearLink.title}: ${enforcements.length} decisions`);
        state.crawled_urls.push(yearLink.url);
      } catch (err) {
        console.error(`    FAILED: ${yearLink.url} — ${(err as Error).message}`);
      }
    }
  }

  // 3b. Crawl monthly sanctions summary pages
  console.log(`\n  Crawling monthly sanctions summaries...`);

  try {
    const summaryHtml = await rateLimitedFetch(`${BASE_URL}${MONTHLY_SANCTIONS_CATEGORY}`);
    const summaryLinks = extractArticleLinks(summaryHtml);

    // Filter for "Decizii de sanctionare" monthly pages
    const monthlyLinks = summaryLinks.filter((l) =>
      /decizii\s+de\s+sanc[tț]ionare/i.test(l.title),
    );
    console.log(`  Found ${monthlyLinks.length} monthly summary pages`);

    const limitedMonthly =
      options.limit > 0 ? monthlyLinks.slice(0, options.limit) : monthlyLinks;

    for (const link of limitedMonthly) {
      if (options.resume && state.crawled_urls.includes(link.url)) {
        continue;
      }

      try {
        const monthHtml = await rateLimitedFetch(link.url);
        const enforcements = parseMonthlySanctionsSummary(monthHtml, "consiliu_asf");

        for (const enf of enforcements) {
          if (options.dryRun) {
            console.log(`    DRY-RUN: ${enf.firm_name.substring(0, 50)} — ${enf.action_type}`);
          } else {
            const wasInserted = insertEnforcement(db, enf);
            if (wasInserted) {
              totalInserted++;
            } else {
              totalSkipped++;
            }
          }
        }

        console.log(`    ${link.title.substring(0, 60)}: ${enforcements.length} entries`);
        state.crawled_urls.push(link.url);
      } catch (err) {
        console.error(`    FAILED: ${link.url} — ${(err as Error).message}`);
      }
    }
  } catch (err) {
    console.error(`  FAILED to fetch sanctions category: ${(err as Error).message}`);
  }

  saveState(state);

  console.log(`\n  Enforcement inserted: ${totalInserted}`);
  console.log(`  Enforcement skipped:  ${totalSkipped}`);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const options = parseArgs();
  const state = options.resume ? loadState() : { crawled_urls: [], last_phase: "", last_run: "" };

  console.log("ASF Ingestion Crawler");
  console.log("=====================");
  console.log(`Database:  ${DB_PATH}`);
  console.log(`Phase:     ${options.phase}`);
  console.log(`Dry run:   ${options.dryRun}`);
  console.log(`Resume:    ${options.resume}`);
  console.log(`Force:     ${options.force}`);
  console.log(`Limit:     ${options.limit || "none"}`);
  console.log(`Rate:      ${RATE_LIMIT_MS}ms between requests`);
  if (options.resume) {
    console.log(`Resumed:   ${state.crawled_urls.length} URLs already processed`);
  }

  const db = options.dryRun ? null : initDb(options.force);

  if (db) {
    ensureSourcebooks(db, REGULATION_CATEGORIES);
  }

  // Phase 1 + 2: Regulations
  if (options.phase === "all" || options.phase === "regulations") {
    const articles = await discoverRegulations(state, options);

    if (articles.length > 0) {
      if (db) {
        await crawlRegulations(db, articles, state, options);
      } else {
        // Dry run — just log discovered articles
        for (const art of articles) {
          console.log(`  DRY-RUN discovered: ${art.sourcebook_id} — ${art.title.substring(0, 70)}`);
        }
      }
    }

    state.last_phase = "regulations";
  }

  // Phase 3: Enforcement
  if (options.phase === "all" || options.phase === "enforcement") {
    if (db) {
      await crawlEnforcement(db, state, options);
    } else {
      console.log("\n=== Phase 3: Enforcement (dry run — discovery only) ===\n");

      for (const cat of SANCTIONS_CATEGORIES) {
        try {
          const html = await rateLimitedFetch(`${BASE_URL}${cat.path}`);
          const links = extractArticleLinks(html);
          console.log(`  ${cat.name}: ${links.length} yearly pages`);
          for (const l of links) {
            console.log(`    ${l.title}`);
          }
        } catch (err) {
          console.error(`  FAILED: ${(err as Error).message}`);
        }
      }
    }

    state.last_phase = "enforcement";
  }

  // Final stats
  state.last_run = new Date().toISOString();
  saveState(state);

  if (db) {
    const provisionCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions").get() as { cnt: number }
    ).cnt;
    const enforcementCount = (
      db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as {
        cnt: number;
      }
    ).cnt;
    const sourcebookCount = (
      db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as { cnt: number }
    ).cnt;

    console.log("\n=== Database summary ===");
    console.log(`  Sourcebooks:         ${sourcebookCount}`);
    console.log(`  Provisions:          ${provisionCount}`);
    console.log(`  Enforcement actions: ${enforcementCount}`);

    db.close();
  }

  console.log(`\nState saved to ${STATE_PATH}`);
  console.log("Done.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
