import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { chromium } from "playwright";

import type {
  BrowserProfile,
  ElementSnapshot,
  ExtractionLogger,
  ExtractionPlan,
  PageSnapshot,
} from "./types.js";

const MAX_HTML_CHARS = 16000;
const MAX_TEXT_CHARS = 8000;
const MAX_ELEMENTS = 120;
const DEFAULT_NAVIGATION_TIMEOUT_MS = 45_000;
const AMAZON_NAVIGATION_TIMEOUT_MS = 65_000;
const AMAZON_NAVIGATION_RETRIES = 2;
const TARGET_RESULTS_RETRY_COUNT = 5;
const TARGET_RESULTS_WAIT_MS = 3_000;
const PLAYWRIGHT_DEBUG_DIR = path.resolve(process.cwd(), "output/playwright");
const INTERNAL_TARGET_PRODUCT_URL_FIELD = "__target_product_url";

const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

function logExtraction(
  logger: ExtractionLogger | undefined,
  message: string,
  level: "info" | "warn" | "error" = "info",
): void {
  logger?.(message, level);
}

function normalizeSelector(selector: string): string {
  return selector.replace(/:nth-of-type\(1\)/g, "");
}

function parseNumber(raw: string): number | null {
  const firstNumber = raw.replace(/\s+/g, " ").match(/-?\d[\d,]*(?:\.\d+)?/);
  const normalized = firstNumber?.[0]?.replace(/,/g, "") ?? "";

  if (!normalized || normalized === "-") {
    return null;
  }

  const parsed = Number(normalized);

  return Number.isFinite(parsed) ? parsed : null;
}

function browserProfileToContextOptions(browserProfile?: BrowserProfile) {
  return {
    viewport: browserProfile?.viewport ?? { width: 1440, height: 1200 },
    userAgent: browserProfile?.userAgent ?? DEFAULT_USER_AGENT,
    locale: browserProfile?.locale ?? "en-US",
    timezoneId: browserProfile?.timezoneId ?? "UTC",
  };
}

export function isTargetSearchUrl(rawUrl: string): boolean {
  try {
    const url = new URL(rawUrl);
    return url.hostname.includes("target.") && url.pathname.startsWith("/s");
  } catch {
    return false;
  }
}

async function ensureTargetSearchResultsReady(
  page: import("playwright").Page,
  logger?: ExtractionLogger,
): Promise<void> {
  let lastState:
    | {
        pageTitle: string;
        productLinkCount: number;
        targetTitleCount: number;
        productCardCount: number;
        placeholderCount: number;
        priceCount: number;
        currentPriceCount: number;
        hasErrorDialog: boolean;
        hasBotChallenge: boolean;
      }
    | undefined;

  for (let attempt = 1; attempt <= TARGET_RESULTS_RETRY_COUNT; attempt += 1) {
    const state = await page.evaluate(() => {
      const pageText = (document.body.innerText || "").replace(/\s+/g, " ").trim();
      const productLinkCount = document.querySelectorAll('a[href*="/p/"]').length;
      const targetTitleCount = document.querySelectorAll(
        'a[data-test="@web/ProductCard/title"]',
      ).length;
      const productCardCount = document.querySelectorAll(
        '[data-test="@web/ProductCard/ProductCardVariantDefault"]',
      ).length;
      const placeholderCount = document.querySelectorAll(
        '[data-test="@web/site-top-of-funnel/ProductCardPlaceholder"]',
      ).length;
      const priceCount = Array.from(document.querySelectorAll("span, div")).filter((element) =>
        /^\$\s*\d/.test((element.textContent || "").replace(/\s+/g, " ").trim()),
      ).length;
      const currentPriceCount = document.querySelectorAll('span[data-test="current-price"]').length;

      return {
        pageTitle: document.title,
        productLinkCount,
        targetTitleCount,
        productCardCount,
        placeholderCount,
        priceCount,
        currentPriceCount,
        hasErrorDialog:
          /something went wrong/i.test(pageText) && /please try again/i.test(pageText),
        hasBotChallenge:
          /verify you are human|access denied|captcha|automated access|security check/i.test(pageText),
      };
    });
    lastState = state;

    logExtraction(
      logger,
      `Target readiness check ${attempt}/${TARGET_RESULTS_RETRY_COUNT}: title="${state.pageTitle}", product_links=${state.productLinkCount}, target_titles=${state.targetTitleCount}, product_cards=${state.productCardCount}, placeholders=${state.placeholderCount}, prices=${state.priceCount}, current_prices=${state.currentPriceCount}, error_dialog=${state.hasErrorDialog}, bot_challenge=${state.hasBotChallenge}.`,
      state.productLinkCount > 0 ||
        state.targetTitleCount > 0 ||
        state.productCardCount > 0 ||
        state.priceCount > 0 ||
        state.currentPriceCount > 0
        ? "info"
        : "warn",
    );

    if (
      state.productLinkCount > 0 ||
      state.targetTitleCount > 0 ||
      state.productCardCount > 0 ||
      state.priceCount > 0 ||
      state.currentPriceCount > 0
    ) {
      return;
    }

    if (attempt < TARGET_RESULTS_RETRY_COUNT) {
      await page.waitForLoadState("networkidle", { timeout: 5_000 }).catch(() => undefined);
      await page.waitForTimeout(TARGET_RESULTS_WAIT_MS).catch(() => undefined);
    } else if (state.hasErrorDialog || state.hasBotChallenge) {
      const artifactPaths = await savePlaywrightDebugArtifacts(page, "target-readiness-failed");
      throw new Error(
        `Target returned a degraded search page or bot challenge instead of real product results. Debug screenshot: ${artifactPaths.screenshotPath}. Debug HTML: ${artifactPaths.htmlPath}. Try another IP/session and retry.`,
      );
    }
  }

  const artifactPaths = await savePlaywrightDebugArtifacts(page, "target-readiness-timeout");
  logExtraction(
    logger,
    `Target readiness checks timed out without detecting product cards. Continuing extraction anyway. Debug screenshot: ${artifactPaths.screenshotPath}. Debug HTML: ${artifactPaths.htmlPath}. Last state: ${JSON.stringify(lastState ?? {})}`,
    "warn",
  );
}

async function savePlaywrightDebugArtifacts(
  page: import("playwright").Page,
  baseName: string,
): Promise<{
  screenshotPath: string;
  htmlPath: string;
}> {
  await mkdir(PLAYWRIGHT_DEBUG_DIR, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const screenshotPath = path.join(PLAYWRIGHT_DEBUG_DIR, `${baseName}-${stamp}.png`);
  const htmlPath = path.join(PLAYWRIGHT_DEBUG_DIR, `${baseName}-${stamp}.html`);

  await page.screenshot({
    path: screenshotPath,
    fullPage: true,
  });
  await writeFile(htmlPath, await page.content(), "utf8");

  return {
    screenshotPath,
    htmlPath,
  };
}

async function gotoWithRetry(
  page: import("playwright").Page,
  url: string,
  options?: {
    timeoutMs?: number;
    retries?: number;
  },
): Promise<void> {
  const timeoutMs = options?.timeoutMs ?? DEFAULT_NAVIGATION_TIMEOUT_MS;
  const retries = options?.retries ?? 0;
  let lastError: unknown;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      await page.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: timeoutMs,
      });
      await page.waitForTimeout(1_500);
      return;
    } catch (error) {
      lastError = error;

      if (attempt < retries) {
        await page.waitForTimeout(1_500);
      }
    }
  }

  throw lastError;
}

function normalizeExtractedValue(
  rawValue: string,
  transform: "none" | "trim" | "number" | "url",
  baseUrl: string,
): string | number {
  const trimmed = rawValue.trim();

  switch (transform) {
    case "number": {
      const parsed = parseNumber(trimmed);
      return parsed ?? trimmed;
    }
    case "url":
      try {
        return new URL(trimmed, baseUrl).toString();
      } catch {
        return trimmed;
      }
    case "trim":
      return trimmed;
    default:
      return trimmed;
  }
}

function finalizeFieldValue(
  field: ExtractionPlan["fields"][number],
  normalizedValues: Array<string | number>,
): unknown {
  if (field.type === "object") {
    return null;
  }

  if (field.type === "object[]") {
    return [];
  }

  if (field.multiple || field.type.endsWith("[]")) {
    return normalizedValues;
  }

  const first = normalizedValues[0];

  if (typeof first === "number") {
    return first;
  }

  if (field.type === "number") {
    return typeof first === "string" ? parseNumber(first) : null;
  }

  if (field.type === "boolean") {
    return first ? ["true", "yes", "1"].includes(String(first).toLowerCase()) : null;
  }

  return first ?? null;
}

function isPresentValue(value: unknown): boolean {
  if (Array.isArray(value)) {
    return value.length > 0;
  }

  return value !== null && value !== undefined && value !== "";
}

function buildGenericDedupeKey(item: Record<string, unknown>, requiredFieldNames: string[]): string {
  const preferredKeys = ["asin", "title", "image", "url", "link", "id"];
  const selectedValues = [...preferredKeys, ...requiredFieldNames]
    .map((key) => item[key])
    .filter((value, index, values) => values.indexOf(value) === index)
    .filter((value): value is string => typeof value === "string" && value.trim().length > 0);

  return selectedValues.slice(0, 3).join("::");
}

function findContainersForPlan(
  page: import("playwright").Page,
  plan: ExtractionPlan,
): Promise<Array<import("playwright").ElementHandle<Node>>> {
  const containerSelectors = [plan.itemContainerSelector, ...(plan.itemContainerFallbackSelectors ?? [])].filter(
    Boolean,
  ) as string[];

  return (async () => {
    for (const selector of containerSelectors) {
      try {
        const matches = await page.locator(selector).elementHandles();
        if (matches.length > 0) {
          return matches;
        }
      } catch {
        continue;
      }
    }

    return [];
  })();
}

function inferTargetProductType(title: string | null): string | null {
  const normalizedTitle = title?.toLowerCase() ?? "";

  if (!normalizedTitle) {
    return null;
  }

  const candidates = [
    "sheet set",
    "bed sheets",
    "bed sheet",
    "bed frame",
    "headboard",
    "comforter",
    "duvet cover",
    "mattress pad",
    "blanket",
    "quilt",
    "pillow",
    "sham",
    "coverlet",
    "bedspread",
  ];

  const match = candidates.find((candidate) => normalizedTitle.includes(candidate));
  return match ?? null;
}

function extractTargetRatingSummary(bodyText: string | null): {
  reviewRate: string | null;
  reviewDetail: string | null;
} {
  const normalized = bodyText?.replace(/\s+/g, " ").trim() ?? "";

  if (!normalized) {
    return {
      reviewRate: null,
      reviewDetail: null,
    };
  }

  const compact = normalized.replace(/\s+/g, "");
  const compactMatch = compact.match(/(\d(?:\.\d)?)\(([\d.,]+[kKmM]?)\)/);

  if (compactMatch) {
    return {
      reviewRate: compactMatch[1],
      reviewDetail: compactMatch[2],
    };
  }

  const starMatch = normalized.match(/(\d(?:\.\d+)?)\s*out of 5 stars/i);
  const countMatch = normalized.match(/\(([\d.,]+[kKmM]?)\)/);

  return {
    reviewRate: starMatch?.[1] ?? null,
    reviewDetail: countMatch?.[1] ?? null,
  };
}

function normalizeTargetFieldValue(
  field: ExtractionPlan["fields"][number],
  rawValue: unknown,
): unknown {
  if (field.type === "object") {
    return null;
  }

  if (field.type === "object[]") {
    return [];
  }

  if (field.type === "string[]") {
    if (Array.isArray(rawValue)) {
      return rawValue
        .map((value) => String(value).trim())
        .filter((value) => value.length > 0);
    }

    if (typeof rawValue === "string" && rawValue.trim()) {
      return [rawValue.trim()];
    }

    return [];
  }

  if (field.type === "number[]") {
    if (!Array.isArray(rawValue)) {
      return [];
    }

    return rawValue
      .map((value) => parseNumber(String(value ?? "")))
      .filter((value): value is number => value !== null);
  }

  if (field.type === "number") {
    if (typeof rawValue === "number") {
      return rawValue;
    }

    return typeof rawValue === "string" ? parseNumber(rawValue) : null;
  }

  if (field.type === "boolean") {
    if (typeof rawValue === "boolean") {
      return rawValue;
    }

    if (typeof rawValue === "string") {
      const lowered = rawValue.toLowerCase();
      return ["true", "yes", "1", "sponsored"].includes(lowered);
    }

    return Boolean(rawValue);
  }

  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return field.multiple || field.type.endsWith("[]") ? [] : null;
  }

  if (field.multiple || field.type.endsWith("[]")) {
    return Array.isArray(rawValue)
      ? rawValue.map((value) => String(value))
      : [String(rawValue)];
  }

  return String(rawValue);
}

async function extractTargetRecordFromContainer(
  plan: ExtractionPlan,
  container: import("playwright").ElementHandle<Node>,
): Promise<Record<string, unknown>> {
  const snapshot = await container.evaluate((node) => {
    const root = node as HTMLElement;
    const titleElement = root.querySelector(
      'a[data-test="@web/ProductCard/title"]',
    ) as HTMLAnchorElement | null;
    const imageElement = root.querySelector(
      '[data-test="@web/ProductCard/ProductCardImage/primary"] img, img',
    ) as HTMLImageElement | null;
    const currentPriceElement = root.querySelector(
      'span[data-test="current-price"]',
    ) as HTMLElement | null;
    const comparisonPriceElement = root.querySelector(
      '[data-test="comparison-price"]',
    ) as HTMLElement | null;
    const swatchesElement = root.querySelector(
      'span[data-test="@web/ProductCard/ProductCardSwatches"]',
    ) as HTMLElement | null;
    const brandElement = root.querySelector(
      '[data-test="@web/ProductCard/ProductCardBrandAndRibbonMessage/brand"]',
    ) as HTMLElement | null;
    const sponsoredElement = root.querySelector('[data-test="sponsoredText"]') as HTMLElement | null;
    const detailElement = root.querySelector('[data-test="product-details"]') as HTMLElement | null;

    return {
      title:
        (titleElement?.innerText || titleElement?.textContent || "").replace(/\s+/g, " ").trim() || null,
      productUrl: titleElement?.getAttribute("href") || null,
      imageUrl: imageElement?.getAttribute("src") || null,
      currentPrice:
        (currentPriceElement?.innerText || currentPriceElement?.textContent || "")
          .replace(/\s+/g, " ")
          .trim() || null,
      comparisonPrice:
        (comparisonPriceElement?.innerText || comparisonPriceElement?.textContent || "")
          .replace(/\s+/g, " ")
          .trim() || null,
      swatchColors: swatchesElement?.getAttribute("aria-label") || null,
      brand:
        (brandElement?.innerText || brandElement?.textContent || "").replace(/\s+/g, " ").trim() || null,
      sponsored:
        (sponsoredElement?.innerText || sponsoredElement?.textContent || "")
          .replace(/\s+/g, " ")
          .trim() || null,
      detailText:
        (detailElement?.innerText || detailElement?.textContent || "").replace(/\s+/g, " ").trim() || null,
      bodyText: (root.innerText || root.textContent || "").replace(/\s+/g, " ").trim() || null,
    };
  });

  const ratingSummary = extractTargetRatingSummary(snapshot.bodyText);
  const inferredType = inferTargetProductType(snapshot.title);
  const description = snapshot.title ?? snapshot.detailText ?? snapshot.bodyText;
  const item: Record<string, unknown> = {};

  for (const field of plan.fields) {
    const lowerName = field.name.toLowerCase();
    let rawValue: unknown = null;

    if (lowerName === "title" || lowerName.includes("title")) {
      rawValue = snapshot.title;
    } else if (
      lowerName === "product_url" ||
      lowerName === "url" ||
      lowerName.includes("link") ||
      (field.attribute === "href" && field.selector.includes("title"))
    ) {
      rawValue = snapshot.productUrl;
    } else if (lowerName === "image" || lowerName.includes("image") || field.type === "image") {
      rawValue = snapshot.imageUrl;
    } else if (lowerName === "price" || lowerName.includes("price")) {
      rawValue = snapshot.currentPrice ?? snapshot.detailText;
    } else if (lowerName === "color" || lowerName.includes("color")) {
      rawValue = snapshot.swatchColors;
    } else if (
      lowerName === "review_rate" ||
      lowerName === "rating" ||
      (lowerName.includes("review") && lowerName.includes("rate"))
    ) {
      rawValue = ratingSummary.reviewRate;
    } else if (
      lowerName === "review_detail" ||
      lowerName === "review_count" ||
      (lowerName.includes("review") &&
        (lowerName.includes("detail") || lowerName.includes("count")))
    ) {
      rawValue = ratingSummary.reviewDetail;
    } else if (
      lowerName === "description" ||
      lowerName === "desc" ||
      lowerName.includes("description")
    ) {
      rawValue = description;
    } else if (lowerName === "type" || lowerName.includes("category")) {
      rawValue = inferredType;
    } else if (lowerName === "brand") {
      rawValue = snapshot.brand;
    } else if (lowerName.includes("sponsored")) {
      rawValue = snapshot.sponsored;
    }

    item[field.name] = normalizeTargetFieldValue(field, rawValue);
  }

  item[INTERNAL_TARGET_PRODUCT_URL_FIELD] = snapshot.productUrl;

  return item;
}

async function extractRecordFromContainer(
  page: import("playwright").Page,
  plan: ExtractionPlan,
  container: import("playwright").ElementHandle<Node>,
): Promise<Record<string, unknown>> {
  if (isTargetSearchUrl(page.url())) {
    return extractTargetRecordFromContainer(plan, container);
  }

  const item: Record<string, unknown> = {};

  for (const field of plan.fields) {
    const values = await extractFieldValues(page, field, {
      scopeHandle: container,
    });
    const normalizedValues = values
      .map((value) => normalizeExtractedValue(value, field.transform, page.url()))
      .filter((value) => value !== "");

    item[field.name] = finalizeFieldValue(field, normalizedValues);
  }

  return item;
}

function filterExtractedListRecords(
  records: Array<Record<string, unknown>>,
  plan: ExtractionPlan,
  options?: {
    logger?: ExtractionLogger;
  },
): Array<Record<string, unknown>> {
  const requiredFieldNames = plan.fields.filter((field) => field.required).map((field) => field.name);
  const seenKeys = new Set<string>();
  let droppedEmpty = 0;
  let droppedMissingRequired = 0;
  let droppedDuplicate = 0;
  const missingRequiredFieldCounts = new Map<string, number>();

  const keptRecords = records.filter((item) => {
    const hasMeaningfulValue = Object.values(item).some((value) => {
      if (Array.isArray(value)) {
        return value.length > 0;
      }
      return value !== null && value !== "";
    });

    if (!hasMeaningfulValue) {
      droppedEmpty += 1;
      return false;
    }

    const missingRequiredFields = requiredFieldNames.filter((fieldName) => !isPresentValue(item[fieldName]));
    const hasAllRequiredFields = missingRequiredFields.length === 0;

    if (!hasAllRequiredFields) {
      droppedMissingRequired += 1;
      for (const fieldName of missingRequiredFields) {
        missingRequiredFieldCounts.set(fieldName, (missingRequiredFieldCounts.get(fieldName) ?? 0) + 1);
      }
      return false;
    }

    const dedupeKey = buildGenericDedupeKey(item, requiredFieldNames);

    if (dedupeKey && seenKeys.has(dedupeKey)) {
      droppedDuplicate += 1;
      return false;
    }

    if (dedupeKey) {
      seenKeys.add(dedupeKey);
    }

    return true;
  });

  logExtraction(
    options?.logger,
    `List filter summary: raw=${records.length}, kept=${keptRecords.length}, empty_dropped=${droppedEmpty}, missing_required_dropped=${droppedMissingRequired}, duplicate_dropped=${droppedDuplicate}.`,
    keptRecords.length === 0 && records.length > 0 ? "warn" : "info",
  );

  if (missingRequiredFieldCounts.size > 0) {
    const details = Array.from(missingRequiredFieldCounts.entries())
      .sort((left, right) => right[1] - left[1])
      .map(([fieldName, count]) => `${fieldName}:${count}`)
      .join(", ");

    logExtraction(
      options?.logger,
      `Missing required field counts: ${details}`,
      "warn",
    );
  }

  return keptRecords;
}

async function extractListRecordsFromPage(
  page: import("playwright").Page,
  plan: ExtractionPlan,
  options?: {
    logger?: ExtractionLogger;
  },
): Promise<Array<Record<string, unknown>>> {
  async function buildAlignedRecords(): Promise<Array<Record<string, unknown>>> {
    const alignedFieldValues = await Promise.all(
      plan.fields.map(async (field) => {
        const rawValues = await extractFieldValues(page, field);
        const normalizedValues = rawValues
          .map((value) => normalizeExtractedValue(value, field.transform, page.url()))
          .filter((value) => value !== "");

        return [field, normalizedValues] as const;
      }),
    );

    for (const [field, normalizedValues] of alignedFieldValues) {
      logExtraction(
        options?.logger,
        `Field-aligned fallback selector count for "${field.name}": ${normalizedValues.length}`,
        "warn",
      );
    }

    const rowCount = alignedFieldValues.reduce((maxCount, [, values]) => Math.max(maxCount, values.length), 0);

    if (rowCount === 0) {
      return [];
    }

    logExtraction(
      options?.logger,
      `Field-aligned fallback found ${rowCount} candidate rows.`,
      "warn",
    );

    const rawRecords: Array<Record<string, unknown>> = [];

    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      const item: Record<string, unknown> = {};

      for (const [field, normalizedValues] of alignedFieldValues) {
        if (field.type === "object") {
          item[field.name] = null;
          continue;
        }

        if (field.type === "object[]") {
          item[field.name] = [];
          continue;
        }

        if (field.multiple || field.type.endsWith("[]")) {
          const rowValue = normalizedValues[rowIndex];
          item[field.name] = rowValue === undefined ? [] : [rowValue];
          continue;
        }

        const rowValue = normalizedValues[rowIndex];

        if (rowValue === undefined) {
          item[field.name] = null;
          continue;
        }

        if (field.type === "number") {
          item[field.name] =
            typeof rowValue === "number" ? rowValue : parseNumber(String(rowValue ?? ""));
          continue;
        }

        if (field.type === "boolean") {
          const lowered = String(rowValue).toLowerCase();
          item[field.name] = lowered.includes("verified purchase") || ["true", "yes", "1"].includes(lowered);
          continue;
        }

        item[field.name] = rowValue;
      }

      rawRecords.push(item);
    }

    return filterExtractedListRecords(rawRecords, plan, {
      logger: options?.logger,
    });
  }

  let containers = await findContainersForPlan(page, plan);

  if (containers.length === 0) {
    logExtraction(
      options?.logger,
      "No list item containers matched the plan. Falling back to field-aligned list extraction.",
      "warn",
    );

    let alignedRecords = await buildAlignedRecords();

    if (alignedRecords.length > 0) {
      return alignedRecords;
    }

    logExtraction(
      options?.logger,
      "First pass found no containers or aligned field results. Waiting for dynamic content and retrying.",
      "warn",
    );

    if (page.isClosed()) {
      throw new Error("The page was closed before list extraction retry could run.");
    }

    await page.waitForLoadState("networkidle", { timeout: 5_000 }).catch(() => undefined);
    await page.waitForTimeout(1_500).catch(() => undefined);

    if (page.isClosed()) {
      throw new Error("The page was closed during list extraction retry.");
    }

    containers = await findContainersForPlan(page, plan);

    if (containers.length === 0) {
      alignedRecords = await buildAlignedRecords();

      if (alignedRecords.length > 0) {
        return alignedRecords;
      }

      throw new Error(
        "List extraction was requested, but no item containers or field-aligned results were found after retry.",
      );
    }
  }

  const rawRecords: Array<Record<string, unknown>> = [];

  for (const container of containers) {
    rawRecords.push(await extractRecordFromContainer(page, plan, container));
  }

  return filterExtractedListRecords(rawRecords, plan, {
    logger: options?.logger,
  });
}

async function extractFieldValues(
  page: import("playwright").Page,
  field: ExtractionPlan["fields"][number],
  options?: {
    scopeHandle?: import("playwright").ElementHandle<Node>;
  },
): Promise<string[]> {
  const selectors = [field.selector, ...field.fallbackSelectors].filter(Boolean);

  for (const selector of selectors) {
    let handles: Array<import("playwright").ElementHandle<Node>>;

    try {
      if (options?.scopeHandle) {
        handles =
          selector === ":scope" ? [options.scopeHandle] : await options.scopeHandle.$$(selector);
      } else {
        handles = await page.locator(selector).elementHandles();
      }
    } catch {
      continue;
    }

    if (handles.length === 0) {
      continue;
    }

    const values: string[] = [];

    for (const handle of handles) {
      const raw = await handle.evaluate(
        (node, source) => {
          const element = node as HTMLElement;

          if (source.kind === "attribute") {
            return element.getAttribute(source.attributeName || "") || "";
          }

          if ((element as HTMLImageElement).src && element.tagName.toLowerCase() === "img") {
            return (element as HTMLImageElement).src;
          }

          return (element.innerText || element.textContent || "").trim();
        },
        {
          kind: field.source,
          attributeName: field.attribute,
        },
      );

      if (raw) {
        values.push(raw);
      }
    }

    if (values.length > 0) {
      return values;
    }
  }

  return [];
}

export async function capturePage(
  url: string,
  browserProfile?: BrowserProfile,
  options?: {
    logger?: ExtractionLogger;
  },
): Promise<PageSnapshot> {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage(browserProfileToContextOptions(browserProfile));

  try {
    await gotoWithRetry(page, url, {
      timeoutMs: isAmazonSearchUrl(url) ? AMAZON_NAVIGATION_TIMEOUT_MS : DEFAULT_NAVIGATION_TIMEOUT_MS,
      retries: isAmazonSearchUrl(url) ? AMAZON_NAVIGATION_RETRIES : 0,
    });

    if (isTargetSearchUrl(url)) {
      logExtraction(options?.logger, "Waiting for Target search results to render.");
      await ensureTargetSearchResultsReady(page, options?.logger);
    }

    const snapshot = await page.evaluate(
      ({ maxHtmlChars, maxTextChars, maxElements }) => {
        // Avoid local named functions here: tsx/esbuild can inject __name helpers
        // into page.evaluate callbacks, but the browser page context does not have them.
        const elements: Array<{
          selector: string;
          tag: string;
          text: string;
          href?: string;
          src?: string;
          ariaLabel?: string;
        }> = [];
        const attrNames = ["data-testid", "data-test", "data-qa", "data-component"];

        for (const element of Array.from(document.querySelectorAll("body *"))) {
          const htmlElement = element as HTMLElement;
          const text = htmlElement.innerText?.trim();
          const href = htmlElement.getAttribute("href");
          const src = htmlElement.getAttribute("src");

          if (!text && !href && !src) {
            continue;
          }

          let selector = "";

          if (htmlElement.id) {
            selector = `#${CSS.escape(htmlElement.id)}`;
          } else {
            for (const attrName of attrNames) {
              const attrValue = htmlElement.getAttribute(attrName);

              if (attrValue) {
                selector = `[${attrName}="${CSS.escape(attrValue)}"]`;
                break;
              }
            }
          }

          if (!selector) {
            const parts: string[] = [];
            let current: Element | null = element;

            while (current && current !== document.body) {
              let part = current.tagName.toLowerCase();
              const currentElement = current as HTMLElement;

              if (currentElement.classList.length > 0) {
                part += Array.from(currentElement.classList)
                  .slice(0, 2)
                  .map((name) => `.${CSS.escape(name)}`)
                  .join("");
              }

              const siblings = current.parentElement
                ? Array.from(current.parentElement.children).filter(
                    (sibling) => sibling.tagName === current?.tagName,
                  )
                : [];

              if (siblings.length > 1) {
                const index = siblings.indexOf(current) + 1;
                part += `:nth-of-type(${index})`;
              }

              parts.unshift(part);
              current = current.parentElement;
            }

            selector = parts.join(" > ");
          }

          elements.push({
            selector,
            tag: element.tagName.toLowerCase(),
            text: (htmlElement.innerText || "").trim().replace(/\s+/g, " ").slice(0, 160),
            href: href || undefined,
            src: src || undefined,
            ariaLabel: htmlElement.getAttribute("aria-label") || undefined,
          });

          if (elements.length >= maxElements) {
            break;
          }
        }

        return {
          title: document.title,
          description:
            document.querySelector('meta[name="description"]')?.getAttribute("content") || "",
          htmlExcerpt: document.documentElement.outerHTML
            .replace(/\s+/g, " ")
            .slice(0, maxHtmlChars),
          textExcerpt: (document.body.innerText || "").replace(/\s+/g, " ").slice(0, maxTextChars),
          elements,
        };
      },
      {
        maxHtmlChars: MAX_HTML_CHARS,
        maxTextChars: MAX_TEXT_CHARS,
        maxElements: MAX_ELEMENTS,
      },
    );

    return {
      url: page.url(),
      title: snapshot.title,
      description: snapshot.description,
      htmlExcerpt: snapshot.htmlExcerpt,
      textExcerpt: snapshot.textExcerpt,
      elements: (snapshot.elements as ElementSnapshot[]).map((element) => ({
        ...element,
        selector: normalizeSelector(element.selector),
      })),
    };
  } finally {
    await browser.close();
  }
}

export function isAmazonSearchUrl(rawUrl: string): boolean {
  try {
    const url = new URL(rawUrl);
    return url.hostname.includes("amazon.") && url.pathname.startsWith("/s");
  } catch {
    return false;
  }
}

export function buildAmazonSearchPlan(): ExtractionPlan {
  return {
    reasoning:
      "Amazon search results use stable search result cards with data-component-type=s-search-result and data-asin. Extract one record per result card and ignore feedback, recommendation, and non-product blocks.",
    pageType: "amazon_search_results",
    extractionMode: "list",
    itemContainerSelector: '[data-component-type="s-search-result"][data-asin]:not([data-asin=""])',
    itemContainerFallbackSelectors: [
      'div.s-main-slot > div[data-component-type="s-search-result"][data-asin]',
    ],
    fields: [
      {
        name: "asin",
        description: "Amazon product ASIN",
        type: "string",
        required: true,
        multiple: false,
        selector: ':scope',
        fallbackSelectors: [],
        source: "attribute",
        attribute: "data-asin",
        transform: "trim",
      },
      {
        name: "title",
        description: "Search result product title",
        type: "string",
        required: true,
        multiple: false,
        selector: "h2 a span",
        fallbackSelectors: ["h2 span", "[data-cy='title-recipe'] span"],
        source: "text",
        transform: "trim",
      },
      {
        name: "description",
        description: "Short descriptive text if present",
        type: "string",
        required: false,
        multiple: false,
        selector: "[data-cy='title-recipe'] span",
        fallbackSelectors: [".a-color-secondary .a-size-base", ".a-row.a-size-base.a-color-secondary"],
        source: "text",
        transform: "trim",
      },
      {
        name: "price",
        description: "Current display price",
        type: "string",
        required: true,
        multiple: false,
        selector: ".a-price .a-offscreen",
        fallbackSelectors: ["[data-cy='price-recipe'] .a-offscreen", ".a-price-whole"],
        source: "text",
        transform: "trim",
      },
      {
        name: "image",
        description: "Search result image URL",
        type: "image",
        required: true,
        multiple: false,
        selector: "img.s-image",
        fallbackSelectors: ["img[data-image-latency='s-product-image']", "img"],
        source: "attribute",
        attribute: "src",
        transform: "url",
      },
      {
        name: "product_url",
        description: "Amazon product detail page URL",
        type: "url",
        required: false,
        multiple: false,
        selector: "h2 a",
        fallbackSelectors: ["a.a-link-normal.s-line-clamp-2.s-link-style.a-text-normal"],
        source: "attribute",
        attribute: "href",
        transform: "url",
      },
      {
        name: "review_rate",
        description: "Displayed average rating",
        type: "string",
        required: true,
        multiple: false,
        selector: "span.a-icon-alt",
        fallbackSelectors: [".a-icon-star-small .a-icon-alt"],
        source: "text",
        transform: "trim",
      },
      {
        name: "review_detail",
        description: "Displayed review count",
        type: "string",
        required: true,
        multiple: false,
        selector: "a[href*='customerReviews'] span:not(.a-icon-alt)",
        fallbackSelectors: ["[aria-label*='ratings'] + span", ".a-size-base.s-underline-text"],
        source: "text",
        transform: "trim",
      },
      {
        name: "review_summary",
        description: "Summary review information from the product detail page",
        type: "object",
        required: false,
        multiple: false,
        selector: "",
        fallbackSelectors: [],
        source: "text",
        transform: "trim",
        properties: [
          {
            name: "review_count",
            description: "Total number of reviews on the product detail page",
            type: "string",
            required: false,
            multiple: false,
            selector: "",
            fallbackSelectors: [],
            source: "text",
            transform: "trim",
          },
          {
            name: "rating",
            description: "Overall average rating from the product detail page",
            type: "string",
            required: false,
            multiple: false,
            selector: "",
            fallbackSelectors: [],
            source: "text",
            transform: "trim",
          },
          {
            name: "rating_breakdown",
            description: "Rating histogram rows such as 5 star: 76%",
            type: "string[]",
            required: false,
            multiple: false,
            selector: "",
            fallbackSelectors: [],
            source: "text",
            transform: "trim",
          },
          {
            name: "top_positive_snippet",
            description: "Top positive review snippet if visible",
            type: "string",
            required: false,
            multiple: false,
            selector: "",
            fallbackSelectors: [],
            source: "text",
            transform: "trim",
          },
          {
            name: "top_critical_snippet",
            description: "Top critical review snippet if visible",
            type: "string",
            required: false,
            multiple: false,
            selector: "",
            fallbackSelectors: [],
            source: "text",
            transform: "trim",
          },
        ],
      },
      {
        name: "reviews",
        description: "First few visible reviews from the product detail page",
        type: "object[]",
        required: false,
        multiple: false,
        selector: "",
        fallbackSelectors: [],
        source: "text",
        transform: "trim",
        properties: [
          {
            name: "reviewer_name",
            description: "Reviewer display name",
            type: "string",
            required: false,
            multiple: false,
            selector: "",
            fallbackSelectors: [],
            source: "text",
            transform: "trim",
          },
          {
            name: "review_title",
            description: "Review title",
            type: "string",
            required: false,
            multiple: false,
            selector: "",
            fallbackSelectors: [],
            source: "text",
            transform: "trim",
          },
          {
            name: "review_text",
            description: "Main review text content",
            type: "string",
            required: false,
            multiple: false,
            selector: "",
            fallbackSelectors: [],
            source: "text",
            transform: "trim",
          },
          {
            name: "review_date",
            description: "Review date string",
            type: "string",
            required: false,
            multiple: false,
            selector: "",
            fallbackSelectors: [],
            source: "text",
            transform: "trim",
          },
          {
            name: "helpful_votes",
            description: "Helpful vote statement for the review",
            type: "string",
            required: false,
            multiple: false,
            selector: "",
            fallbackSelectors: [],
            source: "text",
            transform: "trim",
          },
          {
            name: "verified_purchase",
            description: "Whether the review is marked as a verified purchase",
            type: "boolean",
            required: false,
            multiple: false,
            selector: "",
            fallbackSelectors: [],
            source: "text",
            transform: "trim",
          },
        ],
      },
    ],
  };
}

function normalizeAmazonText(value: string | null | undefined): string | null {
  const trimmed = value?.replace(/\s+/g, " ").trim();
  return trimmed ? trimmed : null;
}

function normalizeTargetText(value: string | null | undefined): string | null {
  const trimmed = value?.replace(/\s+/g, " ").trim();
  return trimmed ? trimmed : null;
}

function stripInternalExtractionFields(record: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(record).filter(([key]) => !key.startsWith("__")),
  );
}

function stripInternalExtractionFieldsFromList(
  records: Array<Record<string, unknown>>,
): Array<Record<string, unknown>> {
  return records.map((record) => stripInternalExtractionFields(record));
}

function absoluteTargetUrl(rawUrl: string | null | undefined): string | null {
  const normalized = normalizeTargetText(rawUrl);

  if (!normalized) {
    return null;
  }

  try {
    return new URL(normalized, "https://www.target.com").toString();
  } catch {
    return normalized;
  }
}

function buildTargetProductUrl(record: Record<string, unknown>, plan: ExtractionPlan): string | null {
  const internalUrl =
    typeof record[INTERNAL_TARGET_PRODUCT_URL_FIELD] === "string"
      ? record[INTERNAL_TARGET_PRODUCT_URL_FIELD]
      : null;

  if (internalUrl) {
    return absoluteTargetUrl(internalUrl);
  }

  const urlField = plan.fields.find((field) => {
    const lowerName = field.name.toLowerCase();
    return (
      lowerName === "product_url" ||
      lowerName === "url" ||
      lowerName.includes("link") ||
      (field.attribute === "href" && field.selector.includes("title"))
    );
  });

  if (!urlField) {
    return null;
  }

  const rawUrlCandidate = record[urlField.name];
  return absoluteTargetUrl(typeof rawUrlCandidate === "string" ? rawUrlCandidate : null);
}

function splitTargetSectionToItems(sectionText: string | null): string[] {
  const normalized = normalizeTargetText(sectionText);

  if (!normalized) {
    return [];
  }

  const keyValueMatches = Array.from(
    normalized.matchAll(/([A-Z][A-Za-z0-9 /&"-]{1,40}:\s*.*?)(?=\s+[A-Z][A-Za-z0-9 /&"-]{1,40}:\s*|$)/g),
  )
    .map((match) => match[1]?.trim())
    .filter((value): value is string => Boolean(value));

  if (keyValueMatches.length > 0) {
    return keyValueMatches;
  }

  return normalized
    .split(/\s+(?=[A-Z][a-z][^.]+(?:\s+[A-Z][a-z][^.]+){0,8}(?::|$))/)
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function extractTargetSection(
  pageText: string,
  sectionTitle: string,
  nextTitles: string[],
): string | null {
  const escapedTitle = sectionTitle.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const escapedNextTitles = nextTitles.map((title) => title.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const boundary = escapedNextTitles.length > 0 ? escapedNextTitles.join("|") : "$";
  const pattern = new RegExp(`${escapedTitle}\\s+([\\s\\S]*?)(?=\\s+(?:${boundary})\\b|$)`, "i");
  const match = pageText.match(pattern);

  return normalizeTargetText(match?.[1] ?? null);
}

function extractTargetReviewSummary(input: {
  ratingText: string | null;
  reviewCountText: string | null;
  mainText: string;
}): {
  rating: string | null;
  review_count: string | null;
  question_count: string | null;
} {
  const combined = [input.ratingText, input.reviewCountText, input.mainText].filter(Boolean).join(" ");
  const normalized = combined.replace(/\s+/g, " ").trim();
  const ratingMatch = normalized.match(/(\d(?:\.\d+)?)\s*out of 5 stars/i);
  const reviewCountMatch = normalized.match(/with\s+([\d,]+)\s+reviews/i);
  const questionMatch = normalized.match(/([\d,]+)\s+Questions?/i);

  return {
    rating: ratingMatch?.[1] ?? null,
    review_count: reviewCountMatch?.[1]?.replace(/,/g, "") ?? null,
    question_count: questionMatch?.[1]?.replace(/,/g, "") ?? null,
  };
}

function hasTargetDetailFields(
  plan: ExtractionPlan,
  options?: {
    reviewsPerItem?: number;
  },
): boolean {
  const detailFieldNames = new Set([
    "description",
    "desc",
    "brand",
    "highlights",
    "features",
    "specifications",
    "specs",
    "details",
    "about_this_item",
    "at_a_glance",
    "shipping",
    "shipping&returns",
    "shipping_and_returns",
    "sold_by",
    "original_price",
    "review_summary",
    "reviews",
    "question_count",
    "questions",
  ]);

  return (
    Boolean(options?.reviewsPerItem) ||
    plan.fields.some((field) => {
      const lowerName = field.name.toLowerCase();
      return field.type === "object" || field.type === "object[]" || detailFieldNames.has(lowerName);
    })
  );
}

function matchesPlanFieldRole(
  field: ExtractionPlan["fields"][number],
  role: "asin" | "title" | "description" | "review_detail" | "product_url",
): boolean {
  const name = field.name.toLowerCase();

  switch (role) {
    case "asin":
      return name === "asin" || field.attribute === "data-asin";
    case "title":
      return name.includes("title") || field.selector.includes("h2");
    case "description":
      return name.includes("description") || name.includes("desc");
    case "review_detail":
      return (
        name === "review_detail" ||
        (name.includes("review") && (name.includes("detail") || name.includes("count")))
      );
    case "product_url":
      return (
        name === "product_url" ||
        name === "url" ||
        name.includes("product_url") ||
        name.includes("product link") ||
        (field.attribute === "href" && field.selector.includes("h2"))
      );
    default:
      return false;
  }
}

function getPlanRoleValue(
  record: Record<string, unknown>,
  plan: ExtractionPlan,
  role: "asin" | "title" | "description" | "review_detail" | "product_url",
): string | null {
  const field = plan.fields.find((candidate) => matchesPlanFieldRole(candidate, role));

  if (!field) {
    return null;
  }

  return normalizeAmazonText(
    typeof record[field.name] === "string" || typeof record[field.name] === "number"
      ? String(record[field.name])
      : null,
  );
}

function buildAmazonProductUrl(record: Record<string, unknown>, plan: ExtractionPlan): string | null {
  const directUrl = getPlanRoleValue(record, plan, "product_url");

  if (directUrl) {
    return directUrl;
  }

  const asin = getPlanRoleValue(record, plan, "asin");
  return asin ? `https://www.amazon.com/dp/${asin}` : null;
}

function hasAmazonDetailFields(plan: ExtractionPlan): boolean {
  return plan.fields.some((field) => field.name === "review_summary" || field.name === "reviews");
}

function fieldByName(
  fields: ExtractionPlan["fields"] | undefined,
  name: string,
): ExtractionPlan["fields"][number] | undefined {
  return fields?.find((field) => field.name === name);
}

function projectValueForField(
  field: ExtractionPlan["fields"][number],
  rawValue: unknown,
): unknown {
  if (field.type === "object") {
    const objectValue = rawValue && typeof rawValue === "object" && !Array.isArray(rawValue) ? rawValue : {};
    return Object.fromEntries(
      (field.properties ?? []).map((childField) => [
        childField.name,
        projectValueForField(
          childField,
          objectValue && typeof objectValue === "object"
            ? (objectValue as Record<string, unknown>)[childField.name]
            : undefined,
        ),
      ]),
    );
  }

  if (field.type === "object[]") {
    const items = Array.isArray(rawValue) ? rawValue : [];
    return items.map((item) =>
      Object.fromEntries(
        (field.properties ?? []).map((childField) => [
          childField.name,
          projectValueForField(
            childField,
            item && typeof item === "object" ? (item as Record<string, unknown>)[childField.name] : undefined,
          ),
        ]),
      ),
    );
  }

  if (field.type === "string[]") {
    return Array.isArray(rawValue)
      ? rawValue.map((value) => String(value)).filter((value) => value.trim().length > 0)
      : [];
  }

  if (field.type === "number[]") {
    return Array.isArray(rawValue)
      ? rawValue
          .map((value) => (typeof value === "number" ? value : parseNumber(String(value ?? ""))))
          .filter((value): value is number => value !== null)
      : [];
  }

  if (field.type === "number") {
    if (typeof rawValue === "number") {
      return rawValue;
    }
    return typeof rawValue === "string" ? parseNumber(rawValue) : null;
  }

  if (field.type === "boolean") {
    if (typeof rawValue === "boolean") {
      return rawValue;
    }

    if (typeof rawValue === "string") {
      const lowered = rawValue.toLowerCase();
      return lowered.includes("verified purchase") || ["true", "yes", "1"].includes(lowered);
    }

    return Boolean(rawValue);
  }

  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return null;
  }

  return String(rawValue);
}

async function extractAmazonDetailPayload(
  page: import("playwright").Page,
  productUrl: string,
  options?: {
    reviewItemsLimit?: number;
    asin?: string | null;
    logger?: ExtractionLogger;
    productLabel?: string;
  },
): Promise<{
  review_summary: Record<string, unknown>;
  reviews: Array<Record<string, unknown>>;
}> {
  function extractAmazonAsinFromUrl(rawUrl: string): string | null {
    const match = rawUrl.match(/\/(?:dp|gp\/product|product-reviews)\/([A-Z0-9]{10})/i);
    return match?.[1]?.toUpperCase() ?? null;
  }

  function shouldLoadReviewPage(payload: {
    review_summary: Record<string, unknown>;
    reviews: Array<Record<string, unknown>>;
  }): boolean {
    const ratingBreakdown = Array.isArray(payload.review_summary.rating_breakdown)
      ? payload.review_summary.rating_breakdown
      : [];

    return (
      ratingBreakdown.length === 0 ||
      (!payload.review_summary.top_positive_snippet && !payload.review_summary.top_critical_snippet) ||
      payload.reviews.length === 0
    );
  }

  function mergeDetailPayloads(
    primary: {
      review_summary: Record<string, unknown>;
      reviews: Array<Record<string, unknown>>;
    },
    fallback: {
      review_summary: Record<string, unknown>;
      reviews: Array<Record<string, unknown>>;
    },
  ) {
    const primaryBreakdown = Array.isArray(primary.review_summary.rating_breakdown)
      ? primary.review_summary.rating_breakdown
      : [];
    const fallbackBreakdown = Array.isArray(fallback.review_summary.rating_breakdown)
      ? fallback.review_summary.rating_breakdown
      : [];

    return {
      review_summary: {
        review_count: primary.review_summary.review_count ?? fallback.review_summary.review_count ?? null,
        rating: primary.review_summary.rating ?? fallback.review_summary.rating ?? null,
        rating_breakdown: primaryBreakdown.length > 0 ? primaryBreakdown : fallbackBreakdown,
        top_positive_snippet:
          primary.review_summary.top_positive_snippet ?? fallback.review_summary.top_positive_snippet ?? null,
        top_critical_snippet:
          primary.review_summary.top_critical_snippet ?? fallback.review_summary.top_critical_snippet ?? null,
      },
      reviews: primary.reviews.length > 0 ? primary.reviews : fallback.reviews,
    };
  }

  await gotoWithRetry(page, productUrl, {
    timeoutMs: AMAZON_NAVIGATION_TIMEOUT_MS,
    retries: AMAZON_NAVIGATION_RETRIES,
  });

  async function runDetailEvaluation<T>(work: () => Promise<T>): Promise<T> {
    try {
      return await work();
    } catch (error) {
      if (error instanceof Error && error.message.includes("Execution context was destroyed")) {
        await page.waitForLoadState("domcontentloaded").catch(() => undefined);
        await page.waitForTimeout(1_000);
        return work();
      }

      throw error;
    }
  }

  const primaryPayload = await runDetailEvaluation(() => page.evaluate(({ reviewItemsLimit }) => {
    const collapsedPageText = (document.body.innerText || "").replace(/\s+/g, " ").trim();
    const ratingBreakdownFromText: string[] = [];
    const ratingRegex = /([1-5])\s*star[s]?\s+(\d+%)/gi;
    const seenBreakdownFromText = new Set<string>();
    let ratingMatch: RegExpExecArray | null;

    while ((ratingMatch = ratingRegex.exec(collapsedPageText)) !== null) {
      const value = `${ratingMatch[1]} star: ${ratingMatch[2]}`;
      if (!seenBreakdownFromText.has(value)) {
        seenBreakdownFromText.add(value);
        ratingBreakdownFromText.push(value);
      }
    }

    const ratingBreakdown: string[] = [];
    for (const row of Array.from(
      document.querySelectorAll(
        "#histogramTable tr, [data-hook='histogram-table'] tr, .cr-widget-ACR tr, .a-size-base.a-color-base tr",
      ),
    )) {
      let label: string | null = null;
      for (const selector of ["td:first-child a", "td:first-child span", "a.a-link-normal"]) {
        const node = row.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          label = value;
          break;
        }
      }

      let detailValue: string | null = null;
      for (const selector of ["td:last-child a", "td:last-child span", ".a-text-right.a-nowrap"]) {
        const node = row.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          detailValue = value;
          break;
        }
      }

      if (label && detailValue) {
        ratingBreakdown.push(`${label}: ${detailValue}`.replace(/\s+/g, " ").trim());
      }
    }

    let reviewCount: string | null = null;
    for (const selector of [
      "#acrCustomerReviewText",
      "[data-hook='total-review-count']",
      "#filter-info-section .a-size-base",
    ]) {
      const node = document.querySelector(selector) as HTMLElement | null;
      const value = node?.innerText?.replace(/\s+/g, " ").trim();
      if (value) {
        reviewCount = value;
        break;
      }
    }

    let rating: string | null = null;
    for (const selector of [
      "#acrPopover",
      "[data-hook='rating-out-of-text']",
      "#cm_cr-product_info .a-icon-alt",
      ".reviewCountTextLinkedHistogram .a-icon-alt",
    ]) {
      const node = document.querySelector(selector) as HTMLElement | null;
      const value = node?.innerText?.replace(/\s+/g, " ").trim();
      if (value) {
        rating = value;
        break;
      }
    }

    let topPositiveSnippet: string | null = null;
    for (const selector of [
      "[data-hook='positive-review-snippet']",
      "#cr-summarization-attributes .a-list-item:first-child span",
      "#product-summary .a-list-item:first-child span",
      "[data-hook='cr-insights-widget-aspects'] .a-size-base",
    ]) {
      const node = document.querySelector(selector) as HTMLElement | null;
      const value = node?.innerText?.replace(/\s+/g, " ").trim();
      if (value) {
        topPositiveSnippet = value;
        break;
      }
    }

    if (!topPositiveSnippet) {
      for (const heading of ["Positive reviews", "Customers say"]) {
        const pattern = new RegExp(
          `${heading}\\s+(.{30,320}?)(?=\\b(?:Positive reviews|Critical reviews|Customers say|Top reviews|See more|Report|Sort by|Filter by|All stars)\\b|$)`,
          "i",
        );
        const match = collapsedPageText.match(pattern);
        const snippet = match?.[1]?.trim();
        if (snippet) {
          topPositiveSnippet = snippet;
          break;
        }
      }
    }

    let topCriticalSnippet: string | null = null;
    for (const selector of [
      "[data-hook='negative-review-snippet']",
      "#cr-summarization-attributes .a-list-item:last-child span",
      "#product-summary .a-list-item:last-child span",
      "#product-summary .a-spacing-top-small span",
    ]) {
      const node = document.querySelector(selector) as HTMLElement | null;
      const value = node?.innerText?.replace(/\s+/g, " ").trim();
      if (value) {
        topCriticalSnippet = value;
        break;
      }
    }

    if (!topCriticalSnippet) {
      for (const heading of ["Critical reviews", "Negative reviews"]) {
        const pattern = new RegExp(
          `${heading}\\s+(.{30,320}?)(?=\\b(?:Positive reviews|Critical reviews|Customers say|Top reviews|See more|Report|Sort by|Filter by|All stars)\\b|$)`,
          "i",
        );
        const match = collapsedPageText.match(pattern);
        const snippet = match?.[1]?.trim();
        if (snippet) {
          topCriticalSnippet = snippet;
          break;
        }
      }
    }

    const reviews: Array<Record<string, unknown>> = [];
    for (const reviewNode of Array.from(
      document.querySelectorAll(
        "#cm-cr-dp-review-list [data-hook='review'], #cm_cr-review_list [data-hook='review'], [data-hook='review']",
      ),
    ).slice(0, reviewItemsLimit)) {
      let reviewerName: string | null = null;
      for (const selector of [".a-profile-name", "[data-hook='genome-widget'] span"]) {
        const node = reviewNode.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          reviewerName = value;
          break;
        }
      }

      let reviewTitle: string | null = null;
      for (const selector of [
        "[data-hook='review-title'] span:not(.a-letter-space)",
        "[data-hook='review-title']",
      ]) {
        const node = reviewNode.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          reviewTitle = value;
          break;
        }
      }

      let reviewText: string | null = null;
      for (const selector of ["[data-hook='review-body'] span", "[data-hook='review-body']"]) {
        const node = reviewNode.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          reviewText = value;
          break;
        }
      }

      let reviewDate: string | null = null;
      for (const selector of ["[data-hook='review-date']"]) {
        const node = reviewNode.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          reviewDate = value;
          break;
        }
      }

      let helpfulVotes: string | null = null;
      for (const selector of ["[data-hook='helpful-vote-statement']", ".cr-vote-text"]) {
        const node = reviewNode.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          helpfulVotes = value;
          break;
        }
      }

      let verifiedPurchase = false;
      for (const selector of ["[data-hook='avp-badge']", ".a-size-mini.a-color-state"]) {
        if (reviewNode.querySelector(selector)) {
          verifiedPurchase = true;
          break;
        }
      }

      reviews.push({
        reviewer_name: reviewerName,
        review_title: reviewTitle,
        review_text: reviewText,
        review_date: reviewDate,
        helpful_votes: helpfulVotes,
        verified_purchase: verifiedPurchase,
      });
    }

    return {
      review_summary: {
        review_count: reviewCount,
        rating,
        rating_breakdown: ratingBreakdown.length > 0 ? ratingBreakdown : ratingBreakdownFromText,
        top_positive_snippet: topPositiveSnippet,
        top_critical_snippet: topCriticalSnippet,
      },
      reviews,
      review_page_href:
        (document.querySelector("a[data-hook='see-all-reviews-link-foot']") as HTMLAnchorElement | null)?.href ??
        null,
    };
  }, { reviewItemsLimit: options?.reviewItemsLimit ?? 3 }));

  if (!shouldLoadReviewPage(primaryPayload)) {
    return {
      review_summary: primaryPayload.review_summary,
      reviews: primaryPayload.reviews,
    };
  }

  logExtraction(
    options?.logger,
    `Detail page review fields incomplete for ${options?.productLabel ?? productUrl}, loading review page fallback.`,
    "warn",
  );

  const asin = options?.asin ?? extractAmazonAsinFromUrl(productUrl);
  const reviewPageUrl =
    primaryPayload.review_page_href ??
    (asin ? `https://www.amazon.com/product-reviews/${asin}/?reviewerType=all_reviews` : null);

  if (!reviewPageUrl) {
    logExtraction(
      options?.logger,
      `No review page URL found for ${options?.productLabel ?? productUrl}; keeping detail page review data only.`,
      "warn",
    );
    return {
      review_summary: primaryPayload.review_summary,
      reviews: primaryPayload.reviews,
    };
  }

  await gotoWithRetry(page, reviewPageUrl, {
    timeoutMs: AMAZON_NAVIGATION_TIMEOUT_MS,
    retries: AMAZON_NAVIGATION_RETRIES,
  });

  logExtraction(
    options?.logger,
    `Loaded review page fallback for ${options?.productLabel ?? productUrl}.`,
  );

  const fallbackPayload = await runDetailEvaluation(() => page.evaluate(({ reviewItemsLimit }) => {
    const collapsedPageText = (document.body.innerText || "").replace(/\s+/g, " ").trim();
    const ratingBreakdownFromText: string[] = [];
    const ratingRegex = /([1-5])\s*star[s]?\s+(\d+%)/gi;
    const seenBreakdownFromText = new Set<string>();
    let ratingMatch: RegExpExecArray | null;

    while ((ratingMatch = ratingRegex.exec(collapsedPageText)) !== null) {
      const value = `${ratingMatch[1]} star: ${ratingMatch[2]}`;
      if (!seenBreakdownFromText.has(value)) {
        seenBreakdownFromText.add(value);
        ratingBreakdownFromText.push(value);
      }
    }

    const ratingBreakdown: string[] = [];
    for (const row of Array.from(
      document.querySelectorAll(
        "#histogramTable tr, [data-hook='histogram-table'] tr, .reviewNumericalSummaryTable tr",
      ),
    )) {
      let label: string | null = null;
      for (const selector of ["td:first-child a", "td:first-child span", "a.a-link-normal"]) {
        const node = row.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          label = value;
          break;
        }
      }

      let detailValue: string | null = null;
      for (const selector of ["td:last-child a", "td:last-child span", ".a-text-right.a-nowrap"]) {
        const node = row.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          detailValue = value;
          break;
        }
      }

      if (label && detailValue) {
        ratingBreakdown.push(`${label}: ${detailValue}`.replace(/\s+/g, " ").trim());
      }
    }

    let reviewCount: string | null = null;
    for (const selector of [
      "#acrCustomerReviewText",
      "[data-hook='total-review-count']",
      "#filter-info-section .a-size-base",
    ]) {
      const node = document.querySelector(selector) as HTMLElement | null;
      const value = node?.innerText?.replace(/\s+/g, " ").trim();
      if (value) {
        reviewCount = value;
        break;
      }
    }

    let rating: string | null = null;
    for (const selector of [
      "#acrPopover",
      "[data-hook='rating-out-of-text']",
      ".reviewCountTextLinkedHistogram .a-icon-alt",
    ]) {
      const node = document.querySelector(selector) as HTMLElement | null;
      const value = node?.innerText?.replace(/\s+/g, " ").trim();
      if (value) {
        rating = value;
        break;
      }
    }

    let topPositiveSnippet: string | null = null;
    for (const selector of [
      "[data-hook='positive-review-snippet']",
      "#cr-summarization-attributes .a-list-item:first-child span",
      "[data-hook='cr-insights-widget-aspects'] .a-size-base",
    ]) {
      const node = document.querySelector(selector) as HTMLElement | null;
      const value = node?.innerText?.replace(/\s+/g, " ").trim();
      if (value) {
        topPositiveSnippet = value;
        break;
      }
    }

    if (!topPositiveSnippet) {
      for (const heading of ["Positive reviews", "Customers say"]) {
        const pattern = new RegExp(
          `${heading}\\s+(.{30,320}?)(?=\\b(?:Positive reviews|Critical reviews|Customers say|Top reviews|See more|Report|Sort by|Filter by|All stars)\\b|$)`,
          "i",
        );
        const match = collapsedPageText.match(pattern);
        const snippet = match?.[1]?.trim();
        if (snippet) {
          topPositiveSnippet = snippet;
          break;
        }
      }
    }

    let topCriticalSnippet: string | null = null;
    for (const selector of [
      "[data-hook='negative-review-snippet']",
      "#cr-summarization-attributes .a-section:last-child span",
      "#cr-summarization-attributes .a-list-item:last-child span",
    ]) {
      const node = document.querySelector(selector) as HTMLElement | null;
      const value = node?.innerText?.replace(/\s+/g, " ").trim();
      if (value) {
        topCriticalSnippet = value;
        break;
      }
    }

    if (!topCriticalSnippet) {
      for (const heading of ["Critical reviews", "Negative reviews"]) {
        const pattern = new RegExp(
          `${heading}\\s+(.{30,320}?)(?=\\b(?:Positive reviews|Critical reviews|Customers say|Top reviews|See more|Report|Sort by|Filter by|All stars)\\b|$)`,
          "i",
        );
        const match = collapsedPageText.match(pattern);
        const snippet = match?.[1]?.trim();
        if (snippet) {
          topCriticalSnippet = snippet;
          break;
        }
      }
    }

    const reviews: Array<Record<string, unknown>> = [];
    for (const reviewNode of Array.from(
      document.querySelectorAll(
        "#cm_cr-review_list [data-hook='review'], #cm-cr-dp-review-list [data-hook='review'], [data-hook='review']",
      ),
    ).slice(0, reviewItemsLimit)) {
      let reviewerName: string | null = null;
      for (const selector of [".a-profile-name", "[data-hook='genome-widget'] span"]) {
        const node = reviewNode.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          reviewerName = value;
          break;
        }
      }

      let reviewTitle: string | null = null;
      for (const selector of [
        "[data-hook='review-title'] span:not(.a-letter-space)",
        "[data-hook='review-title']",
      ]) {
        const node = reviewNode.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          reviewTitle = value;
          break;
        }
      }

      let reviewText: string | null = null;
      for (const selector of ["[data-hook='review-body'] span", "[data-hook='review-body']"]) {
        const node = reviewNode.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          reviewText = value;
          break;
        }
      }

      let reviewDate: string | null = null;
      for (const selector of ["[data-hook='review-date']"]) {
        const node = reviewNode.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          reviewDate = value;
          break;
        }
      }

      let helpfulVotes: string | null = null;
      for (const selector of ["[data-hook='helpful-vote-statement']", ".cr-vote-text"]) {
        const node = reviewNode.querySelector(selector) as HTMLElement | null;
        const value = node?.innerText?.replace(/\s+/g, " ").trim();
        if (value) {
          helpfulVotes = value;
          break;
        }
      }

      let verifiedPurchase = false;
      for (const selector of ["[data-hook='avp-badge']", ".a-size-mini.a-color-state"]) {
        if (reviewNode.querySelector(selector)) {
          verifiedPurchase = true;
          break;
        }
      }

      reviews.push({
        reviewer_name: reviewerName,
        review_title: reviewTitle,
        review_text: reviewText,
        review_date: reviewDate,
        helpful_votes: helpfulVotes,
        verified_purchase: verifiedPurchase,
      });
    }

    return {
      review_summary: {
        review_count: reviewCount,
        rating,
        rating_breakdown: ratingBreakdown.length > 0 ? ratingBreakdown : ratingBreakdownFromText,
        top_positive_snippet: topPositiveSnippet,
        top_critical_snippet: topCriticalSnippet,
      },
      reviews,
    };
  }, { reviewItemsLimit: options?.reviewItemsLimit ?? 3 }));

  return mergeDetailPayloads(
    {
      review_summary: primaryPayload.review_summary,
      reviews: primaryPayload.reviews,
    },
    fallbackPayload,
  );
}

async function enrichAmazonRecordWithDetails(
  page: import("playwright").Page,
  plan: ExtractionPlan,
  record: Record<string, unknown>,
  productUrl: string,
  options?: {
    reviewsPerItem?: number;
    logger?: ExtractionLogger;
    productLabel?: string;
  },
): Promise<Record<string, unknown>> {
  const detailPayload = await extractAmazonDetailPayload(page, productUrl, {
    reviewItemsLimit: options?.reviewsPerItem ?? 3,
    asin: typeof record.asin === "string" ? record.asin : null,
    logger: options?.logger,
    productLabel: options?.productLabel,
  });

  const reviewSummaryField = fieldByName(plan.fields, "review_summary");
  const reviewsField = fieldByName(plan.fields, "reviews");

  return {
    ...record,
    ...(reviewSummaryField
      ? { review_summary: projectValueForField(reviewSummaryField, detailPayload.review_summary) }
      : {}),
    ...(reviewsField ? { reviews: projectValueForField(reviewsField, detailPayload.reviews) } : {}),
  };
}

async function extractTargetDetailPayload(
  page: import("playwright").Page,
  productUrl: string,
  options?: {
    reviewsPerItem?: number;
    logger?: ExtractionLogger;
    productLabel?: string;
  },
): Promise<{
  title: string | null;
  brand: string | null;
  price: string | null;
  original_price: string | null;
  description: string | null;
  highlights: string[];
  features: string[];
  specifications: string[];
  at_a_glance: string[];
  sold_by: string | null;
  shipping: string | null;
  review_summary: Record<string, unknown>;
  reviews: Array<Record<string, unknown>>;
}> {
  await gotoWithRetry(page, productUrl, {
    timeoutMs: DEFAULT_NAVIGATION_TIMEOUT_MS,
  });
  await page.waitForTimeout(2_000);

  const snapshot = await page.evaluate(() => {
    const mainText = (
      (document.querySelector("#pageBodyContainer") as HTMLElement | null)?.innerText ||
      document.body.innerText ||
      ""
    )
      .replace(/\s+/g, " ")
      .trim();

    const titleElements = Array.from(document.querySelectorAll('[data-test="product-title"]'));
    const titleText = titleElements
      .map((element) => (element.textContent || "").replace(/\s+/g, " ").trim())
      .find((value) => value.length > 0) || null;

    const ratingText = (
      (document.querySelector('[data-test="ratings"]') as HTMLElement | null)?.innerText ||
      (document.querySelector('[data-test="ratingFeedbackContainer"]') as HTMLElement | null)?.innerText ||
      ""
    )
      .replace(/\s+/g, " ")
      .trim();

    const reviewCountText = (
      (document.querySelector('[data-test="ratingCountLink"]') as HTMLElement | null)?.innerText ||
      ""
    )
      .replace(/\s+/g, " ")
      .trim();

    const priceText = (
      (document.querySelector('[data-test="product-price"]') as HTMLElement | null)?.innerText ||
      ""
    )
      .replace(/\s+/g, " ")
      .trim();

    const regularPriceText = (
      (document.querySelector('[data-test="product-regular-price"]') as HTMLElement | null)?.innerText ||
      ""
    )
      .replace(/\s+/g, " ")
      .trim();

    const detailText = (
      (document.querySelector('[data-test="item-details-description"]') as HTMLElement | null)?.innerText ||
      ""
    )
      .replace(/\s+/g, " ")
      .trim();

    return {
      mainText,
      titleText,
      ratingText: ratingText || null,
      reviewCountText: reviewCountText || null,
      priceText: priceText || null,
      regularPriceText: regularPriceText || null,
      detailText: detailText || null,
    };
  });

  const normalizedPageText = normalizeTargetText(
    snapshot.detailText ? `${snapshot.mainText} ${snapshot.detailText}` : snapshot.mainText,
  ) ?? "";
  const detailSection = extractTargetSection(normalizedPageText, "Details", [
    "Highlights",
    "Description",
    "Features",
    "Specifications",
    "Shipping & Returns",
    "Q&A",
    "Additional product information",
    "Load all content at once",
    "Guest ratings & reviews",
  ]);
  const highlights = splitTargetSectionToItems(
    extractTargetSection(normalizedPageText, "Highlights", [
      "Description",
      "Features",
      "Specifications",
      "Shipping & Returns",
      "Q&A",
      "Additional product information",
      "Load all content at once",
      "Guest ratings & reviews",
    ]),
  );
  const description =
    extractTargetSection(normalizedPageText, "Description", [
      "Features",
      "Specifications",
      "Shipping & Returns",
      "Q&A",
      "Additional product information",
      "Load all content at once",
      "Guest ratings & reviews",
    ]) ?? detailSection;
  const features = splitTargetSectionToItems(
    extractTargetSection(normalizedPageText, "Features", [
      "Specifications",
      "Shipping & Returns",
      "Q&A",
      "Additional product information",
      "Load all content at once",
      "Guest ratings & reviews",
    ]),
  );
  const specifications = splitTargetSectionToItems(
    extractTargetSection(normalizedPageText, "Specifications", [
      "Shipping & Returns",
      "Q&A",
      "Additional product information",
      "Load all content at once",
      "Guest ratings & reviews",
      "Disclaimer",
    ]),
  );
  const atAGlance = splitTargetSectionToItems(
    extractTargetSection(normalizedPageText, "At a glance", [
      "About this item",
      "Details",
      "Highlights",
      "Description",
      "Features",
      "Specifications",
    ]),
  );
  const reviewSummary = extractTargetReviewSummary({
    ratingText: snapshot.ratingText,
    reviewCountText: snapshot.reviewCountText,
    mainText: normalizedPageText,
  });
  const soldBy =
    normalizeTargetText(
      normalizedPageText.match(/Sold\s*&\s*shipped by\s+(.+?)(?=\s+Report this item|\s+Eligible for registries|\s+At a glance|\s+About this item)/i)?.[1] ??
        null,
    ) ?? null;
  const shipping =
    normalizeTargetText(
      normalizedPageText.match(/(Choose delivery method in cart|Same Day Delivery|Ship it|Pickup|Shipping not available|There was a temporary issue.+?)(?=\s+Qty|\s+Add to cart|\s+Sold\s*&\s*shipped by|\s+Report this item)/i)?.[1] ??
        null,
    ) ?? null;
  const brand =
    normalizeTargetText(
      normalizedPageText.match(/Shop all\s+(.+?)(?=\s+.+?out of 5 stars|\s+\$|\s+There was a temporary issue)/i)?.[1] ??
        null,
    ) ?? null;

  logExtraction(
    options?.logger,
    `Loaded Target detail page for ${options?.productLabel ?? productUrl}. Highlights=${highlights.length}, features=${features.length}, specs=${specifications.length}, reviews=${reviewSummary.review_count ?? "0"}.`,
  );

  return {
    title: normalizeTargetText(snapshot.titleText),
    brand,
    price: normalizeTargetText(snapshot.priceText),
    original_price: normalizeTargetText(snapshot.regularPriceText),
    description: normalizeTargetText(description),
    highlights,
    features,
    specifications,
    at_a_glance: atAGlance,
    sold_by: soldBy,
    shipping,
    review_summary: {
      review_count: reviewSummary.review_count,
      rating: reviewSummary.rating,
      rating_breakdown: [],
      top_positive_snippet: null,
      top_critical_snippet: null,
      question_count: reviewSummary.question_count,
    },
    reviews: [],
  };
}

function projectTargetDetailValueForField(
  field: ExtractionPlan["fields"][number],
  rawValue: unknown,
): unknown {
  if (field.type === "object" || field.type === "object[]") {
    return projectValueForField(field, rawValue);
  }

  if (field.type === "string[]") {
    if (Array.isArray(rawValue)) {
      return rawValue.map((value) => String(value)).filter((value) => value.trim().length > 0);
    }

    return typeof rawValue === "string" && rawValue.trim().length > 0 ? [rawValue.trim()] : [];
  }

  if (field.type === "number[]") {
    if (!Array.isArray(rawValue)) {
      return [];
    }

    return rawValue
      .map((value) => (typeof value === "number" ? value : parseNumber(String(value ?? ""))))
      .filter((value): value is number => value !== null);
  }

  if (field.type === "number") {
    if (typeof rawValue === "number") {
      return rawValue;
    }

    return typeof rawValue === "string" ? parseNumber(rawValue) : null;
  }

  if (field.type === "boolean") {
    if (typeof rawValue === "boolean") {
      return rawValue;
    }

    if (typeof rawValue === "string") {
      return ["true", "yes", "1"].includes(rawValue.toLowerCase());
    }

    return Boolean(rawValue);
  }

  if (Array.isArray(rawValue)) {
    return rawValue.map((value) => String(value)).join(" ");
  }

  return rawValue === null || rawValue === undefined || rawValue === "" ? null : String(rawValue);
}

function getTargetDetailRawValue(
  field: ExtractionPlan["fields"][number],
  detailPayload: Awaited<ReturnType<typeof extractTargetDetailPayload>>,
): unknown {
  const lowerName = field.name.toLowerCase();

  if (lowerName === "title" || lowerName.includes("title")) {
    return detailPayload.title;
  }

  if (lowerName === "brand") {
    return detailPayload.brand;
  }

  if (lowerName === "price") {
    return detailPayload.price;
  }

  if (lowerName === "original_price" || lowerName.includes("regular_price")) {
    return detailPayload.original_price;
  }

  if (lowerName === "description" || lowerName === "desc") {
    return detailPayload.description;
  }

  if (lowerName === "highlights") {
    return detailPayload.highlights;
  }

  if (lowerName === "features") {
    return detailPayload.features;
  }

  if (lowerName === "specifications" || lowerName === "specs") {
    return detailPayload.specifications;
  }

  if (lowerName === "details" || lowerName === "about_this_item") {
    return detailPayload.description;
  }

  if (lowerName === "at_a_glance") {
    return detailPayload.at_a_glance;
  }

  if (lowerName === "shipping" || lowerName === "shipping&returns" || lowerName === "shipping_and_returns") {
    return detailPayload.shipping;
  }

  if (lowerName === "sold_by") {
    return detailPayload.sold_by;
  }

  if (lowerName === "review_rate" || lowerName === "rating") {
    return detailPayload.review_summary.rating;
  }

  if (lowerName === "review_detail" || lowerName === "review_count") {
    return detailPayload.review_summary.review_count;
  }

  if (lowerName === "question_count" || lowerName === "questions") {
    return detailPayload.review_summary.question_count;
  }

  if (lowerName === "review_summary") {
    return detailPayload.review_summary;
  }

  if (lowerName === "reviews") {
    return detailPayload.reviews;
  }

  return null;
}

function enrichTargetRecordWithDetails(
  plan: ExtractionPlan,
  record: Record<string, unknown>,
  detailPayload: Awaited<ReturnType<typeof extractTargetDetailPayload>>,
): Record<string, unknown> {
  const enrichedRecord: Record<string, unknown> = { ...record };

  for (const field of plan.fields) {
    const rawValue = getTargetDetailRawValue(field, detailPayload);

    if (
      rawValue === null ||
      rawValue === undefined ||
      rawValue === "" ||
      (Array.isArray(rawValue) && rawValue.length === 0)
    ) {
      continue;
    }

    enrichedRecord[field.name] = projectTargetDetailValueForField(field, rawValue);
  }

  return enrichedRecord;
}

function looksLikeAmazonNoise(input: {
  title: string | null;
  reviewDetail: string | null;
}): boolean {
  const title = String(input.title ?? "").trim().toLowerCase();
  const reviewDetail = String(input.reviewDetail ?? "").trim().toLowerCase();

  if (!title) {
    return true;
  }

  if (
    title === "leave ad feedback" ||
    title === "customers often bought together" ||
    title === "related searches" ||
    title === "need help?"
  ) {
    return true;
  }

  if (title.length < 6) {
    return true;
  }

  if (reviewDetail === "sponsored" || reviewDetail === "learn more") {
    return true;
  }

  return false;
}

function normalizeAmazonDescription(value: string | null): string | null {
  if (!value) {
    return null;
  }

  const lowered = value.toLowerCase();

  if (
    lowered === "sponsored" ||
    lowered === "featured from amazon brands" ||
    lowered === "overall pick"
  ) {
    return null;
  }

  return value;
}

function normalizeSearchText(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9\s]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokenizeSearchText(value: string): string[] {
  return normalizeSearchText(value)
    .split(" ")
    .filter((token) => token.length >= 2);
}

function getAmazonSearchQuery(rawUrl: string): string {
  try {
    const url = new URL(rawUrl);
    return url.searchParams.get("k")?.trim() ?? "";
  } catch {
    return "";
  }
}

function isRelevantAmazonSheetRecord(
  record: { title: string | null; description: string | null },
  rawUrl: string,
): boolean {
  const title = normalizeSearchText(record.title ?? "");
  const description = normalizeSearchText(record.description ?? "");
  const haystack = `${title} ${description}`.trim();

  if (!haystack) {
    return false;
  }

  const query = getAmazonSearchQuery(rawUrl);
  const queryTokens = tokenizeSearchText(query);
  const wantsSheets = queryTokens.some((token) => token === "sheet" || token === "sheets");

  const positiveTokens = ["sheet", "sheets", "sheet set", "bed sheet", "bed sheets"];
  const negativeTokens = [
    "comforter",
    "duvet",
    "quilt",
    "blanket",
    "coverlet",
    "mattress topper",
    "mattress pad",
    "bed in a bag",
    "comforter set",
  ];

  const hasPositiveSignal = positiveTokens.some((token) => haystack.includes(token));
  const hasNegativeSignal = negativeTokens.some((token) => haystack.includes(token));

  if (wantsSheets && !hasPositiveSignal) {
    return false;
  }

  if (hasNegativeSignal) {
    return false;
  }

  return true;
}

export async function runAmazonSearchExtraction(
  url: string,
  plan: ExtractionPlan,
  browserProfile?: BrowserProfile,
  options?: {
    maxItems?: number;
    reviewsPerItem?: number;
    logger?: ExtractionLogger;
  },
): Promise<Array<Record<string, unknown>>> {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage(browserProfileToContextOptions(browserProfile));

  try {
    const seen = new Set<string>();
    const collected: Array<Record<string, unknown>> = [];
    const maxItems = options?.maxItems;
    let nextUrl: string | null = url;
    let pageCount = 0;

    logExtraction(
      options?.logger,
      `Starting Amazon search extraction${maxItems ? ` with maxItems=${maxItems}` : " for one page"}${options?.reviewsPerItem ? ` and reviewsPerItem=${options.reviewsPerItem}` : ""}.`,
    );

    while (nextUrl && pageCount < 10) {
      logExtraction(options?.logger, `Loading Amazon search page ${pageCount + 1}: ${nextUrl}`);

      await gotoWithRetry(page, nextUrl, {
        timeoutMs: AMAZON_NAVIGATION_TIMEOUT_MS,
        retries: AMAZON_NAVIGATION_RETRIES,
      });

      const pageRecords = await extractListRecordsFromPage(page, plan, {
        logger: options?.logger,
      });
      logExtraction(
        options?.logger,
        `Search page ${pageCount + 1}: found ${pageRecords.length} raw result cards.`,
      );
      const nextHref =
        (await page
          .locator('a.s-pagination-next[href]:not(.s-pagination-disabled)')
          .first()
          .getAttribute("href")
          .catch(() => null)) ?? null;

      const cleanedRecords = pageRecords
        .map((record) => {
          const title = getPlanRoleValue(record, plan, "title");
          const description = normalizeAmazonDescription(getPlanRoleValue(record, plan, "description"));
          const reviewDetail = getPlanRoleValue(record, plan, "review_detail");
          const asin = getPlanRoleValue(record, plan, "asin");

          return {
            record,
            asin,
            title,
            description,
            reviewDetail,
          };
        })
        .filter((entry) =>
          !looksLikeAmazonNoise({
            title: entry.title,
            reviewDetail: entry.reviewDetail,
          }),
        )
        .filter((entry) =>
          isRelevantAmazonSheetRecord(
            {
              title: entry.title,
              description: entry.description,
            },
            url,
          ),
        )
        .filter((entry) => {
          const key = [entry.asin ?? "", entry.title ?? "", buildGenericDedupeKey(entry.record, [])]
            .filter(Boolean)
            .join("::");

          if (!key) {
            return true;
          }

          if (seen.has(key)) {
            return false;
          }

          seen.add(key);
          return true;
        })
        .map((entry) => entry.record);

      logExtraction(
        options?.logger,
        `Search page ${pageCount + 1}: kept ${cleanedRecords.length} product cards after filtering.`,
      );

      collected.push(...cleanedRecords);
      pageCount += 1;

      if (maxItems !== undefined && collected.length >= maxItems) {
        logExtraction(options?.logger, `Reached maxItems=${maxItems}; stopping pagination.`);
        break;
      }

      if (maxItems === undefined) {
        logExtraction(options?.logger, "maxItems not set, stopping after the current search page.");
        break;
      }

      nextUrl = nextHref ? new URL(nextHref, page.url()).toString() : null;

      if (!nextUrl) {
        logExtraction(options?.logger, "No next search results page found; pagination finished.");
      }
    }

    const selectedRecords = maxItems !== undefined ? collected.slice(0, maxItems) : collected;

    logExtraction(
      options?.logger,
      `Collected ${selectedRecords.length} product records after pagination and deduplication.`,
    );

    if (!hasAmazonDetailFields(plan)) {
      return selectedRecords;
    }

    logExtraction(
      options?.logger,
      `Enriching ${selectedRecords.length} products with detail-page reviews.`,
    );

    const enrichedRecords: Array<Record<string, unknown>> = [];

    for (const [index, record] of selectedRecords.entries()) {
      const productUrl = buildAmazonProductUrl(record, plan);
      const productLabel =
        getPlanRoleValue(record, plan, "title") ??
        getPlanRoleValue(record, plan, "asin") ??
        `item ${index + 1}`;

      if (!productUrl) {
        logExtraction(
          options?.logger,
          `Skipping detail enrichment for ${productLabel}; no product URL was found.`,
          "warn",
        );
        enrichedRecords.push(record);
        continue;
      }

      try {
        logExtraction(
          options?.logger,
          `Opening detail page ${index + 1}/${selectedRecords.length}: ${productLabel}`,
        );

        const enrichedRecord = await enrichAmazonRecordWithDetails(
          page,
          plan,
          {
            ...record,
            product_url: productUrl,
          },
          productUrl,
          {
            reviewsPerItem: options?.reviewsPerItem,
            logger: options?.logger,
            productLabel,
          },
        );

        const reviewSummary =
          enrichedRecord.review_summary &&
          typeof enrichedRecord.review_summary === "object" &&
          !Array.isArray(enrichedRecord.review_summary)
            ? (enrichedRecord.review_summary as Record<string, unknown>)
            : null;
        const reviewCount = Array.isArray(enrichedRecord.reviews) ? enrichedRecord.reviews.length : 0;
        const breakdownCount = Array.isArray(reviewSummary?.rating_breakdown)
          ? reviewSummary.rating_breakdown.length
          : 0;

        logExtraction(
          options?.logger,
          `Enriched ${productLabel}: ${reviewCount} reviews, ${breakdownCount} rating breakdown rows.`,
        );
        enrichedRecords.push(enrichedRecord);
      } catch {
        logExtraction(
          options?.logger,
          `Failed to enrich ${productLabel}; returning the search-page fields only.`,
          "warn",
        );
        enrichedRecords.push({
          ...record,
          product_url: productUrl,
        });
      }
    }

    logExtraction(options?.logger, `Amazon search extraction completed with ${enrichedRecords.length} records.`);

    return enrichedRecords;
  } finally {
    await browser.close();
  }
}

export async function runTargetSearchExtraction(
  url: string,
  plan: ExtractionPlan,
  browserProfile?: BrowserProfile,
  options?: {
    maxItems?: number;
    reviewsPerItem?: number;
    logger?: ExtractionLogger;
  },
): Promise<Array<Record<string, unknown>>> {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage(browserProfileToContextOptions(browserProfile));

  try {
    logExtraction(
      options?.logger,
      `Starting Target search extraction${options?.maxItems ? ` with maxItems=${options.maxItems}` : " for one page"}${options?.reviewsPerItem ? ` and reviewsPerItem=${options.reviewsPerItem}` : ""}.`,
    );

    await gotoWithRetry(page, url, {
      timeoutMs: DEFAULT_NAVIGATION_TIMEOUT_MS,
    });
    await ensureTargetSearchResultsReady(page, options?.logger);

    const pageRecords = await extractListRecordsFromPage(page, plan, {
      logger: options?.logger,
    });
    const selectedRecords =
      options?.maxItems !== undefined ? pageRecords.slice(0, options.maxItems) : pageRecords;

    logExtraction(
      options?.logger,
      `Target search page yielded ${pageRecords.length} records; selected ${selectedRecords.length} for enrichment/output.`,
    );

    if (!hasTargetDetailFields(plan, { reviewsPerItem: options?.reviewsPerItem })) {
      return stripInternalExtractionFieldsFromList(selectedRecords);
    }

    logExtraction(
      options?.logger,
      `Enriching ${selectedRecords.length} Target products with detail-page data.`,
    );

    const enrichedRecords: Array<Record<string, unknown>> = [];

    for (const [index, record] of selectedRecords.entries()) {
      const productUrl = buildTargetProductUrl(record, plan);
      const productLabel =
        normalizeTargetText(typeof record.title === "string" ? record.title : null) ?? `item ${index + 1}`;

      if (!productUrl) {
        logExtraction(
          options?.logger,
          `Skipping Target detail enrichment for ${productLabel}; no product URL was found.`,
          "warn",
        );
        enrichedRecords.push(stripInternalExtractionFields(record));
        continue;
      }

      try {
        logExtraction(
          options?.logger,
          `Opening Target detail page ${index + 1}/${selectedRecords.length}: ${productLabel}`,
        );
        const detailPayload = await extractTargetDetailPayload(page, productUrl, {
          reviewsPerItem: options?.reviewsPerItem,
          logger: options?.logger,
          productLabel,
        });
        const enrichedRecord = enrichTargetRecordWithDetails(plan, record, detailPayload);
        enrichedRecords.push(stripInternalExtractionFields(enrichedRecord));
      } catch (error) {
        logExtraction(
          options?.logger,
          `Failed to enrich Target detail page for ${productLabel}; returning search-page fields only.`,
          "warn",
        );
        if (error instanceof Error) {
          logExtraction(options?.logger, error.message, "warn");
        }
        enrichedRecords.push(stripInternalExtractionFields(record));
      }
    }

    logExtraction(
      options?.logger,
      `Target search extraction completed with ${enrichedRecords.length} records.`,
    );

    return enrichedRecords;
  } finally {
    await browser.close();
  }
}

export async function runExtraction(
  url: string,
  plan: ExtractionPlan,
  browserProfile?: BrowserProfile,
  options?: {
    logger?: ExtractionLogger;
  },
): Promise<Record<string, unknown> | Array<Record<string, unknown>>> {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage(browserProfileToContextOptions(browserProfile));

  try {
    await gotoWithRetry(page, url, {
      timeoutMs: isAmazonSearchUrl(url) ? AMAZON_NAVIGATION_TIMEOUT_MS : DEFAULT_NAVIGATION_TIMEOUT_MS,
      retries: isAmazonSearchUrl(url) ? AMAZON_NAVIGATION_RETRIES : 0,
    });

    if (isTargetSearchUrl(url)) {
      logExtraction(options?.logger, "Waiting for Target search results to render before extraction.");
      await ensureTargetSearchResultsReady(page, options?.logger);
    }

    if (plan.extractionMode === "list") {
      return stripInternalExtractionFieldsFromList(
        await extractListRecordsFromPage(page, plan, {
          logger: options?.logger,
        }),
      );
    }

    const result: Record<string, unknown> = {};

    for (const field of plan.fields) {
      const values = await extractFieldValues(page, field);
      const normalizedValues = values
        .map((value) => normalizeExtractedValue(value, field.transform, page.url()))
        .filter((value) => value !== "");

      result[field.name] = finalizeFieldValue(field, normalizedValues);
    }

    return stripInternalExtractionFields(result);
  } finally {
    await browser.close();
  }
}
