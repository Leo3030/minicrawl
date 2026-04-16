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
  const normalized = raw.replace(/[^0-9.,-]/g, "").replace(/,/g, "");

  if (!normalized) {
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

async function extractRecordFromContainer(
  page: import("playwright").Page,
  plan: ExtractionPlan,
  container: import("playwright").ElementHandle<Node>,
): Promise<Record<string, unknown>> {
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
): Array<Record<string, unknown>> {
  const requiredFieldNames = plan.fields.filter((field) => field.required).map((field) => field.name);
  const seenKeys = new Set<string>();

  return records.filter((item) => {
    const hasMeaningfulValue = Object.values(item).some((value) => {
      if (Array.isArray(value)) {
        return value.length > 0;
      }
      return value !== null && value !== "";
    });

    if (!hasMeaningfulValue) {
      return false;
    }

    const hasAllRequiredFields = requiredFieldNames.every((fieldName) => isPresentValue(item[fieldName]));

    if (!hasAllRequiredFields) {
      return false;
    }

    const dedupeKey = buildGenericDedupeKey(item, requiredFieldNames);

    if (dedupeKey && seenKeys.has(dedupeKey)) {
      return false;
    }

    if (dedupeKey) {
      seenKeys.add(dedupeKey);
    }

    return true;
  });
}

async function extractListRecordsFromPage(
  page: import("playwright").Page,
  plan: ExtractionPlan,
): Promise<Array<Record<string, unknown>>> {
  const containers = await findContainersForPlan(page, plan);

  if (containers.length === 0) {
    throw new Error("List extraction was requested, but no item containers were found.");
  }

  const rawRecords: Array<Record<string, unknown>> = [];

  for (const container of containers) {
    rawRecords.push(await extractRecordFromContainer(page, plan, container));
  }

  return filterExtractedListRecords(rawRecords, plan);
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

export async function capturePage(url: string, browserProfile?: BrowserProfile): Promise<PageSnapshot> {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage(browserProfileToContextOptions(browserProfile));

  try {
    await gotoWithRetry(page, url, {
      timeoutMs: isAmazonSearchUrl(url) ? AMAZON_NAVIGATION_TIMEOUT_MS : DEFAULT_NAVIGATION_TIMEOUT_MS,
      retries: isAmazonSearchUrl(url) ? AMAZON_NAVIGATION_RETRIES : 0,
    });

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

      const pageRecords = await extractListRecordsFromPage(page, plan);
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

export async function runExtraction(
  url: string,
  plan: ExtractionPlan,
  browserProfile?: BrowserProfile,
): Promise<Record<string, unknown> | Array<Record<string, unknown>>> {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage(browserProfileToContextOptions(browserProfile));

  try {
    await gotoWithRetry(page, url, {
      timeoutMs: isAmazonSearchUrl(url) ? AMAZON_NAVIGATION_TIMEOUT_MS : DEFAULT_NAVIGATION_TIMEOUT_MS,
      retries: isAmazonSearchUrl(url) ? AMAZON_NAVIGATION_RETRIES : 0,
    });

    if (plan.extractionMode === "list") {
      return extractListRecordsFromPage(page, plan);
    }

    const result: Record<string, unknown> = {};

    for (const field of plan.fields) {
      const values = await extractFieldValues(page, field);
      const normalizedValues = values
        .map((value) => normalizeExtractedValue(value, field.transform, page.url()))
        .filter((value) => value !== "");

      result[field.name] = finalizeFieldValue(field, normalizedValues);
    }

    return result;
  } finally {
    await browser.close();
  }
}
