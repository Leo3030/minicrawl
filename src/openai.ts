import type { ExtractionPlan, PageSnapshot } from "./types.js";

const PLAN_SCHEMA = {
  name: "extraction_plan",
  strict: true,
  schema: {
    type: "object",
    additionalProperties: false,
    $defs: {
      field: {
        type: "object",
        additionalProperties: false,
        properties: {
          name: { type: "string" },
          description: { type: "string" },
          type: {
            type: "string",
            enum: [
              "string",
              "number",
              "boolean",
              "url",
              "image",
              "object",
              "object[]",
              "string[]",
              "number[]",
            ],
          },
          required: { type: "boolean" },
          multiple: { type: "boolean" },
          selector: { type: "string" },
          fallbackSelectors: {
            type: "array",
            items: { type: "string" },
          },
          source: { type: "string", enum: ["text", "attribute"] },
          attribute: {
            anyOf: [{ type: "string" }, { type: "null" }],
          },
          transform: { type: "string", enum: ["none", "trim", "number", "url"] },
          properties: {
            anyOf: [
              {
                type: "array",
                items: { $ref: "#/$defs/field" },
              },
              { type: "null" },
            ],
          },
        },
        required: [
          "name",
          "description",
          "type",
          "required",
          "multiple",
          "selector",
          "fallbackSelectors",
          "source",
          "attribute",
          "transform",
          "properties",
        ],
      },
    },
    properties: {
      reasoning: { type: "string" },
      pageType: { type: "string" },
      extractionMode: {
        type: "string",
        enum: ["single", "list"],
      },
      itemContainerSelector: {
        anyOf: [{ type: "string" }, { type: "null" }],
      },
      itemContainerFallbackSelectors: {
        type: "array",
        items: { type: "string" },
      },
      fields: {
        type: "array",
        minItems: 1,
        maxItems: 16,
        items: { $ref: "#/$defs/field" },
      },
    },
    required: [
      "reasoning",
      "pageType",
      "extractionMode",
      "itemContainerSelector",
      "itemContainerFallbackSelectors",
      "fields",
    ],
  },
} as const;

export interface MiniMaxClientConfig {
  apiKey: string;
  baseURL: string;
  model: string;
}

export function createOpenAIClient(): MiniMaxClientConfig {
  const apiKey =
    process.env.MINIMAX_API_KEY ?? process.env.MINIMAX_API_KEY ?? process.env.OPENAI_API_KEY;
  const baseURL = process.env.MINIMAX_BASE_URL ?? "https://api.minimaxi.com";
  const model = process.env.MINIMAX_MODEL ?? "MiniMax-M2.7";

  if (!apiKey) {
    throw new Error(
      "Missing MINIMAX_API_KEY. Add it to your environment before starting the server.",
    );
  }

  return {
    apiKey,
    baseURL,
    model,
  };
}

function extractJsonObject(raw: string): string {
  const withoutThinking = raw.replace(/<think>[\s\S]*?<\/think>/g, "").trim();
  const fencedMatch = withoutThinking.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = fencedMatch?.[1]?.trim() ?? withoutThinking;

  let depth = 0;
  let start = -1;

  for (let index = 0; index < candidate.length; index += 1) {
    const char = candidate[index];

    if (char === "{") {
      if (depth === 0) {
        start = index;
      }
      depth += 1;
    } else if (char === "}") {
      depth -= 1;

      if (depth === 0 && start >= 0) {
        return candidate.slice(start, index + 1);
      }
    }
  }

  return candidate;
}

function normalizePlan(parsed: ExtractionPlan): ExtractionPlan {
  function normalizeField(field: ExtractionPlan["fields"][number]): ExtractionPlan["fields"][number] {
    return {
      name: field.name,
      description: field.description,
      type: field.type,
      required: field.required,
      multiple: field.multiple,
      selector: field.selector,
      fallbackSelectors: Array.isArray(field.fallbackSelectors) ? field.fallbackSelectors : [],
      source: field.source,
      attribute: field.attribute ?? undefined,
      transform: field.transform,
      properties: Array.isArray(field.properties) ? field.properties.map(normalizeField) : undefined,
    };
  }

  return {
    reasoning: parsed.reasoning,
    pageType: parsed.pageType,
    extractionMode: parsed.extractionMode ?? "single",
    itemContainerSelector: parsed.itemContainerSelector ?? undefined,
    itemContainerFallbackSelectors: Array.isArray(parsed.itemContainerFallbackSelectors)
      ? parsed.itemContainerFallbackSelectors
      : [],
    fields: parsed.fields.map(normalizeField),
  };
}

function isAmazonSearchSnapshot(snapshot: PageSnapshot): boolean {
  try {
    const url = new URL(snapshot.url);
    return url.hostname.includes("amazon.") && url.pathname.startsWith("/s");
  } catch {
    return false;
  }
}

function isTargetSearchSnapshot(snapshot: PageSnapshot): boolean {
  try {
    const url = new URL(snapshot.url);
    return url.hostname.includes("target.") && url.pathname.startsWith("/s");
  } catch {
    return false;
  }
}

function sanitizePlanForKnownPageContexts(
  plan: ExtractionPlan,
  snapshot: PageSnapshot,
  options?: {
    preserveUserShape?: boolean;
  },
): ExtractionPlan {
  if (isTargetSearchSnapshot(snapshot)) {
    return {
      ...plan,
      pageType: "target_search_results",
      extractionMode: "list",
      itemContainerSelector: '[data-test="@web/ProductCard/ProductCardVariantDefault"]',
      itemContainerFallbackSelectors: [
        '[data-test="@web/ProductCard/body"]',
        '[data-test="product-details"]',
      ],
      fields: plan.fields.map((field) => {
        const lowerName = field.name.toLowerCase();
        const isTitleField = lowerName === "title" || lowerName.includes("title");
        const isUrlField =
          lowerName === "product_url" ||
          lowerName === "url" ||
          lowerName.includes("link") ||
          (field.attribute === "href" && field.selector.includes("title"));
        const isImageField =
          lowerName === "image" || lowerName.includes("image") || field.type === "image";
        const isPriceField = lowerName === "price" || lowerName.includes("price");
        const isColorField = lowerName === "color" || lowerName.includes("color");
        const isReviewRateField =
          lowerName === "review_rate" ||
          lowerName === "rating" ||
          (lowerName.includes("review") && lowerName.includes("rate"));
        const isReviewDetailField =
          lowerName === "review_detail" ||
          lowerName === "review_count" ||
          (lowerName.includes("review") &&
            (lowerName.includes("detail") || lowerName.includes("count")));
        const isDescriptionField =
          lowerName === "description" || lowerName === "desc" || lowerName.includes("description");
        const isTypeField = lowerName === "type" || lowerName.includes("category");
        const isBrandField = lowerName === "brand";

        if (isTitleField) {
          return {
            ...field,
            selector: 'a[data-test="@web/ProductCard/title"]',
            fallbackSelectors: ['[data-test="product-details"] a[href*="/p/"]'],
            source: "text",
            attribute: undefined,
            transform: "trim",
            multiple: false,
          };
        }

        if (isUrlField) {
          return {
            ...field,
            type: options?.preserveUserShape ? field.type : "url",
            selector: 'a[data-test="@web/ProductCard/title"]',
            fallbackSelectors: ['[data-test="product-details"] a[href*="/p/"]'],
            source: "attribute",
            attribute: "href",
            transform: "url",
            multiple: false,
          };
        }

        if (isImageField) {
          return {
            ...field,
            selector: '[data-test="@web/ProductCard/ProductCardImage/primary"] img',
            fallbackSelectors: ["img"],
            source: "attribute",
            attribute: "src",
            transform: "url",
            multiple: false,
          };
        }

        if (isPriceField) {
          return {
            ...field,
            selector: 'span[data-test="current-price"]',
            fallbackSelectors: [
              '[data-test="@web/Price/PriceStandard"]',
              '[data-test="product-details"]',
            ],
            source: "text",
            attribute: undefined,
            transform: field.type === "number" ? "number" : "trim",
            multiple: false,
          };
        }

        if (isColorField) {
          return {
            ...field,
            required: options?.preserveUserShape ? field.required : false,
            selector: 'span[data-test="@web/ProductCard/ProductCardSwatches"]',
            fallbackSelectors: ['[data-test="product-details"]'],
            source: "attribute",
            attribute: "aria-label",
            transform: "trim",
            multiple: false,
          };
        }

        if (isReviewRateField || isReviewDetailField || isDescriptionField || isTypeField) {
          return {
            ...field,
            required:
              options?.preserveUserShape || isReviewRateField || isReviewDetailField
                ? field.required
                : false,
            selector: '[data-test="product-details"]',
            fallbackSelectors: ['[data-test="@web/ProductCard/body"]'],
            source: "text",
            attribute: undefined,
            transform: "trim",
            multiple: false,
          };
        }

        if (isBrandField) {
          return {
            ...field,
            required: options?.preserveUserShape ? field.required : false,
            selector: '[data-test="@web/ProductCard/ProductCardBrandAndRibbonMessage/brand"]',
            fallbackSelectors: ['[data-test="product-details"]'],
            source: "text",
            attribute: undefined,
            transform: "trim",
            multiple: false,
          };
        }

        return field;
      }),
    };
  }

  if (!isAmazonSearchSnapshot(snapshot)) {
    return plan;
  }

  return {
    ...plan,
    fields: plan.fields.map((field) => {
      const lowerName = field.name.toLowerCase();
      const lowerSelector = field.selector.toLowerCase();
      const isReviewDetailField =
        lowerName === "review_detail" ||
        (lowerName.includes("review") && (lowerName.includes("detail") || lowerName.includes("count"))) ||
        lowerSelector.includes("customerreviews") ||
        lowerSelector.includes("ratings");

      if (!isReviewDetailField) {
        return field;
      }

      return {
        ...field,
        description: "Displayed review count on the search result card",
        type: options?.preserveUserShape ? field.type : "string",
        multiple: options?.preserveUserShape ? field.multiple : false,
        transform: "trim",
      };
    }),
  };
}

function parsePlanResponse(text: string, snapshot?: PageSnapshot): ExtractionPlan {
  const output = extractJsonObject(text);

  if (!output) {
    throw new Error("The planner did not return parseable JSON.");
  }

  const parsed = JSON.parse(output) as ExtractionPlan;

  if (!parsed.fields || !Array.isArray(parsed.fields) || parsed.fields.length === 0) {
    throw new Error("The planner returned JSON, but it did not include any fields.");
  }

  const normalizedPlan = normalizePlan(parsed);
  return snapshot ? sanitizePlanForKnownPageContexts(normalizedPlan, snapshot) : normalizedPlan;
}

export async function generatePlan(input: {
  client: MiniMaxClientConfig;
  snapshot: PageSnapshot;
  extractionGoal: string;
}): Promise<ExtractionPlan> {
  const { client, snapshot, extractionGoal } = input;

  const response = await fetch(`${client.baseURL}/v1/text/chatcompletion_v2`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${client.apiKey}`,
    },
    body: JSON.stringify({
      model: client.model,
      stream: false,
      temperature: 0.2,
      top_p: 0.95,
      max_completion_tokens: 16384,
      messages: [
        {
          role: "system",
          content:
            "你是一个网页结构化抽取规划器。请严格只返回 JSON，不要返回解释、Markdown 或代码块。不要臆造页面中不存在的数据。字段名尽量使用简短 snake_case。若用户要“所有结果”“搜索结果列表”“全部商品”，必须将 extractionMode 设为 list，并提供 itemContainerSelector 与 itemContainerFallbackSelectors。列表模式下，fields 描述的是单个 item 内的字段，不要把字段自身标成数组。",
        },
        {
          role: "user",
          content: [
            `Target URL: ${snapshot.url}`,
            `Page title: ${snapshot.title}`,
            `Meta description: ${snapshot.description || "N/A"}`,
            `User extraction goal: ${extractionGoal}`,
            "",
            "Return a JSON object that follows this schema exactly:",
            JSON.stringify(PLAN_SCHEMA.schema, null, 2),
            "",
            "Visible element inventory:",
            JSON.stringify(snapshot.elements, null, 2),
            "",
            "HTML excerpt:",
            snapshot.htmlExcerpt,
            "",
            "Text excerpt:",
            snapshot.textExcerpt,
          ].join("\n"),
        },
      ],
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`MiniMax API error ${response.status}: ${errorText}`);
  }

  const json = (await response.json()) as {
    choices?: Array<{
      message?: {
        content?: string;
      };
    }>;
  };

  const text = json.choices?.[0]?.message?.content;

  if (!text) {
    throw new Error("The planner returned an empty response.");
  }

  return parsePlanResponse(text, snapshot);
}

export async function revisePlan(input: {
  client: MiniMaxClientConfig;
  snapshot: PageSnapshot;
  extractionGoal: string;
  currentPlan: ExtractionPlan;
  revisionInstruction: string;
}): Promise<ExtractionPlan> {
  const { client, snapshot, extractionGoal, currentPlan, revisionInstruction } = input;

  const response = await fetch(`${client.baseURL}/v1/text/chatcompletion_v2`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${client.apiKey}`,
    },
    body: JSON.stringify({
      model: client.model,
      stream: false,
      temperature: 0.1,
      top_p: 0.95,
      max_completion_tokens: 16384,
      messages: [
        {
          role: "system",
          content:
            "你是一个网页结构化抽取 schema 修订助手。你会根据用户对 schema 的修改意见，返回完整的更新后 JSON。请严格只返回 JSON，不要返回解释、Markdown 或代码块。除非用户明确要求修改抽取逻辑，否则尽量保持 selector、fallbackSelectors、itemContainerSelector、itemContainerFallbackSelectors 不变。可以按用户要求新增、删除、重命名字段，调整 description、type、required、multiple。",
        },
        {
          role: "user",
          content: [
            `Target URL: ${snapshot.url}`,
            `Page title: ${snapshot.title}`,
            `Meta description: ${snapshot.description || "N/A"}`,
            `User extraction goal: ${extractionGoal}`,
            `Revision instruction: ${revisionInstruction}`,
            "",
            "Current extraction plan JSON:",
            JSON.stringify(currentPlan, null, 2),
            "",
            "Return a JSON object that follows this schema exactly:",
            JSON.stringify(PLAN_SCHEMA.schema, null, 2),
            "",
            "Visible element inventory:",
            JSON.stringify(snapshot.elements, null, 2),
            "",
            "Text excerpt:",
            snapshot.textExcerpt,
          ].join("\n"),
        },
      ],
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`MiniMax API error ${response.status}: ${errorText}`);
  }

  const json = (await response.json()) as {
    choices?: Array<{
      message?: {
        content?: string;
      };
    }>;
  };

  const text = json.choices?.[0]?.message?.content;

  if (!text) {
    throw new Error("The planner returned an empty revision response.");
  }

  return parsePlanResponse(text, snapshot);
}
