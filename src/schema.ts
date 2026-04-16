import AjvModule, { type ErrorObject } from "ajv";

import type { ExtractionPlan, ExtractionResult, FieldType, ValidationSummary } from "./types.js";

type AjvConstructor = new (options?: { allErrors?: boolean }) => {
  compile: (schema: Record<string, unknown>) => {
    (data: unknown): boolean;
    errors?: ErrorObject[] | null;
  };
};

const Ajv = AjvModule as unknown as AjvConstructor;
const ajv = new Ajv({ allErrors: true });

function mapFieldType(type: FieldType): Record<string, unknown> {
  switch (type) {
    case "number":
      return { type: "number" };
    case "boolean":
      return { type: "boolean" };
    case "url":
    case "image":
      return { type: "string" };
    case "string[]":
      return { type: "array", items: { type: "string" } };
    case "number[]":
      return { type: "array", items: { type: "number" } };
    default:
      return { type: "string" };
  }
}

function buildFieldSchema(field: ExtractionPlan["fields"][number]): Record<string, unknown> {
  let fieldSchema: Record<string, unknown>;

  if (field.type === "object") {
    fieldSchema = buildObjectSchema(field.properties ?? []);
  } else if (field.type === "object[]") {
    fieldSchema = {
      type: "array",
      items: buildObjectSchema(field.properties ?? []),
    };
  } else {
    fieldSchema = {
      ...mapFieldType(field.type),
      description: field.description,
    };
  }

  if (!field.required) {
    return {
      anyOf: [fieldSchema, { type: "null" }],
      description: field.description,
    };
  }

  return fieldSchema;
}

function buildObjectSchema(fields: ExtractionPlan["fields"]): Record<string, unknown> {
  return {
    type: "object",
    additionalProperties: false,
    properties: Object.fromEntries(fields.map((field) => [field.name, buildFieldSchema(field)])),
    required: fields.filter((field) => field.required).map((field) => field.name),
  };
}

export function buildJsonSchema(plan: ExtractionPlan): Record<string, unknown> {
  const itemSchema = buildObjectSchema(plan.fields);

  if (plan.extractionMode === "list") {
    return {
      type: "array",
      items: itemSchema,
    };
  }

  return itemSchema;
}

export function validateResult(result: ExtractionResult): ValidationSummary {
  const validate = ajv.compile(result.schema);
  const valid = validate(result.data);

  return {
    valid: Boolean(valid),
    errors:
      validate.errors?.map((error: ErrorObject) => `${error.instancePath || "/"} ${error.message}`) ??
      [],
  };
}
