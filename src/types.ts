export type FieldType =
  | "string"
  | "number"
  | "boolean"
  | "url"
  | "image"
  | "object"
  | "object[]"
  | "string[]"
  | "number[]";

export interface ExtractionField {
  name: string;
  description: string;
  type: FieldType;
  required: boolean;
  multiple: boolean;
  selector: string;
  fallbackSelectors: string[];
  source: "text" | "attribute";
  attribute?: string;
  transform: "none" | "trim" | "number" | "url";
  properties?: ExtractionField[];
}

export interface ExtractionPlan {
  reasoning: string;
  pageType: string;
  extractionMode: "single" | "list";
  itemContainerSelector?: string;
  itemContainerFallbackSelectors?: string[];
  fields: ExtractionField[];
}

export type ExtractionData = Record<string, unknown> | Array<Record<string, unknown>>;

export interface BrowserProfile {
  id: string;
  locale: string;
  timezoneId: string;
  viewport: {
    width: number;
    height: number;
  };
  userAgent: string;
}

export interface ElementSnapshot {
  selector: string;
  tag: string;
  text: string;
  href?: string;
  src?: string;
  ariaLabel?: string;
}

export interface PageSnapshot {
  url: string;
  title: string;
  description: string;
  htmlExcerpt: string;
  textExcerpt: string;
  elements: ElementSnapshot[];
}

export type ExtractionLogLevel = "info" | "warn" | "error";

export interface ExtractionLogEntry {
  timestamp: string;
  level: ExtractionLogLevel;
  message: string;
}

export type ExtractionLogger = (message: string, level?: ExtractionLogLevel) => void;

export interface ExtractionResult {
  schema: Record<string, unknown>;
  plan: ExtractionPlan;
  data: ExtractionData;
  diagnostics: {
    extractedAt: string;
    pageTitle: string;
    finalUrl: string;
    siteHost: string;
    sessionProfileId: string;
    workerId: string;
    egressLabel: string;
    egressRegion: string;
    rateLimitWaitMs: number;
  };
}

export interface ValidationSummary {
  valid: boolean;
  errors: string[];
}

export interface WorkerDefinition {
  id: string;
  egressLabel: string;
  region: string;
  maxConcurrentSites: number;
}

export interface SessionProfileBinding {
  id: string;
  siteHost: string;
  workerId: string;
  browserProfile: BrowserProfile;
  createdAt: string;
  lastUsedAt: string;
}

export interface ScheduledLease {
  worker: WorkerDefinition;
  sessionProfile: SessionProfileBinding;
  siteHost: string;
  rateLimitWaitMs: number;
}

export interface SitePolicy {
  siteHost: string;
  minIntervalMs: number;
  maxAttempts: number;
  retryBackoffMs: number;
  cooldownMs: number;
  updatedAt: string;
}

export interface JobInput {
  url: string;
  goal: string;
  sessionProfileId?: string;
  maxItems?: number;
  reviewsPerItem?: number;
}

export type JobStatus = "pending" | "running" | "completed" | "failed";

export interface JobAttemptRecord {
  attempt: number;
  startedAt: string;
  completedAt?: string;
  success: boolean;
  error?: string;
  workerId?: string;
  rateLimitWaitMs?: number;
}

export interface JobRecord {
  id: string;
  status: JobStatus;
  input: JobInput;
  siteHost: string;
  sessionProfileId: string;
  createdAt: string;
  updatedAt: string;
  attempts: JobAttemptRecord[];
  result?: ExtractionResult;
  validation?: ValidationSummary;
  error?: string;
}

export interface RuntimeStoreData {
  sessionProfiles: SessionProfileBinding[];
  sitePolicies: SitePolicy[];
  jobs: JobRecord[];
}
