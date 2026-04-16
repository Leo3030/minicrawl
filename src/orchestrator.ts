import path from "node:path";

import type {
  BrowserProfile,
  JobAttemptRecord,
  JobInput,
  JobRecord,
  ScheduledLease,
  SessionProfileBinding,
  SitePolicy,
  ValidationSummary,
  WorkerDefinition,
} from "./types.js";
import { RuntimeStore } from "./runtime-store.js";

const DEFAULT_WORKERS: WorkerDefinition[] = [
  {
    id: "worker-us-east-1",
    egressLabel: "egress-us-east",
    region: "us-east-1",
    maxConcurrentSites: 2,
  },
  {
    id: "worker-eu-west-1",
    egressLabel: "egress-eu-west",
    region: "eu-west-1",
    maxConcurrentSites: 2,
  },
  {
    id: "worker-ap-southeast-1",
    egressLabel: "egress-ap-southeast",
    region: "ap-southeast-1",
    maxConcurrentSites: 2,
  },
];

const DEFAULT_USER_AGENTS = [
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
];

const DEFAULT_VIEWPORTS = [
  { width: 1440, height: 1200 },
  { width: 1366, height: 1024 },
  { width: 1536, height: 1152 },
];

function normalizeHost(rawUrl: string): string {
  const url = new URL(rawUrl);
  return url.hostname.toLowerCase();
}

function stableHash(value: string): number {
  let hash = 0;

  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) >>> 0;
  }

  return hash;
}

function toProfileId(rawUrl: string, requestedProfileId?: string): string {
  if (requestedProfileId?.trim()) {
    return requestedProfileId.trim().toLowerCase().replace(/[^a-z0-9:_-]+/g, "-");
  }

  return `profile:${normalizeHost(rawUrl)}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildBrowserProfile(profileId: string, siteHost: string): BrowserProfile {
  const hash = stableHash(`${profileId}:${siteHost}`);
  const viewport = DEFAULT_VIEWPORTS[hash % DEFAULT_VIEWPORTS.length];
  const userAgent = DEFAULT_USER_AGENTS[hash % DEFAULT_USER_AGENTS.length];
  const locale = ["en-US", "en-GB", "zh-CN"][hash % 3];
  const timezoneId = ["UTC", "America/New_York", "Asia/Singapore"][hash % 3];

  return {
    id: profileId,
    locale,
    timezoneId,
    viewport,
    userAgent,
  };
}

class SiteRateLimiter {
  private readonly nextReadyAt = new Map<string, number>();
  private readonly cooldownUntil = new Map<string, number>();

  async waitTurn(policy: SitePolicy): Promise<number> {
    const now = Date.now();
    const nextReady = this.nextReadyAt.get(policy.siteHost) ?? now;
    const cooldownReady = this.cooldownUntil.get(policy.siteHost) ?? now;
    const target = Math.max(now, nextReady, cooldownReady);
    const waitMs = Math.max(0, target - now);

    this.nextReadyAt.set(policy.siteHost, target + policy.minIntervalMs);

    if (waitMs > 0) {
      await sleep(waitMs);
    }

    return waitMs;
  }

  noteCooldown(policy: SitePolicy): void {
    this.cooldownUntil.set(policy.siteHost, Date.now() + policy.cooldownMs);
  }
}

class SessionRegistry {
  constructor(
    private readonly workers: WorkerDefinition[],
    private readonly store: RuntimeStore,
  ) {}

  async getOrCreate(rawUrl: string, requestedProfileId?: string): Promise<SessionProfileBinding> {
    const siteHost = normalizeHost(rawUrl);
    const profileId = toProfileId(rawUrl, requestedProfileId);
    const existing = this.store.listSessionProfiles().find((binding) => binding.id === profileId);

    if (existing) {
      const updated: SessionProfileBinding = {
        ...existing,
        lastUsedAt: new Date().toISOString(),
      };
      await this.store.upsertSessionProfile(updated);
      return updated;
    }

    const worker = this.pickWorker(profileId, siteHost);
    const now = new Date().toISOString();
    const binding: SessionProfileBinding = {
      id: profileId,
      siteHost,
      workerId: worker.id,
      browserProfile: buildBrowserProfile(profileId, siteHost),
      createdAt: now,
      lastUsedAt: now,
    };

    await this.store.upsertSessionProfile(binding);
    return binding;
  }

  listBindings(): SessionProfileBinding[] {
    return this.store.listSessionProfiles();
  }

  private pickWorker(profileId: string, siteHost: string): WorkerDefinition {
    const hash = stableHash(`${siteHost}:${profileId}`);
    return this.workers[hash % this.workers.length];
  }
}

function defaultPolicy(siteHost: string): SitePolicy {
  return {
    siteHost,
    minIntervalMs: Number(process.env.SITE_MIN_INTERVAL_MS ?? 1500),
    maxAttempts: Number(process.env.SITE_MAX_ATTEMPTS ?? 2),
    retryBackoffMs: Number(process.env.SITE_RETRY_BACKOFF_MS ?? 1200),
    cooldownMs: Number(process.env.SITE_COOLDOWN_MS ?? 5000),
    updatedAt: new Date().toISOString(),
  };
}

export class ExtractionOrchestrator {
  private readonly store: RuntimeStore;
  private readonly rateLimiter = new SiteRateLimiter();
  private readonly sessionRegistry: SessionRegistry;
  private readonly completionWaiters = new Map<string, Array<(job: JobRecord) => void>>();
  private readonly activeJobs = new Set<string>();

  constructor() {
    const filePath =
      process.env.RUNTIME_STORE_PATH ?? path.join(process.cwd(), "data/runtime-store.json");
    this.store = new RuntimeStore(filePath);
    this.sessionRegistry = new SessionRegistry(DEFAULT_WORKERS, this.store);
  }

  async initialize(): Promise<void> {
    await this.store.initialize();
  }

  getTopology() {
    const driverInfo = this.store.getDriverInfo();

    return {
      workers: DEFAULT_WORKERS,
      sitePolicies: this.store.listSitePolicies(),
      sessionProfiles: this.sessionRegistry.listBindings(),
      queue: {
        activeJobs: this.activeJobs.size,
        recentJobs: this.store.listJobs(10),
      },
      persistence: {
        driver: driverInfo.driver,
        target: driverInfo.target,
      },
    };
  }

  listJobs(limit?: number): JobRecord[] {
    return this.store.listJobs(limit);
  }

  getJob(jobId: string): JobRecord | undefined {
    return this.store.getJob(jobId);
  }

  listSitePolicies(): SitePolicy[] {
    return this.store.listSitePolicies();
  }

  async upsertSitePolicy(input: {
    siteHost: string;
    minIntervalMs?: number;
    maxAttempts?: number;
    retryBackoffMs?: number;
    cooldownMs?: number;
  }): Promise<SitePolicy> {
    const siteHost = input.siteHost.toLowerCase();
    const base = this.store.getSitePolicy(siteHost) ?? defaultPolicy(siteHost);
    const nextPolicy: SitePolicy = {
      siteHost,
      minIntervalMs: input.minIntervalMs ?? base.minIntervalMs,
      maxAttempts: input.maxAttempts ?? base.maxAttempts,
      retryBackoffMs: input.retryBackoffMs ?? base.retryBackoffMs,
      cooldownMs: input.cooldownMs ?? base.cooldownMs,
      updatedAt: new Date().toISOString(),
    };

    await this.store.upsertSitePolicy(nextPolicy);
    return nextPolicy;
  }

  async enqueueJob(input: JobInput): Promise<JobRecord> {
    const sessionProfileId = toProfileId(input.url, input.sessionProfileId);
    const now = new Date().toISOString();
    const job: JobRecord = {
      id: `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      status: "pending",
      input,
      siteHost: normalizeHost(input.url),
      sessionProfileId,
      createdAt: now,
      updatedAt: now,
      attempts: [],
    };

    await this.store.createJob(job);
    return job;
  }

  async runJob(
    jobId: string,
    handler: (lease: ScheduledLease) => Promise<{
      result: JobRecord["result"];
      validation: ValidationSummary;
    }>,
  ): Promise<JobRecord> {
    const initialJob = this.store.getJob(jobId);

    if (!initialJob) {
      throw new Error(`Job "${jobId}" was not found.`);
    }

    if (this.activeJobs.has(jobId)) {
      return this.waitForCompletion(jobId);
    }

    this.activeJobs.add(jobId);

    try {
      let job = {
        ...initialJob,
        status: "running" as const,
        updatedAt: new Date().toISOString(),
      };
      await this.store.updateJob(job);

      const policy = await this.ensureSitePolicy(job.siteHost);

      for (let attempt = 1; attempt <= policy.maxAttempts; attempt += 1) {
        const startedAt = new Date().toISOString();

        try {
          const lease = await this.acquireLease({
            url: job.input.url,
            sessionProfileId: job.input.sessionProfileId,
          });

          const payload = await handler(lease);

          const attemptRecord: JobAttemptRecord = {
            attempt,
            startedAt,
            completedAt: new Date().toISOString(),
            success: true,
            workerId: lease.worker.id,
            rateLimitWaitMs: lease.rateLimitWaitMs,
          };

          const completed = await this.completeJob({
            jobId,
            attemptRecord,
            payload,
          });

          return completed;
        } catch (error) {
          const message = error instanceof Error ? error.message : "Unknown job error";
          const attemptRecord: JobAttemptRecord = {
            attempt,
            startedAt,
            completedAt: new Date().toISOString(),
            success: false,
            error: message,
          };

          job = {
            ...job,
            attempts: [...job.attempts, attemptRecord],
            updatedAt: new Date().toISOString(),
          };
          await this.store.updateJob(job);

          if (attempt < policy.maxAttempts) {
            await sleep(policy.retryBackoffMs);
            continue;
          }

          this.rateLimiter.noteCooldown(policy);
          throw error;
        }
      }

      throw new Error(`Job "${jobId}" exhausted its attempts without producing a result.`);
    } catch (error) {
      const failedJob = {
        ...(this.store.getJob(jobId) as JobRecord),
        status: "failed" as const,
        error: error instanceof Error ? error.message : "Unknown job error",
        updatedAt: new Date().toISOString(),
      };
      await this.store.updateJob(failedJob);
      this.resolveWaiters(failedJob);
      return failedJob;
    } finally {
      this.activeJobs.delete(jobId);
    }
  }

  async completeJob(input: {
    jobId: string;
    attemptRecord: JobAttemptRecord;
    payload: {
      result: JobRecord["result"];
      validation: ValidationSummary;
    };
  }): Promise<JobRecord> {
    const existing = this.store.getJob(input.jobId);

    if (!existing) {
      throw new Error(`Job "${input.jobId}" was not found.`);
    }

    const completed: JobRecord = {
      ...existing,
      status: "completed",
      attempts: [...existing.attempts, input.attemptRecord],
      result: input.payload.result,
      validation: input.payload.validation,
      updatedAt: new Date().toISOString(),
      error: undefined,
    };

    await this.store.updateJob(completed);
    this.resolveWaiters(completed);
    return completed;
  }

  async failJob(jobId: string, message: string): Promise<JobRecord> {
    const existing = this.store.getJob(jobId);

    if (!existing) {
      throw new Error(`Job "${jobId}" was not found.`);
    }

    const failed: JobRecord = {
      ...existing,
      status: "failed",
      error: message,
      updatedAt: new Date().toISOString(),
    };

    await this.store.updateJob(failed);
    this.resolveWaiters(failed);
    return failed;
  }

  waitForCompletion(jobId: string): Promise<JobRecord> {
    const current = this.store.getJob(jobId);

    if (current && (current.status === "completed" || current.status === "failed")) {
      return Promise.resolve(current);
    }

    return new Promise((resolve) => {
      const waiters = this.completionWaiters.get(jobId) ?? [];
      waiters.push(resolve);
      this.completionWaiters.set(jobId, waiters);
    });
  }

  async acquireLease(input: { url: string; sessionProfileId?: string }): Promise<ScheduledLease> {
    const siteHost = normalizeHost(input.url);
    const sessionProfile = await this.sessionRegistry.getOrCreate(input.url, input.sessionProfileId);
    const worker = DEFAULT_WORKERS.find((candidate) => candidate.id === sessionProfile.workerId);
    const policy = await this.ensureSitePolicy(siteHost);

    if (!worker) {
      throw new Error(`Worker binding "${sessionProfile.workerId}" is unavailable.`);
    }

    const rateLimitWaitMs = await this.rateLimiter.waitTurn(policy);

    return {
      worker,
      sessionProfile,
      siteHost,
      rateLimitWaitMs,
    };
  }

  private async ensureSitePolicy(siteHost: string): Promise<SitePolicy> {
    const existing = this.store.getSitePolicy(siteHost);

    if (existing) {
      return existing;
    }

    const policy = defaultPolicy(siteHost);
    await this.store.upsertSitePolicy(policy);
    return policy;
  }

  private resolveWaiters(job: JobRecord): void {
    const waiters = this.completionWaiters.get(job.id) ?? [];

    for (const waiter of waiters) {
      waiter(job);
    }

    this.completionWaiters.delete(job.id);
  }
}

export function createExtractionOrchestrator(): ExtractionOrchestrator {
  return new ExtractionOrchestrator();
}
