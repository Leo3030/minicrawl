import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { Pool } from "pg";

import type {
  JobRecord,
  RuntimeStoreData,
  SessionProfileBinding,
  SitePolicy,
} from "./types.js";

const DEFAULT_STORE: RuntimeStoreData = {
  sessionProfiles: [],
  sitePolicies: [],
  jobs: [],
};

function cloneState(state: RuntimeStoreData): RuntimeStoreData {
  return JSON.parse(JSON.stringify(state)) as RuntimeStoreData;
}

export class RuntimeStore {
  private state: RuntimeStoreData = cloneState(DEFAULT_STORE);
  private readonly databaseUrl = process.env.DATABASE_URL?.trim();
  private readonly driver = this.databaseUrl ? "postgres" : "json-file";
  private pool: Pool | null = null;

  constructor(private readonly filePath: string) {}

  async initialize(): Promise<void> {
    if (this.driver === "postgres") {
      this.pool = new Pool({
        connectionString: this.databaseUrl,
      });
      await this.initializePostgres();
      return;
    }

    await mkdir(path.dirname(this.filePath), { recursive: true });

    try {
      const raw = await readFile(this.filePath, "utf8");
      this.state = {
        ...cloneState(DEFAULT_STORE),
        ...(JSON.parse(raw) as Partial<RuntimeStoreData>),
      };
    } catch (error) {
      const errorCode = (error as NodeJS.ErrnoException).code;

      if (errorCode !== "ENOENT") {
        throw error;
      }

      await this.flush();
    }
  }

  getSnapshot(): RuntimeStoreData {
    return cloneState(this.state);
  }

  getDriverInfo(): { driver: string; target: string } {
    if (this.driver === "postgres") {
      return {
        driver: "postgres",
        target: this.databaseUrl ?? "",
      };
    }

    return {
      driver: "json-file",
      target: this.filePath,
    };
  }

  async upsertSessionProfile(binding: SessionProfileBinding): Promise<void> {
    const index = this.state.sessionProfiles.findIndex((item) => item.id === binding.id);

    if (index >= 0) {
      this.state.sessionProfiles[index] = binding;
    } else {
      this.state.sessionProfiles.push(binding);
    }

    if (this.driver === "postgres") {
      await this.upsertSessionProfilePostgres(binding);
      return;
    }

    await this.flush();
  }

  listSessionProfiles(): SessionProfileBinding[] {
    return this.getSnapshot().sessionProfiles;
  }

  async upsertSitePolicy(policy: SitePolicy): Promise<void> {
    const index = this.state.sitePolicies.findIndex((item) => item.siteHost === policy.siteHost);

    if (index >= 0) {
      this.state.sitePolicies[index] = policy;
    } else {
      this.state.sitePolicies.push(policy);
    }

    if (this.driver === "postgres") {
      await this.upsertSitePolicyPostgres(policy);
      return;
    }

    await this.flush();
  }

  getSitePolicy(siteHost: string): SitePolicy | undefined {
    return this.state.sitePolicies.find((policy) => policy.siteHost === siteHost);
  }

  listSitePolicies(): SitePolicy[] {
    return this.getSnapshot().sitePolicies;
  }

  async createJob(job: JobRecord): Promise<void> {
    this.state.jobs.unshift(job);

    if (this.driver === "postgres") {
      await this.upsertJobPostgres(job);
      return;
    }

    await this.flush();
  }

  async updateJob(job: JobRecord): Promise<void> {
    const index = this.state.jobs.findIndex((item) => item.id === job.id);

    if (index < 0) {
      this.state.jobs.unshift(job);
    } else {
      this.state.jobs[index] = job;
    }

    if (this.driver === "postgres") {
      await this.upsertJobPostgres(job);
      return;
    }

    await this.flush();
  }

  getJob(jobId: string): JobRecord | undefined {
    return this.state.jobs.find((job) => job.id === jobId);
  }

  listJobs(limit = 20): JobRecord[] {
    return this.getSnapshot().jobs.slice(0, limit);
  }

  private async flush(): Promise<void> {
    await writeFile(this.filePath, `${JSON.stringify(this.state, null, 2)}\n`, "utf8");
  }

  private async initializePostgres(): Promise<void> {
    const pool = this.requirePool();

    await pool.query(`
      create table if not exists session_profiles (
        profile_id text primary key,
        site_host text not null,
        worker_id text not null,
        browser_profile jsonb not null,
        created_at timestamptz not null,
        last_used_at timestamptz not null
      );
    `);

    await pool.query(`
      create table if not exists site_policies (
        site_host text primary key,
        min_interval_ms integer not null,
        max_attempts integer not null,
        retry_backoff_ms integer not null,
        cooldown_ms integer not null,
        updated_at timestamptz not null
      );
    `);

    await pool.query(`
      create table if not exists jobs (
        job_id text primary key,
        status text not null,
        input jsonb not null,
        site_host text not null,
        session_profile_id text not null,
        created_at timestamptz not null,
        updated_at timestamptz not null,
        attempts jsonb not null,
        result jsonb,
        validation jsonb,
        error text
      );
    `);

    const [sessionProfiles, sitePolicies, jobs] = await Promise.all([
      pool.query(`
        select
          profile_id,
          site_host,
          worker_id,
          browser_profile,
          created_at,
          last_used_at
        from session_profiles
        order by created_at desc
      `),
      pool.query(`
        select
          site_host,
          min_interval_ms,
          max_attempts,
          retry_backoff_ms,
          cooldown_ms,
          updated_at
        from site_policies
        order by site_host asc
      `),
      pool.query(`
        select
          job_id,
          status,
          input,
          site_host,
          session_profile_id,
          created_at,
          updated_at,
          attempts,
          result,
          validation,
          error
        from jobs
        order by created_at desc
      `),
    ]);

    this.state = {
      sessionProfiles: sessionProfiles.rows.map((row) => ({
        id: row.profile_id,
        siteHost: row.site_host,
        workerId: row.worker_id,
        browserProfile: row.browser_profile,
        createdAt: new Date(row.created_at).toISOString(),
        lastUsedAt: new Date(row.last_used_at).toISOString(),
      })),
      sitePolicies: sitePolicies.rows.map((row) => ({
        siteHost: row.site_host,
        minIntervalMs: Number(row.min_interval_ms),
        maxAttempts: Number(row.max_attempts),
        retryBackoffMs: Number(row.retry_backoff_ms),
        cooldownMs: Number(row.cooldown_ms),
        updatedAt: new Date(row.updated_at).toISOString(),
      })),
      jobs: jobs.rows.map((row) => ({
        id: row.job_id,
        status: row.status,
        input: row.input,
        siteHost: row.site_host,
        sessionProfileId: row.session_profile_id,
        createdAt: new Date(row.created_at).toISOString(),
        updatedAt: new Date(row.updated_at).toISOString(),
        attempts: row.attempts ?? [],
        result: row.result ?? undefined,
        validation: row.validation ?? undefined,
        error: row.error ?? undefined,
      })),
    };
  }

  private async upsertSessionProfilePostgres(binding: SessionProfileBinding): Promise<void> {
    const pool = this.requirePool();

    await pool.query(
      `
        insert into session_profiles (
          profile_id,
          site_host,
          worker_id,
          browser_profile,
          created_at,
          last_used_at
        ) values ($1, $2, $3, $4::jsonb, $5, $6)
        on conflict (profile_id) do update set
          site_host = excluded.site_host,
          worker_id = excluded.worker_id,
          browser_profile = excluded.browser_profile,
          created_at = excluded.created_at,
          last_used_at = excluded.last_used_at
      `,
      [
        binding.id,
        binding.siteHost,
        binding.workerId,
        JSON.stringify(binding.browserProfile),
        binding.createdAt,
        binding.lastUsedAt,
      ],
    );
  }

  private async upsertSitePolicyPostgres(policy: SitePolicy): Promise<void> {
    const pool = this.requirePool();

    await pool.query(
      `
        insert into site_policies (
          site_host,
          min_interval_ms,
          max_attempts,
          retry_backoff_ms,
          cooldown_ms,
          updated_at
        ) values ($1, $2, $3, $4, $5, $6)
        on conflict (site_host) do update set
          min_interval_ms = excluded.min_interval_ms,
          max_attempts = excluded.max_attempts,
          retry_backoff_ms = excluded.retry_backoff_ms,
          cooldown_ms = excluded.cooldown_ms,
          updated_at = excluded.updated_at
      `,
      [
        policy.siteHost,
        policy.minIntervalMs,
        policy.maxAttempts,
        policy.retryBackoffMs,
        policy.cooldownMs,
        policy.updatedAt,
      ],
    );
  }

  private async upsertJobPostgres(job: JobRecord): Promise<void> {
    const pool = this.requirePool();

    await pool.query(
      `
        insert into jobs (
          job_id,
          status,
          input,
          site_host,
          session_profile_id,
          created_at,
          updated_at,
          attempts,
          result,
          validation,
          error
        ) values (
          $1, $2, $3::jsonb, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb, $11
        )
        on conflict (job_id) do update set
          status = excluded.status,
          input = excluded.input,
          site_host = excluded.site_host,
          session_profile_id = excluded.session_profile_id,
          created_at = excluded.created_at,
          updated_at = excluded.updated_at,
          attempts = excluded.attempts,
          result = excluded.result,
          validation = excluded.validation,
          error = excluded.error
      `,
      [
        job.id,
        job.status,
        JSON.stringify(job.input),
        job.siteHost,
        job.sessionProfileId,
        job.createdAt,
        job.updatedAt,
        JSON.stringify(job.attempts),
        JSON.stringify(job.result ?? null),
        JSON.stringify(job.validation ?? null),
        job.error ?? null,
      ],
    );
  }

  private requirePool(): Pool {
    if (!this.pool) {
      throw new Error("Postgres pool is not initialized.");
    }

    return this.pool;
  }
}
