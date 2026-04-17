import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { loadEnvFile } from "./env.js";
import {
  buildAmazonSearchPlan,
  capturePage,
  isAmazonSearchUrl,
  isTargetSearchUrl,
  runAmazonSearchExtraction,
  runTargetSearchExtraction,
  runExtraction,
} from "./extractor.js";
import { createOpenAIClient, generatePlan, revisePlan } from "./openai.js";
import { createExtractionOrchestrator } from "./orchestrator.js";
import { buildJsonSchema, validateResult } from "./schema.js";
import type {
  ExtractionData,
  ExtractionLogEntry,
  ExtractionLogLevel,
  ExtractionLogger,
  ExtractionPlan,
  ExtractionResult,
} from "./types.js";

loadEnvFile();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

interface LogStreamState {
  entries: ExtractionLogEntry[];
  clients: Set<express.Response>;
  completed: boolean;
  finalStatus?: "completed" | "failed";
  cleanupTimer?: ReturnType<typeof setTimeout>;
}

function normalizeSiteHost(rawUrl: string): string {
  return new URL(rawUrl).hostname.toLowerCase();
}

function parseOptionalPositiveNumber(rawValue: unknown): number | undefined {
  return rawValue === undefined || rawValue === null || rawValue === ""
    ? undefined
    : Number(rawValue);
}

function assertValidPositiveNumber(value: number | undefined, fieldName: string): void {
  if (value !== undefined && (!Number.isFinite(value) || value <= 0)) {
    throw new Error(`${fieldName} must be a positive number.`);
  }
}

function isExtractionPlan(value: unknown): value is ExtractionPlan {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Partial<ExtractionPlan>;

  return (
    typeof candidate.reasoning === "string" &&
    typeof candidate.pageType === "string" &&
    (candidate.extractionMode === "single" || candidate.extractionMode === "list") &&
    Array.isArray(candidate.fields) &&
    candidate.fields.length > 0
  );
}

function countExtractedItems(data: ExtractionData): number {
  return Array.isArray(data) ? data.length : Object.keys(data).length > 0 ? 1 : 0;
}

async function buildDraftPlan(args: {
  url: string;
  goal: string;
  client: ReturnType<typeof createOpenAIClient>;
}): Promise<{ snapshot: Awaited<ReturnType<typeof capturePage>>; plan: ExtractionPlan }> {
  const snapshot = await capturePage(args.url);
  const plan = isAmazonSearchUrl(args.url)
    ? buildAmazonSearchPlan()
    : await generatePlan({
        client: args.client,
        snapshot,
        extractionGoal: args.goal,
      });

  return { snapshot, plan };
}

async function bootstrap() {
  const app = express();
  const port = Number(process.env.PORT ?? 3000);
  const orchestrator = createExtractionOrchestrator();
  const logStreams = new Map<string, LogStreamState>();

  function writeSseEvent(
    res: express.Response,
    eventName: string,
    payload: unknown,
  ): void {
    res.write(`event: ${eventName}\n`);
    res.write(`data: ${JSON.stringify(payload)}\n\n`);
    (res as express.Response & { flush?: () => void }).flush?.();
  }

  function getOrCreateLogStream(requestId: string): LogStreamState {
    const existing = logStreams.get(requestId);

    if (existing) {
      return existing;
    }

    const created: LogStreamState = {
      entries: [],
      clients: new Set(),
      completed: false,
      finalStatus: undefined,
    };

    logStreams.set(requestId, created);
    return created;
  }

  function prepareLogStream(requestId: string): void {
    const existing = getOrCreateLogStream(requestId);

    if (existing.cleanupTimer) {
      clearTimeout(existing.cleanupTimer);
      existing.cleanupTimer = undefined;
    }

    existing.entries = [];
    existing.completed = false;
    existing.finalStatus = undefined;
  }

  function scheduleLogStreamCleanup(requestId: string): void {
    const stream = logStreams.get(requestId);

    if (!stream) {
      return;
    }

    if (stream.cleanupTimer) {
      clearTimeout(stream.cleanupTimer);
    }

    stream.cleanupTimer = setTimeout(() => {
      logStreams.delete(requestId);
    }, 60_000);
  }

  function emitRequestLog(
    requestId: string,
    message: string,
    level: ExtractionLogLevel = "info",
  ): void {
    const stream = getOrCreateLogStream(requestId);
    const entry: ExtractionLogEntry = {
      timestamp: new Date().toISOString(),
      level,
      message,
    };

    stream.entries.push(entry);

    if (stream.entries.length > 300) {
      stream.entries.shift();
    }

    const prefix = `[extract:${requestId}]`;
    const consoleMethod = level === "error" ? console.error : level === "warn" ? console.warn : console.log;
    consoleMethod(`${prefix} ${message}`);

    for (const client of stream.clients) {
      writeSseEvent(client, "log", entry);
    }
  }

  function completeRequestLog(requestId: string, status: "completed" | "failed"): void {
    const stream = getOrCreateLogStream(requestId);
    stream.completed = true;
    stream.finalStatus = status;

    for (const client of stream.clients) {
      writeSseEvent(client, "done", {
        timestamp: new Date().toISOString(),
        status,
      });
    }

    if (stream.clients.size === 0) {
      scheduleLogStreamCleanup(requestId);
    }
  }

  await orchestrator.initialize();

  app.use(express.json({ limit: "1mb" }));
  app.use(express.static(path.join(__dirname, "../public")));

  app.get("/api/health", (_req, res) => {
    res.json({ ok: true });
  });

  app.get("/api/logs/stream", (req, res) => {
    const requestId = String(req.query.requestId ?? "").trim();

    if (!requestId) {
      res.status(400).json({ error: "requestId is required." });
      return;
    }

    const stream = getOrCreateLogStream(requestId);

    if (stream.cleanupTimer) {
      clearTimeout(stream.cleanupTimer);
      stream.cleanupTimer = undefined;
    }

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders();
    res.write(": connected\n\n");

    stream.clients.add(res);
    writeSseEvent(res, "snapshot", {
      bufferedCount: stream.entries.length,
      completed: stream.completed,
      status: stream.finalStatus ?? null,
    });

    for (const entry of stream.entries) {
      writeSseEvent(res, "log", entry);
    }

    if (stream.completed) {
      writeSseEvent(res, "done", {
        timestamp: new Date().toISOString(),
        status: stream.finalStatus ?? "completed",
      });
    }

    res.on("close", () => {
      stream.clients.delete(res);

      if (stream.clients.size === 0 && stream.completed) {
        scheduleLogStreamCleanup(requestId);
      }
    });
  });

  app.get("/api/system/topology", (_req, res) => {
    res.json({
      ok: true,
      topology: orchestrator.getTopology(),
    });
  });

  app.get("/api/jobs", (req, res) => {
    const limit = Number(req.query.limit ?? 20);
    res.json({
      ok: true,
      jobs: orchestrator.listJobs(Number.isFinite(limit) ? limit : 20),
    });
  });

  app.get("/api/jobs/:jobId", (req, res) => {
    const job = orchestrator.getJob(req.params.jobId);

    if (!job) {
      res.status(404).json({ error: "Job not found." });
      return;
    }

    res.json({ ok: true, job });
  });

  app.get("/api/system/policies", (_req, res) => {
    res.json({
      ok: true,
      policies: orchestrator.listSitePolicies(),
    });
  });

  app.post("/api/system/policies", async (req, res) => {
    const siteHostInput = String(req.body?.siteHost ?? "").trim();
    const siteHost = siteHostInput.includes("://")
      ? normalizeSiteHost(siteHostInput)
      : siteHostInput.toLowerCase();

    if (!siteHost) {
      res.status(400).json({ error: "siteHost is required." });
      return;
    }

    try {
      const policy = await orchestrator.upsertSitePolicy({
        siteHost,
        minIntervalMs:
          req.body?.minIntervalMs === undefined ? undefined : Number(req.body.minIntervalMs),
        maxAttempts: req.body?.maxAttempts === undefined ? undefined : Number(req.body.maxAttempts),
        retryBackoffMs:
          req.body?.retryBackoffMs === undefined ? undefined : Number(req.body.retryBackoffMs),
        cooldownMs: req.body?.cooldownMs === undefined ? undefined : Number(req.body.cooldownMs),
      });

      res.json({ ok: true, policy });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown policy error";
      res.status(500).json({ error: message });
    }
  });

  app.post("/api/schema/draft", async (req, res) => {
    const url = String(req.body?.url ?? "").trim();
    const goal = String(req.body?.goal ?? "").trim();

    if (!url || !goal) {
      res.status(400).json({ error: "Both url and goal are required." });
      return;
    }

    try {
      const openai = createOpenAIClient();
      const { plan } = await buildDraftPlan({
        url,
        goal,
        client: openai,
      });

      res.json({
        ok: true,
        reply: "Schema 草案已生成。你可以继续说“删掉某个字段”“新增字段”“把某个字段改成 number”。",
        plan,
        schema: buildJsonSchema(plan),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown draft schema error";
      res.status(500).json({
        ok: false,
        error: message,
      });
    }
  });

  app.post("/api/schema/revise", async (req, res) => {
    const url = String(req.body?.url ?? "").trim();
    const goal = String(req.body?.goal ?? "").trim();
    const revisionInstruction = String(req.body?.message ?? "").trim();
    const currentPlan = req.body?.plan;

    if (!url || !goal || !revisionInstruction) {
      res.status(400).json({ error: "url, goal, and message are required." });
      return;
    }

    if (!isExtractionPlan(currentPlan)) {
      res.status(400).json({ error: "A valid current plan is required." });
      return;
    }

    try {
      const openai = createOpenAIClient();
      const snapshot = await capturePage(url);
      const nextPlan = await revisePlan({
        client: openai,
        snapshot,
        extractionGoal: goal,
        currentPlan,
        revisionInstruction,
      });

      res.json({
        ok: true,
        reply: "Schema 已按你的要求更新。你可以继续修改，或者直接确认后开始抽取。",
        plan: nextPlan,
        schema: buildJsonSchema(nextPlan),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown schema revision error";
      res.status(500).json({
        ok: false,
        error: message,
      });
    }
  });

  app.post("/api/extract", async (req, res) => {
    const url = String(req.body?.url ?? "").trim();
    const goal = String(req.body?.goal ?? "").trim();
    const requestId =
      String(req.body?.requestId ?? "").trim() ||
      `extract_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    const sessionProfileId = String(req.body?.sessionProfileId ?? "").trim() || undefined;
    const maxItems = parseOptionalPositiveNumber(req.body?.maxItems);
    const reviewsPerItem = parseOptionalPositiveNumber(req.body?.reviewsPerItem);
    const confirmedPlan = req.body?.plan;
    const log: ExtractionLogger = (message, level = "info") => {
      emitRequestLog(requestId, message, level);
    };

    prepareLogStream(requestId);
    log(`Received extract request for ${url || "unknown url"}.`);

    if (!url || !goal) {
      log("Rejecting extract request because url or goal is missing.", "error");
      completeRequestLog(requestId, "failed");
      res.status(400).json({ error: "Both url and goal are required." });
      return;
    }

    try {
      assertValidPositiveNumber(maxItems, "maxItems");
      assertValidPositiveNumber(reviewsPerItem, "reviewsPerItem");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Invalid maxItems";
      log(`Rejecting extract request: ${message}`, "error");
      completeRequestLog(requestId, "failed");
      res.status(400).json({ error: message });
      return;
    }

    try {
      log("Input validated. Scheduling extraction job.");
      const job = await orchestrator.enqueueJob({
        url,
        goal,
        sessionProfileId,
        maxItems,
        reviewsPerItem,
      });
      log(`Job ${job.id} queued for ${job.siteHost}.`);

      const finishedJob = await orchestrator.runJob(job.id, async (lease) => {
        log(
          `Running on ${lease.worker.id} (${lease.worker.egressLabel}, ${lease.worker.region}) with profile ${lease.sessionProfile.id}.`,
        );
        log("Capturing page snapshot.");
        const snapshot = await capturePage(url, lease.sessionProfile.browserProfile, {
          logger: log,
        });
        log(`Snapshot captured: ${snapshot.title || snapshot.url}`);
        const plan =
          confirmedPlan && isExtractionPlan(confirmedPlan)
            ? confirmedPlan
            : isAmazonSearchUrl(url)
              ? buildAmazonSearchPlan()
              : await generatePlan({
                  client: createOpenAIClient(),
                  snapshot,
                  extractionGoal: goal,
                });
        log(
          confirmedPlan && isExtractionPlan(confirmedPlan)
            ? "Using confirmed schema plan from the review flow."
            : "Generated extraction plan automatically.",
        );
        const schema = buildJsonSchema(plan);
        const data = isAmazonSearchUrl(url)
          ? plan.extractionMode === "list"
            ? await runAmazonSearchExtraction(url, plan, lease.sessionProfile.browserProfile, {
                maxItems,
                reviewsPerItem,
                logger: log,
              })
            : await runExtraction(url, plan, lease.sessionProfile.browserProfile, {
                logger: log,
              })
          : isTargetSearchUrl(url) && plan.extractionMode === "list"
            ? await runTargetSearchExtraction(url, plan, lease.sessionProfile.browserProfile, {
                maxItems,
                reviewsPerItem,
                logger: log,
              })
          : await runExtraction(url, plan, lease.sessionProfile.browserProfile, {
              logger: log,
            });

        if (
          (!isAmazonSearchUrl(url) && !isTargetSearchUrl(url)) ||
          plan.extractionMode !== "list"
        ) {
          log("Base extraction completed.");
        }

        const result: ExtractionResult = {
          schema,
          plan,
          data,
          diagnostics: {
            extractedAt: new Date().toISOString(),
            pageTitle: snapshot.title,
            finalUrl: snapshot.url,
            siteHost: lease.siteHost,
            sessionProfileId: lease.sessionProfile.id,
            workerId: lease.worker.id,
            egressLabel: lease.worker.egressLabel,
            egressRegion: lease.worker.region,
            rateLimitWaitMs: lease.rateLimitWaitMs,
          },
        };

        const validation = validateResult(result);
        log(
          validation.valid
            ? "Validation passed."
            : `Validation completed with ${validation.errors.length} issue(s).`,
          validation.valid ? "info" : "warn",
        );
        return {
          result,
          validation,
        };
      });

      if (finishedJob.status !== "completed" || !finishedJob.result) {
        log(finishedJob.error ?? "Extraction failed before producing a result.", "error");
        completeRequestLog(requestId, "failed");
        res.status(500).json({
          ok: false,
          error: finishedJob.error ?? "Extraction failed.",
        });
        return;
      }

      log(`Extraction completed. Returning ${countExtractedItems(finishedJob.result.data)} result item(s).`);
      completeRequestLog(requestId, "completed");

      res.json({
        ok: finishedJob.validation?.valid ?? true,
        totalCount: countExtractedItems(finishedJob.result.data),
        result: finishedJob.result.data,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown extraction error";
      log(message, "error");
      completeRequestLog(requestId, "failed");
      res.status(500).json({
        ok: false,
        error: message,
      });
    }
  });

  app.listen(port, () => {
    console.log(`Spider MVP listening on http://localhost:${port}`);
  });
}

bootstrap().catch((error) => {
  console.error("Failed to bootstrap Spider MVP", error);
  process.exitCode = 1;
});
