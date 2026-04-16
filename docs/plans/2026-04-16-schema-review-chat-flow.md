# Schema Review Chat Flow Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a chat-style schema review step so users can inspect and revise the generated schema before extraction, then run extraction against the confirmed schema and return `totalCount`.

**Architecture:** Split the current one-shot `/api/extract` flow into a two-step workflow. The server will generate a draft plan/schema first, revise it turn-by-turn from user instructions, and only extract after the user confirms the plan. The frontend will keep draft state in memory and render a lightweight schema review chat panel before showing extracted results.

**Tech Stack:** Express, TypeScript, Playwright, MiniMax chatcompletion API, static HTML/CSS/JS frontend.

---

### Task 1: Add review-flow API contracts

**Files:**
- Modify: `src/types.ts`
- Modify: `src/server.ts`

**Steps:**
1. Add request and response types for draft schema generation, schema revision, and confirmed extraction.
2. Extend extraction result types so API responses can include `totalCount`.
3. Keep the old extraction internals reusable, but change public responses to support the review-first flow.

### Task 2: Add planner revision support

**Files:**
- Modify: `src/openai.ts`

**Steps:**
1. Add a prompt path that accepts the current `ExtractionPlan` plus a user revision instruction.
2. Return a fully updated `ExtractionPlan`, not partial patches, so the frontend can treat each revision as the latest source of truth.
3. Keep field-level edits safe by preserving selectors unless the model has a strong reason to change them.

### Task 3: Refactor extraction execution around confirmed plans

**Files:**
- Modify: `src/extractor.ts`
- Modify: `src/schema.ts`
- Modify: `src/server.ts`

**Steps:**
1. Add a shared extraction entry point that accepts a confirmed `ExtractionPlan`.
2. Preserve Amazon pagination and filtering for confirmed Amazon list plans.
3. Return `{ ok, totalCount, result }` after confirmed extraction.

### Task 4: Build chat-style schema review UI

**Files:**
- Modify: `public/index.html`
- Modify: `public/styles.css`
- Modify: `public/app.js`

**Steps:**
1. Replace the single submit flow with a two-stage flow: generate schema, then confirm and extract.
2. Render current schema in a readable panel and add a simple chat transcript with a text input for revision commands.
3. Add explicit actions for `生成 schema`、`发送修改`、`确认 schema 并抽取`、`重新开始`.

### Task 5: Verify the end-to-end flow

**Files:**
- Modify as needed based on build/runtime fixes

**Steps:**
1. Run `pnpm build`.
2. Restart the local server.
3. Smoke test draft schema generation, one revision turn, and confirmed extraction on an Amazon search page.
4. Verify response shape includes `totalCount` and results still paginate when `maxItems` exceeds one page.
