const form = document.querySelector("#extract-form");
const result = document.querySelector("#result");
const statusText = document.querySelector("#status");
const submitButton = document.querySelector("#submit-button");
const reviewWorkspace = document.querySelector("#review-workspace");
const schemaPreview = document.querySelector("#schema-preview");
const chatLog = document.querySelector("#chat-log");
const chatForm = document.querySelector("#chat-form");
const chatInput = document.querySelector("#chat-input");
const sendButton = document.querySelector("#send-button");
const confirmButton = document.querySelector("#confirm-button");
const resetButton = document.querySelector("#reset-button");
const liveLog = document.querySelector("#live-log");

const state = {
  draftPayload: null,
  plan: null,
  schema: null,
  messages: [],
  logs: [],
  logStream: null,
  isBusy: false,
};

function setBusy(nextBusy) {
  state.isBusy = nextBusy;
  submitButton.disabled = nextBusy;
  sendButton.disabled = nextBusy || !state.plan;
  confirmButton.disabled = nextBusy || !state.plan;
  resetButton.disabled = nextBusy;
}

function setStatus(text) {
  statusText.textContent = text;
}

function renderSchema() {
  schemaPreview.textContent = state.schema
    ? JSON.stringify(state.schema, null, 2)
    : "等待生成 schema 草案...";
}

function renderMessages() {
  if (state.messages.length === 0) {
    chatLog.innerHTML = '<div class="chat-bubble assistant"><span class="chat-role">assistant</span>生成 schema 后，你可以直接说“删掉某个字段”或“新增某个字段”。</div>';
    return;
  }

  chatLog.innerHTML = state.messages
    .map(
      (message) => `
        <div class="chat-bubble ${message.role}">
          <span class="chat-role">${message.role}</span>
          ${escapeHtml(message.content)}
        </div>
      `,
    )
    .join("");

  chatLog.scrollTop = chatLog.scrollHeight;
}

function renderReviewWorkspace() {
  reviewWorkspace.classList.toggle("is-hidden", !state.plan);
  renderSchema();
  renderMessages();
  setBusy(state.isBusy);
}

function renderResult(payload) {
  result.textContent = JSON.stringify(payload, null, 2);
}

function closeLogStream() {
  if (state.logStream) {
    state.logStream.close();
    state.logStream = null;
  }
}

function formatLogTime(timestamp) {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return timestamp;
  }

  return date.toLocaleTimeString("zh-CN", { hour12: false });
}

function renderLogs() {
  if (state.logs.length === 0) {
    liveLog.innerHTML = '<div class="log-line log-placeholder">等待开始抽取...</div>';
    return;
  }

  liveLog.innerHTML = state.logs
    .map(
      (entry) => `
        <div class="log-line is-${entry.level || "info"}">
          <span class="log-time">${escapeHtml(formatLogTime(entry.timestamp || ""))}</span>
          <span class="log-message">${escapeHtml(entry.message || "")}</span>
        </div>
      `,
    )
    .join("");

  liveLog.scrollTop = liveLog.scrollHeight;
}

function resetLogs() {
  closeLogStream();
  state.logs = [];
  renderLogs();
}

function openLogStream(requestId) {
  resetLogs();

  const stream = new EventSource(`/api/logs/stream?requestId=${encodeURIComponent(requestId)}`);
  state.logStream = stream;

  stream.addEventListener("log", (event) => {
    try {
      const entry = JSON.parse(event.data);
      state.logs.push(entry);
      renderLogs();
    } catch {
      state.logs.push({
        timestamp: new Date().toISOString(),
        level: "warn",
        message: "收到一条无法解析的日志。",
      });
      renderLogs();
    }
  });

  stream.addEventListener("done", () => {
    if (state.logStream === stream) {
      closeLogStream();
    }
  });
}

function resetState() {
  state.draftPayload = null;
  state.plan = null;
  state.schema = null;
  state.messages = [];
  chatInput.value = "";
  resetLogs();
  renderReviewWorkspace();
  renderResult({
    tip: "先生成 schema 草案，确认后再抽取。",
    responseShape: {
      ok: true,
      totalCount: 24,
      result: [],
    },
  });
  setStatus("等待输入");
  setBusy(false);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

async function requestJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const json = await response.json();

  if (!response.ok || json.ok === false) {
    throw new Error(json.error || `Request failed: ${response.status}`);
  }

  return json;
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();

  const formData = new FormData(form);
  const payload = {
    url: String(formData.get("url") || "").trim(),
    goal: String(formData.get("goal") || "").trim(),
    sessionProfileId: String(formData.get("sessionProfileId") || "").trim(),
    maxItems: formData.get("maxItems"),
    reviewsPerItem: formData.get("reviewsPerItem"),
  };

  state.draftPayload = payload;
  state.plan = null;
  state.schema = null;
  state.messages = [];
  resetLogs();
  renderReviewWorkspace();
  renderResult({ loading: "正在生成 schema 草案..." });
  setStatus("正在生成 schema 草案...");
  setBusy(true);

  try {
    const json = await requestJson("/api/schema/draft", payload);
    state.plan = json.plan;
    state.schema = json.schema;
    state.messages = [
      {
        role: "assistant",
        content: json.reply || "Schema 草案已生成。",
      },
    ];
    renderReviewWorkspace();
    renderResult({
      ok: true,
      tip: "Schema 草案已生成。你可以继续修改，确认后再抽取数据。",
    });
    setStatus("Schema 草案已生成");
  } catch (error) {
    renderResult({
      ok: false,
      error: error instanceof Error ? error.message : "Unknown draft error",
    });
    setStatus("生成 schema 失败");
  } finally {
    setBusy(false);
  }
});

chatForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  const message = chatInput.value.trim();

  if (!message || !state.plan || !state.draftPayload) {
    return;
  }

  state.messages.push({
    role: "user",
    content: message,
  });
  chatInput.value = "";
  renderMessages();
  setStatus("正在修改 schema...");
  setBusy(true);

  try {
    const json = await requestJson("/api/schema/revise", {
      url: state.draftPayload.url,
      goal: state.draftPayload.goal,
      message,
      plan: state.plan,
    });

    state.plan = json.plan;
    state.schema = json.schema;
    state.messages.push({
      role: "assistant",
      content: json.reply || "Schema 已更新。",
    });
    renderReviewWorkspace();
    setStatus("Schema 已更新");
  } catch (error) {
    state.messages.push({
      role: "assistant",
      content: error instanceof Error ? error.message : "Schema 修改失败",
    });
    renderMessages();
    setStatus("Schema 修改失败");
  } finally {
    setBusy(false);
  }
});

confirmButton.addEventListener("click", async () => {
  if (!state.plan || !state.draftPayload) {
    return;
  }

  const requestId = `extract_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  openLogStream(requestId);
  setBusy(true);
  setStatus("正在根据确认后的 schema 抽取数据...");
  renderResult({ loading: "正在抽取数据..." });

  try {
    const json = await requestJson("/api/extract", {
      ...state.draftPayload,
      requestId,
      plan: state.plan,
    });

    state.messages.push({
      role: "assistant",
      content: `抽取完成，共返回 ${json.totalCount} 条结果。`,
    });
    renderReviewWorkspace();
    renderResult(json);
    setStatus("抽取完成");
  } catch (error) {
    renderResult({
      ok: false,
      error: error instanceof Error ? error.message : "Unknown extraction error",
    });
    setStatus("抽取失败");
  } finally {
    window.setTimeout(() => {
      closeLogStream();
    }, 1500);
    setBusy(false);
  }
});

resetButton.addEventListener("click", () => {
  resetState();
});

resetState();
