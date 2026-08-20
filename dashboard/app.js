"use strict";

const state = {
  diagnostic: null,
  events: [],
  activeView: "overview",
  logFilter: "",
};

const columnsByTable = {
  "overview-positions": 6,
  "positions-table": 7,
  "history-table": 6,
  "issues-table": 6,
  "logs-table": 5,
};

const terminalEventKinds = new Set([
  "exit.closed",
  "exit.passive_close_fallback_terminal_flat",
  "exit.passive_close_recovery_probe_flat",
  "exit.passive_close_resolved",
  "recovery.flat",
  "runtime.position_lifecycle_terminal",
]);

const openingEventKinds = new Set(["entry.opened", "runtime.position_opened"]);

const elements = {
  diagnosticUpload: document.querySelector("#diagnostic-upload"),
  journalUpload: document.querySelector("#journal-upload"),
  openDiagnostic: document.querySelector("#open-diagnostic"),
  openJournal: document.querySelector("#open-journal"),
  loadLocal: document.querySelector("#load-local"),
  notice: document.querySelector("#notice"),
  sourceState: document.querySelector("#source-state"),
  logFilter: document.querySelector("#log-filter"),
};

function text(value, fallback = "未提供") {
  if (value === null || value === undefined || value === "") return fallback;
  return String(value);
}

function number(value, fallback = "--") {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 8 }).format(parsed);
}

function timestamp(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return "未提供";
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(parsed));
}

function escapeHtml(value) {
  return text(value, "").replace(/[&<>'"]/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "'": "&#39;",
    "\"": "&quot;",
  })[character]);
}

function eventPayload(event) {
  return event && typeof event.payload === "object" && event.payload !== null
    ? event.payload
    : {};
}

function eventIdentifier(event) {
  const payload = eventPayload(event);
  return text(
    payload.position_id ||
      payload.entry_id ||
      payload.pending_id ||
      payload.source_entry_id ||
      payload.internal_entry_id ||
      payload.pair_id,
    ""
  );
}

function eventSymbol(event) {
  const payload = eventPayload(event);
  return text(payload.symbol || payload.instrument || "", "").toUpperCase();
}

function eventVenue(event) {
  const payload = eventPayload(event);
  const venues = [payload.venue, payload.long_venue, payload.short_venue]
    .filter(Boolean)
    .map((value) => String(value).toLowerCase());
  return venues.length ? [...new Set(venues)].join(" / ") : "未提供";
}

function eventReason(event) {
  const payload = eventPayload(event);
  return text(
    payload.reason ||
      payload.error ||
      payload.message ||
      payload.exchange_msg ||
      payload.classification,
    text(event.kind, "未提供")
  );
}

function isTerminalEvent(event) {
  const payload = eventPayload(event);
  if (terminalEventKinds.has(text(event.kind, ""))) {
    if (event.kind !== "runtime.position_lifecycle_terminal") return true;
    return ["flat", "closed"].includes(text(payload.terminal_state || payload.state, "").toLowerCase());
  }
  return false;
}

function isProblemEvent(event) {
  const kind = text(event.kind, "").toLowerCase();
  const reason = eventReason(event).toLowerCase();
  return ["error", "failed", "rejected", "blocked", "recovery", "uncertainty", "drift"].some(
    (term) => kind.includes(term) || reason.includes(term)
  );
}

function showNotice(message, tone = "") {
  elements.notice.textContent = message;
  elements.notice.hidden = !message;
  elements.notice.className = `notice${tone ? ` is-${tone}` : ""}`;
}

function setText(selector, value) {
  const node = document.querySelector(selector);
  if (node) node.textContent = value;
}

function renderEmptyRow(target, message) {
  const body = document.querySelector(`#${target}`);
  if (!body) return;
  body.innerHTML = `<tr><td class="empty-cell" colspan="${columnsByTable[target]}">${escapeHtml(message)}</td></tr>`;
}

function renderRows(target, rows, emptyMessage) {
  const body = document.querySelector(`#${target}`);
  if (!body) return;
  body.innerHTML = rows.length
    ? rows.join("")
    : `<tr><td class="empty-cell" colspan="${columnsByTable[target]}">${escapeHtml(emptyMessage)}</td></tr>`;
}

function reportState() {
  return state.diagnostic && typeof state.diagnostic.local_state === "object"
    ? state.diagnostic.local_state
    : {};
}

function reportConclusion() {
  return state.diagnostic && typeof state.diagnostic.conclusion === "object"
    ? state.diagnostic.conclusion
    : {};
}

function reportExchangeTruth() {
  return state.diagnostic && typeof state.diagnostic.exchange_truth === "object"
    ? state.diagnostic.exchange_truth
    : {};
}

function statusClass(value) {
  const normalized = text(value, "").toLowerCase();
  if (["healthy", "running", "ok", "low"].includes(normalized)) return "is-good";
  if (["degraded", "warning", "medium"].includes(normalized)) return "is-warning";
  return "is-danger";
}

function statusLabel(value) {
  const labels = {
    healthy: "健康",
    unhealthy: "不健康",
    degraded: "降级",
    critical: "严重",
    running: "运行中",
    booting: "启动中",
    low: "低风险",
    medium: "中风险",
    high: "高风险",
  };
  const normalized = text(value, "").toLowerCase();
  return labels[normalized] || text(value);
}

function renderSummary() {
  if (!state.diagnostic) {
    setText("#status-kicker", "尚未加载诊断");
    setText("#status-title", "导入一次只读诊断开始查看");
    setText("#status-summary", "支持诊断 JSON 与可选的 journal JSONL。数据不会上传或写回交易系统。");
    setText("#generated-at", "未提供");
    setText("#metric-positions", "--");
    setText("#metric-positions-detail", "等待诊断");
    setText("#metric-pending-entry", "--");
    setText("#metric-pending-entry-detail", "等待诊断");
    setText("#metric-pending-close", "--");
    setText("#metric-pending-close-detail", "等待诊断");
    setText("#metric-risk-mode", "--");
    setText("#metric-risk-mode-detail", "等待诊断");
    return;
  }

  const localState = reportState();
  const conclusion = reportConclusion();
  const health = state.diagnostic && typeof state.diagnostic.health === "object" ? state.diagnostic.health : {};
  const status = text(conclusion.status, localState.lifecycle || "未提供");
  const generated = state.diagnostic ? timestamp(state.diagnostic.generated_at_ms) : "未提供";
  const pendingClose = Number(localState.pending_close_count || 0);
  const pendingReconciliation = Number(localState.pending_close_reconciliation_count || 0);

  setText("#status-kicker", `${statusLabel(status)} | ${statusLabel(conclusion.risk || localState.risk_mode)}`);
  setText("#status-title", state.diagnostic ? `当前诊断为${statusLabel(status)}` : "导入一次只读诊断开始查看");
  setText(
    "#status-summary",
    state.diagnostic
      ? text(conclusion.summary, "诊断未提供摘要。")
      : "支持诊断 JSON 与可选的 journal JSONL。数据不会上传或写回交易系统。"
  );
  setText("#generated-at", generated);
  setText("#metric-positions", number(localState.open_position_count));
  setText("#metric-positions-detail", text(localState.lifecycle, "等待诊断"));
  setText("#metric-pending-entry", number(localState.pending_entry_count));
  setText("#metric-pending-entry-detail", "本地恢复状态");
  setText("#metric-pending-close", number(pendingClose + pendingReconciliation));
  setText("#metric-pending-close-detail", `平仓 ${number(pendingClose, "0")} | 对账 ${number(pendingReconciliation, "0")}`);
  setText("#metric-risk-mode", statusLabel(localState.risk_mode));
  setText(
    "#metric-risk-mode-detail",
    health.fingerprints && health.fingerprints.length ? health.fingerprints.slice(0, 2).join("，") : "无健康指纹"
  );
}

function positionRows(withIdentifier) {
  const positions = Array.isArray(reportState().positions) ? reportState().positions : [];
  return positions.map((position) => {
    const cells = [
      withIdentifier ? text(position.position_id) : text(position.symbol),
      ...(withIdentifier ? [text(position.symbol)] : []),
      text(position.long_venue),
      text(position.short_venue),
      number(position.quantity),
      number(position.matched_quantity),
      timestamp(position.opened_at_ms),
    ];
    return `<tr>${cells.map((cell) => `<td>${escapeHtml(cell)}</td>`).join("")}</tr>`;
  });
}

function renderPositions() {
  renderRows("overview-positions", positionRows(false), "当前没有本地活动仓位。");
  renderRows("positions-table", positionRows(true), "当前没有本地活动仓位。");
}

function deriveHistory() {
  const history = new Map();
  state.events.forEach((event) => {
    const identifier = eventIdentifier(event);
    if (!identifier) return;
    const key = identifier;
    const payload = eventPayload(event);
    if (!history.has(key)) {
      history.set(key, {
        id: key,
        symbol: eventSymbol(event),
        longVenue: text(payload.long_venue, ""),
        shortVenue: text(payload.short_venue, ""),
        openedAt: 0,
        closedAt: 0,
        terminal: false,
      });
    }
    const entry = history.get(key);
    entry.symbol = entry.symbol || eventSymbol(event);
    entry.longVenue = entry.longVenue || text(payload.long_venue, "");
    entry.shortVenue = entry.shortVenue || text(payload.short_venue, "");
    if (openingEventKinds.has(text(event.kind, ""))) {
      entry.openedAt = entry.openedAt || Number(event.ts_ms || 0);
    }
    if (isTerminalEvent(event)) {
      entry.terminal = true;
      entry.closedAt = Math.max(entry.closedAt, Number(event.ts_ms || 0));
    }
  });
  return [...history.values()]
    .filter((entry) => entry.openedAt || entry.closedAt)
    .sort((left, right) => Math.max(right.openedAt, right.closedAt) - Math.max(left.openedAt, left.closedAt));
}

function renderHistory() {
  const rows = deriveHistory().map((entry) => {
    const route = [entry.longVenue, entry.shortVenue].filter(Boolean).join(" / ") || "未提供";
    return `<tr>
      <td>${escapeHtml(entry.id)}</td>
      <td>${escapeHtml(entry.symbol || "未提供")}</td>
      <td>${escapeHtml(route)}</td>
      <td>${escapeHtml(timestamp(entry.openedAt))}</td>
      <td>${escapeHtml(timestamp(entry.closedAt))}</td>
      <td><span class="status-chip ${entry.terminal ? "is-good" : "is-warning"}">${entry.terminal ? "已结束" : "未确认结束"}</span></td>
    </tr>`;
  });
  renderRows("history-table", rows, "导入 journal JSONL 后显示历史仓位生命周期。");
}

function diagnosticIssues() {
  const items = state.diagnostic && Array.isArray(state.diagnostic.order_error_evidence)
    ? state.diagnostic.order_error_evidence
    : [];
  return items.map((item) => ({
    time: item.ts_ms || item.last_seen_ms || 0,
    source: text(item.kind, "订单错误"),
    symbol: text(item.symbol, "未提供").toUpperCase(),
    venue: text(item.venue),
    reason: text(item.error || item.exchange_msg || item.reason),
    count: number(item.count, "1"),
  }));
}

function journalIssues() {
  return state.events.filter(isProblemEvent).map((event) => ({
    time: event.ts_ms || 0,
    source: text(event.kind),
    symbol: eventSymbol(event) || "未提供",
    venue: eventVenue(event),
    reason: eventReason(event),
    count: "1",
  }));
}

function issueItems() {
  return [...diagnosticIssues(), ...journalIssues()]
    .sort((left, right) => Number(right.time) - Number(left.time))
    .slice(0, 150);
}

function renderIssues() {
  const rows = issueItems().map((item) => `<tr>
    <td>${escapeHtml(timestamp(item.time))}</td>
    <td>${escapeHtml(item.source)}</td>
    <td>${escapeHtml(item.symbol)}</td>
    <td>${escapeHtml(item.venue)}</td>
    <td class="is-wrap">${escapeHtml(item.reason)}</td>
    <td>${escapeHtml(item.count)}</td>
  </tr>`);
  renderRows("issues-table", rows, "诊断和 journal 中没有发现问题开仓证据。");
}

function balanceCards() {
  const balanceViews = reportExchangeTruth().balance_views;
  if (!balanceViews || typeof balanceViews !== "object" || !Object.keys(balanceViews).length) return [];
  return Object.entries(balanceViews).map(([venue, view]) => {
    const safeView = view && typeof view === "object" ? view : {};
    const perp = safeView.perp && typeof safeView.perp === "object" ? safeView.perp : {};
    const spot = safeView.spot && typeof safeView.spot === "object" ? safeView.spot : {};
    const balances = Array.isArray(spot.balances) ? spot.balances : [];
    const assets = balances.length
      ? balances.slice(0, 4).map((asset) => `${text(asset.coin)} ${number(asset.total)}`).join("<br />")
      : "没有资产明细";
    return `<article class="balance-card">
      <h3>${escapeHtml(venue)}</h3>
      <p>${escapeHtml(text(safeView.classification, "余额视图已采集"))}</p>
      <div class="balance-values">
        <div><span>可用保证金</span><strong>${escapeHtml(number(perp.withdrawable))}</strong></div>
        <div><span>账户权益</span><strong>${escapeHtml(number(perp.account_value))}</strong></div>
        <div><span>USDC 总额</span><strong>${escapeHtml(number(spot.usdc_total))}</strong></div>
        <div><span>USDC 可用</span><strong>${escapeHtml(number(spot.usdc_available))}</strong></div>
      </div>
      <p class="balance-assets">${assets}</p>
    </article>`;
  });
}

function renderBalances() {
  const grid = document.querySelector("#balance-grid");
  if (!grid) return;
  const cards = balanceCards();
  grid.innerHTML = cards.length
    ? cards.join("")
    : `<article class="balance-card"><h3>尚无余额视图</h3><p>请导入包含 exchange_truth.balance_views 的诊断 JSON。未采集不代表余额为零。</p></article>`;
}

function renderActions() {
  const list = document.querySelector("#next-actions");
  if (!list) return;
  const actions = Array.isArray(reportConclusion().next_actions) ? reportConclusion().next_actions : [];
  list.innerHTML = (actions.length ? actions : ["导入诊断 JSON 以显示处置建议。"]).map(
    (action) => `<li>${escapeHtml(action)}</li>`
  ).join("");
}

function renderLogs() {
  const filter = state.logFilter.trim().toLowerCase();
  const rows = state.events
    .filter(isProblemEvent)
    .filter((event) => {
      if (!filter) return true;
      return [event.kind, eventSymbol(event), eventVenue(event), eventReason(event)]
        .join(" ")
        .toLowerCase()
        .includes(filter);
    })
    .sort((left, right) => Number(right.ts_ms || 0) - Number(left.ts_ms || 0))
    .slice(0, 300)
    .map((event) => `<tr>
      <td>${escapeHtml(timestamp(event.ts_ms))}</td>
      <td>${escapeHtml(text(event.kind))}</td>
      <td>${escapeHtml(eventSymbol(event) || "未提供")}</td>
      <td>${escapeHtml(eventVenue(event))}</td>
      <td class="is-wrap">${escapeHtml(eventReason(event))}</td>
    </tr>`);
  renderRows("logs-table", rows, "导入 journal JSONL 后显示需要关注的运行事件。");
}

function renderAll() {
  renderSummary();
  renderPositions();
  renderHistory();
  renderIssues();
  renderBalances();
  renderActions();
  renderLogs();
}

function showView(view) {
  state.activeView = view;
  document.querySelectorAll("[data-view-section]").forEach((section) => {
    section.classList.toggle("is-visible", section.dataset.viewSection === view);
  });
  document.querySelectorAll("[data-nav]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.nav === view);
  });
  const target = document.querySelector(`#${view}`);
  if (target) target.scrollIntoView({ behavior: "smooth", block: "start" });
}

function validateDiagnostic(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("诊断文件必须是 JSON 对象。");
  }
  if (!value.local_state && !value.conclusion && !value.exchange_truth) {
    throw new Error("该 JSON 不像 LightFee diagnose_live 输出，未找到可识别字段。");
  }
  return value;
}

function parseJournal(source) {
  const events = [];
  const errors = [];
  source.split(/\r?\n/).forEach((line, index) => {
    if (!line.trim()) return;
    try {
      const event = JSON.parse(line);
      if (event && typeof event === "object" && !Array.isArray(event)) events.push(event);
      else errors.push(index + 1);
    } catch {
      errors.push(index + 1);
    }
  });
  if (!events.length && errors.length) {
    throw new Error("未能从 journal 文件解析任何 JSONL 事件。");
  }
  return { events, errors };
}

async function importDiagnostic(file) {
  try {
    const parsed = validateDiagnostic(JSON.parse(await file.text()));
    state.diagnostic = parsed;
    elements.sourceState.textContent = `已导入诊断: ${file.name}`;
    renderAll();
    showNotice("诊断 JSON 已载入，只在当前浏览器会话中使用。", "success");
  } catch (error) {
    showNotice(error instanceof Error ? error.message : "无法读取诊断 JSON。", "error");
  }
}

async function importJournal(file) {
  try {
    const parsed = parseJournal(await file.text());
    state.events = parsed.events;
    elements.sourceState.textContent = state.diagnostic
      ? `诊断与 journal 已载入`
      : `已导入 journal: ${file.name}`;
    renderAll();
    const warning = parsed.errors.length ? `，跳过 ${parsed.errors.length} 行无效记录` : "";
    showNotice(`已载入 ${parsed.events.length} 条 journal 事件${warning}。`, "success");
  } catch (error) {
    showNotice(error instanceof Error ? error.message : "无法读取 journal JSONL。", "error");
  }
}

async function loadLocalSnapshots() {
  try {
    const diagnosticResponse = await fetch("./data/latest.json", { cache: "no-store" });
    if (!diagnosticResponse.ok) throw new Error("未找到 dashboard/data/latest.json。");
    state.diagnostic = validateDiagnostic(await diagnosticResponse.json());

    let journalMessage = "";
    try {
      const journalResponse = await fetch("./data/events.jsonl", { cache: "no-store" });
      if (journalResponse.ok) {
        const parsed = parseJournal(await journalResponse.text());
        state.events = parsed.events;
        journalMessage = `，以及 ${parsed.events.length} 条 journal 事件`;
      }
    } catch {
      journalMessage = "";
    }

    elements.sourceState.textContent = "已读取本地只读快照";
    renderAll();
    showNotice(`已读取诊断快照${journalMessage}。`, "success");
  } catch (error) {
    showNotice(
      `${error instanceof Error ? error.message : "本地快照读取失败。"} 请导入诊断或日志文件，或按 README 放置快照。`,
      "error"
    );
  }
}

document.querySelectorAll("[data-nav]").forEach((button) => {
  button.addEventListener("click", () => showView(button.dataset.nav));
});

elements.openDiagnostic.addEventListener("click", () => elements.diagnosticUpload.click());
elements.openJournal.addEventListener("click", () => elements.journalUpload.click());
elements.loadLocal.addEventListener("click", loadLocalSnapshots);
elements.diagnosticUpload.addEventListener("change", () => {
  const [file] = elements.diagnosticUpload.files;
  if (file) importDiagnostic(file);
  elements.diagnosticUpload.value = "";
});
elements.journalUpload.addEventListener("change", () => {
  const [file] = elements.journalUpload.files;
  if (file) importJournal(file);
  elements.journalUpload.value = "";
});
elements.logFilter.addEventListener("input", () => {
  state.logFilter = elements.logFilter.value;
  renderLogs();
});

renderAll();
