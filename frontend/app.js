(function () {
  "use strict";

  const HISTORY_KEY = "pharma_ui_history_v1";
  const PREFS_KEY = "pharma_ui_prefs_v1";
  const MOBILE_BP = 980;
  const RUN_POLL_MS = 3500;

  const state = {
    searchPending: false,
    actionPending: false,
    history: [],
    activeHistoryId: null,
    drawerDesktopOpen: true,
    drawerMobileOpen: false,
    historyView: false,
    pendingRunId: null,
    pendingMessage: null,
    pendingSessions: new Set(),
    runningRunIds: new Set(),
    runningBySession: new Map(),
  };

  const $ = (id) => document.getElementById(id);
  const dom = {
    appShell: $("appShell"),
    timeline: $("timeline"),
    template: $("messageTemplate"),
    form: $("searchForm"),
    searchBtn: $("searchBtn"),
    mnn: $("mnnInput"),
    routes: $("routesInput"),
    baseForm: $("baseFormInput"),
    releaseType: $("releaseTypeInput"),
    dosage: $("dosageInput"),
    autorun: $("autorunCheckbox"),
    apiUrl: $("apiUrlInput"),
    xlsPath: $("xlsPathInput"),
    saveJson: $("saveJsonPathInput"),
    saveRouter: $("saveRouterOutputPathInput"),
    resetFormBtn: $("resetFormBtn"),
    drawerToggleBtn: $("drawerToggleBtn"),
    drawerCloseBtn: $("drawerCloseBtn"),
    drawerBackdrop: $("drawerBackdrop"),
    newChatBtn: $("newChatBtn"),
    historyList: $("historyList"),
    composerShell: $("composerShell"),
  };

  async function init() {
    dom.apiUrl.value = window.location.origin;
    loadPrefs();
    loadHistory();
    applyDrawerState();
    renderHistory();
    bind();
    await loadHistoryFromServer();
    restorePendingRun();
  }

  function bind() {
    dom.form.addEventListener("submit", onSearch);
    dom.resetFormBtn.addEventListener("click", () => {
      dom.form.reset();
      dom.releaseType.value = "обычное";
      dom.autorun.checked = true;
      dom.mnn.focus();
    });
    dom.timeline.addEventListener("click", onTimelineClick);
    dom.historyList.addEventListener("click", onHistoryClick);
    dom.newChatBtn.addEventListener("click", newChat);
    dom.drawerToggleBtn.addEventListener("click", toggleDrawer);
    dom.drawerCloseBtn.addEventListener("click", closeDrawerMobile);
    dom.drawerBackdrop.addEventListener("click", closeDrawerMobile);
    window.addEventListener("resize", applyDrawerState);
  }

  async function restorePendingRun() {
    updateComposerVisibility();
    try {
      const activeRecord = getActiveRecord();
      if (!activeRecord) {
        return;
      }
      maybeShowRunningForRecord(activeRecord);
    } catch {
      // ignore
    }
  }

  function showLastHistoryIfAny() {
    updateComposerVisibility();
  }

  async function pollRun(runId) {
    try {
      const data = await api(`/runs/get?run_id=${encodeURIComponent(runId)}`, "GET", null, 120000);
      const run = data.run;
      if (!run || run.status !== "done") {
        setTimeout(() => pollRun(runId), RUN_POLL_MS);
        return;
      }
      if (state.pendingMessage && state.pendingRunId === runId) removeNode(state.pendingMessage);
      state.pendingMessage = null;
      state.pendingRunId = null;
      state.runningRunIds.delete(runId);
      if (run.session_id && state.runningBySession.get(run.session_id) === runId) {
        state.runningBySession.delete(run.session_id);
      }

      const result = buildPipelineResultFromRun(run);
      ensureHistoryFromRun(run);
      updateHistoryFromPipeline(result);
      const activeRecord = getActiveRecord();
      if (activeRecord && isRunRelatedToRecord(run, activeRecord)) {
        appendPipeline(result);
      }
    } catch {
      setTimeout(() => pollRun(runId), RUN_POLL_MS);
    }
  }

  function isMobile() {
    return window.innerWidth <= MOBILE_BP;
  }

  function toggleDrawer() {
    if (isMobile()) state.drawerMobileOpen = !state.drawerMobileOpen;
    else state.drawerDesktopOpen = !state.drawerDesktopOpen;
    savePrefs();
    applyDrawerState();
  }

  function closeDrawerMobile() {
    if (isMobile()) state.drawerMobileOpen = false;
    else state.drawerDesktopOpen = false;
    savePrefs();
    applyDrawerState();
  }

  function applyDrawerState() {
    const open = isMobile() ? state.drawerMobileOpen : state.drawerDesktopOpen;
    dom.appShell.classList.toggle("drawer-open", open);
    dom.appShell.classList.toggle("drawer-collapsed", !open);
  }

  function loadPrefs() {
    try {
      const p = JSON.parse(localStorage.getItem(PREFS_KEY) || "{}");
      if (typeof p.drawerDesktopOpen === "boolean") state.drawerDesktopOpen = p.drawerDesktopOpen;
      if (typeof p.drawerMobileOpen === "boolean") state.drawerMobileOpen = p.drawerMobileOpen;
    } catch (_) {}
  }

  function savePrefs() {
    try {
      localStorage.setItem(PREFS_KEY, JSON.stringify({
        drawerDesktopOpen: state.drawerDesktopOpen,
        drawerMobileOpen: state.drawerMobileOpen,
      }));
    } catch (_) {}
  }

  function loadHistory() {
    try {
      const raw = JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]");
      state.history = Array.isArray(raw) ? raw : [];
    } catch (_) {
      state.history = [];
    }
  }

  async function loadHistoryFromServer() {
    try {
      const data = await api("/runs/list?limit=50", "GET", null, 120000);
      const runs = Array.isArray(data.runs) ? data.runs : [];
      const serverItems = runs.map(mapRunToHistory);
      const serverIds = new Set(serverItems.map((h) => h.runId));
      const serverSessions = new Set(serverItems.map((h) => h.sessionId).filter(Boolean));
      const localOnly = (state.history || []).filter((h) => {
        if (h.runId && serverIds.has(h.runId)) return false;
        if (h.sessionId && serverSessions.has(h.sessionId)) return false;
        return true;
      });
      state.history = [...serverItems, ...localOnly];
      state.runningRunIds = new Set(runs.filter((r) => r.status === "running").map((r) => r.id));
      state.runningBySession = new Map();
      runs.filter((r) => r.status === "running").forEach((r) => {
        if (r.session_id) state.runningBySession.set(r.session_id, r.id);
      });
      renderHistory();
    } catch (_) {
      // Keep local history if server is unavailable.
    }
  }

  function mapRunToHistory(run) {
    const createdAt = run.created_at || new Date().toISOString();
    const updatedAt = run.finished_at || run.created_at || createdAt;
    return {
      id: run.id,
      sessionId: run.session_id || null,
      createdAt,
      updatedAt,
      query: run.query || {},
      xlsPath: null,
      matchesCount: null,
      referenceOptionsCount: null,
      selectedReferenceDrug: run.selected_reference_drug || null,
      selectionJsonPath: run.selection_file_path || null,
      routerOutputPath: run.router_output_path || null,
      analysisPreview: run.router_output_text ? String(run.router_output_text).slice(0, 3500) : null,
      analysisText: run.router_output_text || null,
      selectionRows: run.selection_payload?.selected_reference_rows || null,
      runId: run.id,
      synopsisRunId: null,
      synopsisDocxUrl: null,
      synopsisStatus: null,
    };
  }

  function saveHistory() {
    try {
      localStorage.setItem(HISTORY_KEY, JSON.stringify(state.history.slice(0, 100)));
    } catch (_) {}
  }

  function newChat() {
    state.activeHistoryId = null;
    state.historyView = false;
    renderHistory();
    dom.timeline.innerHTML = "";
    dom.timeline.classList.remove("has-messages");
    if (state.pendingMessage) removeNode(state.pendingMessage);
    state.pendingMessage = null;
    state.pendingRunId = null;
    updateComposerVisibility();
    closeDrawerMobile();
  }

  function getApiBase() {
    return (dom.apiUrl.value || window.location.origin).trim().replace(/\/+$/, "");
  }

  async function api(path, method, body, timeoutMs) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs || 120000);
    try {
      const res = await fetch(`${getApiBase()}${path}`, {
        method: method || "GET",
        headers: body ? { "Content-Type": "application/json; charset=utf-8" } : undefined,
        body: body ? JSON.stringify(body) : undefined,
        signal: ctrl.signal,
      });
      const text = await res.text();
      let data = {};
      try { data = text ? JSON.parse(text) : {}; } catch { data = { ok: false, error: text }; }
      if (!res.ok || data.ok === false) {
        const e = new Error(data.error || `HTTP ${res.status}`);
        e.payload = data;
        throw e;
      }
      return data;
    } finally {
      clearTimeout(timer);
    }
  }

  function getQuery() {
    return {
      mnn: dom.mnn.value.trim(),
      routes: dom.routes.value.trim(),
      base_form: dom.baseForm.value.trim(),
      release_type: dom.releaseType.value.trim(),
      dosage: dom.dosage.value.trim(),
    };
  }

  function validateQuery(q) {
    const missing = Object.entries(q).filter(([, v]) => !v).map(([k]) => k);
    if (missing.length) throw new Error(`Заполните поля: ${missing.join(", ")}`);
  }

  async function onSearch(e) {
    e.preventDefault();
    if (state.searchPending) return;
    const q = getQuery();
    try { validateQuery(q); } catch (err) { return appendError(err.message); }

    closeDrawerMobile();
    state.activeHistoryId = null;
    state.historyView = false;
    renderHistory();
    updateComposerVisibility();

    appendUserQuery(q);
    const loading = appendLoading("щу референтные препараты...");

    const payload = { ...q };
    if (dom.xlsPath.value.trim()) payload.xls_path = dom.xlsPath.value.trim();

    setSearchPending(true);
    try {
      const result = await api("/reference/search", "POST", payload, 180000);
      removeNode(loading);
      handleSearchResult(result);
    } catch (err) {
      removeNode(loading);
      appendError(err.message, err.payload);
    } finally {
      setSearchPending(false);
    }
  }

  function handleSearchResult(result) {
    if (!result.matches_count) {
      appendAssistantMessage("Совпадения не найдены", ["Проверьте дозировку, тип высвобождения и путь введения."], "Поиск");
      return;
    }

    const recordId = cryptoId();
    const rec = {
      id: recordId,
      sessionId: result.session_id,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      query: result.query,
      xlsPath: result.xls_path,
      matchesCount: result.matches_count,
      referenceOptionsCount: result.reference_options_count,
      selectedReferenceDrug: null,
      selectionJsonPath: null,
      routerOutputPath: null,
      analysisPreview: null,
      analysisText: null,
      selectionRows: null,
      runId: null,
    };
    state.history.unshift(rec);
    state.activeHistoryId = recordId;
    saveHistory();
    renderHistory();

    const msg = createMessage("assistant", "Поиск референта", "Кандидаты");
    const body = msg.querySelector(".message-body");
    body.append(p(`Найдено ${result.matches_count} строк и ${result.reference_options_count} вариант(ов) референта.`));
    // no session/xls chips in UI

    const wrap = document.createElement("div");
    wrap.className = "ref-options";
    (result.reference_options || []).forEach((opt, i) => {
      wrap.append(referenceCard(opt, result.session_id, i + 1, dom.autorun.checked));
    });
    body.append(wrap);
    mountMessage(msg);
    dom.timeline.scrollTop = dom.timeline.scrollHeight;
  }

  function referenceCard(opt, sessionId, optionIndex, autorun) {
    const card = el("section", "ref-card");
    const head = el("div", "ref-card-head");
    const left = document.createElement("div");
    left.append(
      el("div", "ref-card-index", `Вариант ${optionIndex}`),
      el("div", "ref-card-title", opt.reference_drug || "—"),
      el("div", "ref-card-meta", `Строк: ${opt.rows_count ?? 0}`)
    );
    head.append(left);
    card.append(head);

    const samples = el("div", "ref-card-samples");
    (opt.sample_rows || []).slice(0, 3).forEach((s) => {
      samples.append(el("div", "ref-sample", `${s.trade_name || "—"} • ${s.drug_form || "—"} • ${s.dosage || "—"}`));
    });
    if ((opt.sample_rows || []).length) card.append(samples);

    const actions = el("div", "ref-card-actions");
    actions.append(selectBtn(sessionId, optionIndex, opt.reference_drug, autorun ? "pipeline" : "choose", autorun ? "Выбрать и запустить анализ" : "Выбрать референт"));
    if (autorun) actions.append(selectBtn(sessionId, optionIndex, opt.reference_drug, "choose", "Только сохранить выбор", true));
    card.append(actions);
    return card;
  }

  function selectBtn(sessionId, optionIndex, referenceDrug, mode, label, secondary) {
    const btn = el("button", secondary ? "ref-select-btn secondary" : "ref-select-btn", label);
    btn.type = "button";
    btn.dataset.action = "pick-reference";
    btn.dataset.sessionId = sessionId;
    btn.dataset.optionIndex = String(optionIndex);
    btn.dataset.referenceDrug = referenceDrug || "";
    btn.dataset.mode = mode;
    return btn;
  }

  async function onTimelineClick(e) {
    const btn = e.target.closest("[data-action]");
    if (!btn) return;
    if (btn.dataset.action === "pick-reference") {
      await chooseReference(btn);
      return;
    }
    if (btn.dataset.action === "build-synopsis") {
      await buildSynopsis(btn);
      return;
    }
    if (btn.dataset.action === "delete-message") {
      const msg = btn.closest(".message");
      if (msg) msg.remove();
      return;
    }
    if (btn.dataset.action === "delete-run") {
      await deleteRun(btn);
      return;
    }
    if (btn.dataset.action === "copy-text") {
      const src = document.getElementById(btn.dataset.targetId);
      if (!src) return;
      try {
        await navigator.clipboard.writeText(src.textContent || "");
        btn.textContent = "Скопировано";
        setTimeout(() => (btn.textContent = "Скопировать ответ"), 1200);
      } catch {
        appendError("Не удалось скопировать в буфер обмена");
      }
    }
  }

  async function chooseReference(btn) {
    if (state.actionPending) return;
    const sessionId = btn.dataset.sessionId;
    const optionIndex = Number(btn.dataset.optionIndex);
    const mode = btn.dataset.mode;
    const loading = mode === "pipeline"
      ? appendAnalysisInline("Выбор референта и запуск `test_router.py`...")
      : appendLoading("Сохраняю выбор референта...");
    setActionPending(true);
    if (mode === "pipeline" && sessionId) {
      state.pendingSessions.add(sessionId);
    }

    const body = { session_id: sessionId, option_index: optionIndex };
    if (dom.saveJson.value.trim()) body.save_json_path = dom.saveJson.value.trim();
    if (dom.saveRouter.value.trim()) body.save_router_output_path = dom.saveRouter.value.trim();

    try {
      const result = await api(mode === "pipeline" ? "/pipeline/analyze" : "/reference/choose", "POST", body, mode === "pipeline" ? 600000 : 120000);
      removeNode(loading);
      if (mode === "pipeline") {
        updateHistoryFromPipeline(result);
        if (sessionId) state.pendingSessions.delete(sessionId);
        const activeRecord = getActiveRecord();
        if (activeRecord && activeRecord.sessionId === sessionId) {
          if (state.pendingMessage) removeNode(state.pendingMessage);
          state.pendingMessage = null;
          state.pendingRunId = null;
          appendPipeline(result);
        }
      } else {
        updateHistoryFromChoose(result);
        const activeRecord = getActiveRecord();
        if (activeRecord && activeRecord.sessionId === sessionId) {
          appendChooseOnly(result);
        }
      }
    } catch (err) {
      removeNode(loading);
      if (mode === "pipeline" && sessionId) state.pendingSessions.delete(sessionId);
      const activeRecord = getActiveRecord();
      if (activeRecord && activeRecord.sessionId === sessionId) {
        appendError(err.message, err.payload);
      }
    } finally {
      setActionPending(false);
    }
  }

  function updateHistoryFromChoose(result) {
    patchHistory(result.session_id, {
      updatedAt: new Date().toISOString(),
      selectedReferenceDrug: result.selected_reference_drug,
      selectionJsonPath: result.saved_json_path,
      selectionRows: result.selection_payload?.selected_reference_rows || null,
      runId: result.run_id || null,
    });
  }

  function updateHistoryFromPipeline(result) {
    const s = result.selection || {};
    const r = result.router || {};
    patchHistory(s.session_id, {
      updatedAt: new Date().toISOString(),
      selectedReferenceDrug: s.selected_reference_drug,
      selectionJsonPath: s.saved_json_path,
      routerOutputPath: r.saved_response_path || null,
      analysisPreview: r.analysis_text ? String(r.analysis_text).slice(0, 3500) : null,
      analysisText: r.analysis_text !== undefined ? String(r.analysis_text) : null,
      selectionRows: s.selection_payload?.selected_reference_rows || null,
      runId: s.run_id || null,
    });
  }

  function buildPipelineResultFromRun(run) {
    const selectionPayload = run.selection_payload || null;
    return {
      selection: {
        run_id: run.id,
        session_id: run.session_id,
        saved_json_path: run.selection_file_path || null,
        selected_reference_drug: run.selected_reference_drug || null,
        selected_reference_rows_count: run.selection_rows_count ?? null,
        selection_payload: selectionPayload,
      },
      router: {
        reference_drug: run.selected_reference_drug || null,
        saved_response_path: run.router_output_path || null,
        analysis_text: run.router_output_text !== undefined ? run.router_output_text : null,
      },
    };
  }

  function ensureHistoryFromRun(run) {
    if (state.history.some((h) => h.runId === run.id)) return;
    const recordId = cryptoId();
    const rec = {
      id: recordId,
      sessionId: run.session_id || null,
      createdAt: run.created_at || new Date().toISOString(),
      updatedAt: run.finished_at || run.created_at || new Date().toISOString(),
      query: run.query || {},
      xlsPath: null,
      matchesCount: null,
      referenceOptionsCount: null,
      selectedReferenceDrug: run.selected_reference_drug || null,
      selectionJsonPath: run.selection_file_path || null,
      routerOutputPath: run.router_output_path || null,
      analysisPreview: run.router_output_text ? String(run.router_output_text).slice(0, 3500) : null,
      analysisText: run.router_output_text || null,
      selectionRows: run.selection_payload?.selected_reference_rows || null,
      runId: run.id,
    };
    state.history.unshift(rec);
    saveHistory();
    renderHistory();
  }

  function patchHistory(sessionId, patch) {
    const idx = state.history.findIndex((h) => h.sessionId === sessionId);
    if (idx === -1) return;
    state.history[idx] = { ...state.history[idx], ...patch };
    state.activeHistoryId = state.history[idx].id;
    if (idx > 0) {
      const [item] = state.history.splice(idx, 1);
      state.history.unshift(item);
    }
    saveHistory();
    renderHistory();
  }

  function patchHistoryByRunId(runId, patch) {
    const idx = state.history.findIndex((h) => h.runId === runId);
    if (idx === -1) return;
    state.history[idx] = { ...state.history[idx], ...patch };
    saveHistory();
    renderHistory();
  }

  function updateHistoryWithSynopsis(runId, synopsis) {
    if (!runId || !synopsis) return;
    patchHistoryByRunId(runId, {
      synopsisRunId: synopsis.id || null,
      synopsisDocxUrl: synopsis.download_url || null,
      synopsisStatus: synopsis.status || null,
    });
  }

  function appendChooseOnly(result) {
    const msg = createMessage("assistant", "Референт выбран", "Сохранение");
    if (result.run_id) msg.dataset.runId = result.run_id;
    const body = msg.querySelector(".message-body");
    body.append(p("JSON выбора сохранен. Анализ `test_router.py` не запускался."));
    body.append(kvGrid([
      ["Референт", result.selected_reference_drug || "—"],
      ["Строк по референту", String(result.selected_reference_rows_count ?? "—")],
      ["session_id", result.session_id || "—"],
      ["JSON", result.saved_json_path || "—"],
    ]));
    if (result.selection_payload && result.selection_payload.selected_reference_rows) {
      body.append(buildReferenceTable(result.selection_payload.selected_reference_rows));
    }
    appendDeleteRunAction(body, result.run_id);
    mountMessage(msg);
  }

  function appendPipeline(result) {
    const s = result.selection || {};
    const r = result.router || {};
    const msg = createMessage("assistant", "Анализ завершен", "Pipeline");
    if (s.run_id) msg.dataset.runId = s.run_id;
    const body = msg.querySelector(".message-body");
    body.append(p("Референт выбран и передан в `test_router.py`."));
    body.append(kvGrid([
      ["Референт", s.selected_reference_drug || r.reference_drug || "—"],
      ["JSON выбора", s.saved_json_path || "—"],
      ["Файл ответа", r.saved_response_path || "—"],
      ["Строк по референту", String(s.selected_reference_rows_count ?? "—")],
    ]));

    if (s.selection_payload && s.selection_payload.selected_reference_rows) {
      body.append(buildReferenceTable(s.selection_payload.selected_reference_rows));
    }

    if (r.analysis_text) {
      const parsedTable = parseMarkdownTable(r.analysis_text);
      if (parsedTable) {
        body.append(buildAnalysisTable(parsedTable));
      }
      const details = document.createElement("details");
      details.className = "text-block";
      const summary = document.createElement("summary");
      summary.textContent = "Ответ `test_router.py`";
      details.append(summary);

      const pre = el("pre", "mono-block");
      pre.id = `analysis-${cryptoId()}`;
      pre.textContent = r.analysis_text;
      details.append(pre);

      const row = el("div", "message-actions-row");
      const copy = el("button", "ghost-btn", "Скопировать ответ");
      copy.type = "button";
      copy.dataset.action = "copy-text";
      copy.dataset.targetId = pre.id;
      row.append(copy);
      details.append(row);
      body.append(details);
    } else if (r.analysis_text === "") {
      const details = document.createElement("details");
      details.className = "text-block";
      const summary = document.createElement("summary");
      summary.textContent = "Ответ `test_router.py`";
      details.append(summary);
      details.append(el("p", null, "Ответ пустой (0 символов)."));
      body.append(details);
    }
    if (s.run_id) {
      const record = getRecordByRunId(s.run_id);
      const block = buildSynopsisSection(s.run_id, record);
      body.append(block);
      hydrateSynopsisSection(block, s.run_id);
    }
    appendDeleteRunAction(body, s.run_id);
    mountMessage(msg);
    updateComposerVisibility();
  }

  function appendUserQuery(q) {
    const msg = createMessage("user", "Запрос", "Поля");
    msg.querySelector(".message-body").append(kvGrid([
      ["МНН", q.mnn], ["Пути введения", q.routes], ["Базовая форма", q.base_form],
      ["Тип высвобождения", q.release_type], ["Дозировка", q.dosage],
    ]));
    mountMessage(msg);
  }

  function appendAssistantMessage(title, lines, metaRight) {
    const msg = createMessage("assistant", title, metaRight || "Ответ");
    const body = msg.querySelector(".message-body");
    (lines || []).forEach((line) => body.append(p(line)));
    mountMessage(msg);
  }

  function appendLoading(text) {
    const msg = createMessage("assistant", "Обработка", "Ожидание");
    const row = el("div", "loading-line");
    const dots = el("span", "dots");
    dots.append(el("span"), el("span"), el("span"));
    row.append(el("span", null, text), dots);
    msg.querySelector(".message-body").append(row);
    mountMessage(msg);
    return msg;
  }

  function appendAnalysisInline(text) {
    const msg = createMessage("assistant", "Анализ", "В процессе");
    const body = msg.querySelector(".message-body");
    const card = el("div", "analysis-inline");
    card.append(
      el("div", "analysis-title", "Анализ выполняется"),
      el("div", "analysis-subtitle", text)
    );

    const rain = el("div", "pill-rain");
    for (let i = 0; i < 10; i += 1) {
      rain.append(el("span"));
    }
    card.append(rain);

    body.append(card);
    mountMessage(msg);
    return msg;
  }

  function appendError(message, payload) {
    const msg = createMessage("system", "Ошибка", "API");
    const body = msg.querySelector(".message-body");
    body.append(p(message));
    if (payload) {
      const d = document.createElement("details");
      d.className = "text-block";
      const s = document.createElement("summary");
      s.textContent = "Детали";
      d.append(s, el("pre", "mono-block", JSON.stringify(payload, null, 2)));
      body.append(d);
    }
    mountMessage(msg);
  }

  function createMessage(role, leftText, rightText) {
    const node = dom.template.content.firstElementChild.cloneNode(true);
    node.dataset.role = role;
    const meta = node.querySelector(".message-meta");
    meta.append(el("span", null, leftText), el("span", null, `${rightText} • ${fmtTime(new Date())}`));
    return node;
  }

  function mountMessage(node) {
    dom.timeline.classList.add("has-messages");
    dom.timeline.append(node);
    dom.timeline.scrollTop = dom.timeline.scrollHeight;
  }

  function removeNode(node) {
    if (node && node.parentNode) node.parentNode.removeChild(node);
  }

  function el(tag, className, text) {
    const n = document.createElement(tag);
    if (className) n.className = className;
    if (typeof text === "string") n.textContent = text;
    return n;
  }

  function p(text) {
    return el("p", null, text);
  }

  function kvGrid(items) {
    const grid = el("div", "kv-grid");
    items.forEach(([k, v]) => {
      const box = el("div", "kv");
      box.append(el("div", "k", k), el("div", "v", v || "—"));
      grid.append(box);
    });
    return grid;
  }

  function buildReferenceTable(rows) {
    const wrapper = el("div", "ref-table-wrap");
    const table = el("table", "ref-table");
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    ["ТН", "Форма", "Дозировка", "Страна", "РУ", "Дата РУ"].forEach((title) => {
      headRow.append(el("th", null, title));
    });
    thead.append(headRow);
    table.append(thead);

    const tbody = document.createElement("tbody");
    rows.slice(0, 20).forEach((row) => {
      const tr = document.createElement("tr");
      tr.append(
        el("td", null, row.trade_name || "—"),
        el("td", null, row.drug_form || "—"),
        el("td", null, row.dosage || "—"),
        el("td", null, row.country || "—"),
        el("td", null, row.ru_number || "—"),
        el("td", null, row.ru_date || "—")
      );
      tbody.append(tr);
    });
    table.append(tbody);
    wrapper.append(table);
    return wrapper;
  }

  function parseMarkdownTable(text) {
    if (!text) return null;
    const lines = String(text).split(/\r?\n/);
    let start = -1;
    for (let i = 0; i < lines.length - 1; i += 1) {
      const line = lines[i];
      const next = lines[i + 1];
      if (line.includes("|") && next && next.trim().startsWith("|") && next.includes("---")) {
        start = i;
        break;
      }
    }
    if (start === -1) return null;
    const header = splitRow(lines[start]);
    const rows = [];
    for (let i = start + 2; i < lines.length; i += 1) {
      const l = lines[i];
      if (!l || !l.includes("|")) break;
      const r = splitRow(l);
      if (r.length) rows.push(r);
    }
    if (!header.length || !rows.length) return null;
    return { header, rows };
  }

  function splitRow(line) {
    return line
      .trim()
      .replace(/^\|/, "")
      .replace(/\|$/, "")
      .split("|")
      .map((c) => c.trim())
      .filter((c) => c.length);
  }

  function buildAnalysisTable(table) {
    const wrapper = el("div", "analysis-table-wrap");
    const title = el("div", "analysis-table-title", "Таблица найденных данных");
    wrapper.append(title);
    const t = el("table", "analysis-table");
    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    table.header.forEach((h) => trh.append(el("th", null, h)));
    thead.append(trh);
    t.append(thead);
    const tbody = document.createElement("tbody");
    table.rows.forEach((row) => {
      const tr = document.createElement("tr");
      row.forEach((cell) => tr.append(el("td", null, cell)));
      tbody.append(tr);
    });
    t.append(tbody);
    wrapper.append(t);
    return wrapper;
  }

  function chipRow(values) {
    const row = el("div", "message-actions-row");
    values.forEach((v) => row.append(el("span", "note-chip", v)));
    return row;
  }

  function shortPath(path) {
    if (!path) return "—";
    const parts = String(path).split(/[/\\]+/);
    return parts.slice(-2).join("/");
  }

  function fmtTime(d) {
    try {
      return new Intl.DateTimeFormat("ru-RU", { hour: "2-digit", minute: "2-digit" }).format(d);
    } catch (_) {
      return d.toLocaleTimeString();
    }
  }

  function fmtDate(iso) {
    if (!iso) return "—";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    try {
      return new Intl.DateTimeFormat("ru-RU", {
        day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit",
      }).format(d);
    } catch (_) {
      return d.toLocaleString();
    }
  }

  function cryptoId() {
    return (crypto && crypto.randomUUID) ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  function renderHistory() {
    dom.historyList.innerHTML = "";
    if (!state.history.length) {
      dom.historyList.append(el("div", "history-empty", "История пока пуста. После поиска запись появится здесь."));
      return;
    }
    state.history.forEach((h) => {
      const b = el("button", "history-item");
      b.type = "button";
      b.dataset.historyId = h.id;
      if (h.id === state.activeHistoryId) b.classList.add("is-active");
      const titleRow = el("div", "history-item-title-row");
      titleRow.append(el("div", "history-item-title", `${h.query?.mnn || "—"} • ${h.query?.dosage || "—"}`));
      if (h.runId) {
        const del = el("button", "history-delete-btn", "✕");
        del.type = "button";
        del.dataset.action = "delete-history";
        del.dataset.runId = h.runId;
        del.dataset.historyId = h.id;
        titleRow.append(del);
      }
      b.append(
        titleRow,
        subRow(`${fmtDate(h.updatedAt || h.createdAt)}`, `${h.referenceOptionsCount || 0} refs / ${h.matchesCount || 0} rows`)
      );
      if (h.selectedReferenceDrug) b.append(el("div", "history-item-ref", trim(h.selectedReferenceDrug, 120)));
      dom.historyList.append(b);
    });
  }

  function subRow(left, right) {
    const row = el("div", "history-item-sub");
    row.append(el("span", null, left), el("span", null, right));
    return row;
  }

  function trim(text, n) {
    const t = String(text || "");
    return t.length > n ? `${t.slice(0, n - 1)}…` : t;
  }

  function onHistoryClick(e) {
    const actionBtn = e.target.closest("[data-action]");
    if (actionBtn && actionBtn.dataset.action === "delete-history") {
      e.preventDefault();
      e.stopPropagation();
      deleteHistoryItem(actionBtn);
      return;
    }
    const btn = e.target.closest("[data-history-id]");
    if (!btn) return;
    const record = state.history.find((h) => h.id === btn.dataset.historyId);
    if (!record) return;
    state.activeHistoryId = record.id;
    state.historyView = true;
    renderHistory();
    fillForm(record);
    renderHistoryConversation(record);
  }

  function fillForm(record) {
    const q = record.query || {};
    dom.mnn.value = q.mnn || "";
    dom.routes.value = q.routes || "";
    dom.baseForm.value = q.base_form || "";
    dom.releaseType.value = q.release_type || "обычное";
    dom.dosage.value = q.dosage || "";
    if (record.xlsPath) dom.xlsPath.value = record.xlsPath;
  }

  function renderHistoryConversation(record) {
    dom.timeline.innerHTML = "";
    dom.timeline.classList.remove("has-messages");
    state.pendingMessage = null;
    state.pendingRunId = null;
    appendUserQuery(record.query || {});
    appendAssistantMessage(
      "Запись из истории",
      [
        `Совпадений: ${record.matchesCount || 0}, вариантов референта: ${record.referenceOptionsCount || 0}`,
      ],
      "История"
    );
    if (record.selectedReferenceDrug) {
      const msg = createMessage("assistant", "Выбранный референт", "История");
      if (record.runId) msg.dataset.runId = record.runId;
      const body = msg.querySelector(".message-body");
      body.append(p(record.selectedReferenceDrug));
      body.append(kvGrid([
        ["JSON выбора", record.selectionJsonPath || "—"],
        ["Файл ответа", record.routerOutputPath || "—"],
      ]));
      if (record.analysisText) {
        const parsedTable = parseMarkdownTable(record.analysisText);
        if (parsedTable) {
          body.append(buildAnalysisTable(parsedTable));
        }
        const d = document.createElement("details");
        d.className = "text-block";
        const s = document.createElement("summary");
        s.textContent = "Ответ `test_router.py`";
        d.append(s, el("pre", "mono-block", record.analysisText));
        body.append(d);
      } else if (record.analysisText === "") {
        const d = document.createElement("details");
        d.className = "text-block";
        const s = document.createElement("summary");
        s.textContent = "Ответ `test_router.py`";
        d.append(s, el("p", null, "Ответ пустой (0 символов)."));
        body.append(d);
      } else if (record.analysisPreview) {
        const d = document.createElement("details");
        d.className = "text-block";
        const s = document.createElement("summary");
        s.textContent = "Фрагмент ответа `test_router.py`";
        d.append(s, el("pre", "mono-block", record.analysisPreview));
        body.append(d);
      }
      if (record.selectionRows && Array.isArray(record.selectionRows)) {
        body.append(buildReferenceTable(record.selectionRows));
      }
      if (record.runId) {
        const block = buildSynopsisSection(record.runId, record);
        body.append(block);
        hydrateSynopsisSection(block, record.runId);
      }
      appendDeleteRunAction(body, record.runId);
      mountMessage(msg);
    }
    maybeShowRunningForRecord(record);
    updateComposerVisibility();
  }

  function setSearchPending(flag) {
    state.searchPending = flag;
    dom.searchBtn.disabled = flag;
    dom.searchBtn.classList.toggle("is-loading", flag);
    updateComposerVisibility();
  }

  function setActionPending(flag) {
    state.actionPending = flag;
  }

  function setComposerVisible(show) {
    if (!dom.composerShell) return;
    dom.composerShell.classList.toggle("is-hidden", !show);
  }

  function updateComposerVisibility() {
    const hasMessages = dom.timeline && dom.timeline.childElementCount > 0;
    const show = !hasMessages && !state.historyView;
    setComposerVisible(show);
  }

  function getActiveRecord() {
    return state.history.find((h) => h.id === state.activeHistoryId) || null;
  }

  function getRecordByRunId(runId) {
    if (!runId) return null;
    return state.history.find((h) => h.runId === runId) || null;
  }

  function isRunRelatedToRecord(run, record) {
    if (!run || !record) return false;
    if (record.runId && run.id === record.runId) return true;
    if (record.sessionId && run.session_id && record.sessionId === run.session_id) return true;
    return false;
  }

  function getRunningRunIdForRecord(record) {
    if (!record) return null;
    if (record.runId && state.runningRunIds.has(record.runId)) return record.runId;
    if (record.sessionId && state.runningBySession.has(record.sessionId)) return state.runningBySession.get(record.sessionId) || null;
    return null;
  }

  function maybeShowRunningForRecord(record) {
    const runId = getRunningRunIdForRecord(record);
    const localPending = Boolean(record && record.sessionId && state.pendingSessions.has(record.sessionId));
    if (!runId && !localPending) return;
    const pendingKey = runId || `session:${record.sessionId}`;
    if (state.pendingRunId === pendingKey) return;
    if (state.pendingMessage) removeNode(state.pendingMessage);
    state.pendingRunId = pendingKey;
    state.pendingMessage = appendAnalysisInline("Продолжаем анализ `test_router.py`...");
    if (runId) {
      pollRun(runId);
    }
  }

  async function deleteRun(btn) {
    const runId = btn.dataset.runId;
    if (!runId) return;
    try {
      await api("/runs/delete", "POST", { run_id: runId }, 120000);
      const msg = btn.closest(".message");
      if (msg) msg.remove();
      removeHistoryByRunId(runId);
    } catch (err) {
      appendError(err.message, err.payload);
    }
  }

  function removeHistoryByRunId(runId) {
    const idx = state.history.findIndex((h) => h.runId === runId);
    if (idx === -1) return;
    state.history.splice(idx, 1);
    if (state.activeHistoryId && state.history.every((h) => h.id !== state.activeHistoryId)) {
      state.activeHistoryId = null;
    }
    saveHistory();
    renderHistory();
  }

  function appendDeleteRunAction(body, runId) {
    if (!runId) return;
    const row = el("div", "message-actions-row");
    const del = el("button", "ghost-btn", "Удалить блок");
    del.type = "button";
    del.dataset.action = "delete-run";
    del.dataset.runId = runId;
    row.append(del);
    body.append(row);
  }

  function buildSynopsisSection(runId, record) {
    const wrapper = el("div", "message-actions-row");
    wrapper.dataset.role = "synopsis-block";
    wrapper.dataset.runId = runId;

    const btn = el("button", "primary-btn", "Создать синопсис (.docx)");
    btn.type = "button";
    btn.dataset.action = "build-synopsis";
    btn.dataset.runId = runId;

    const status = el("span", "note-chip", "Готово к запуску");
    status.dataset.role = "synopsis-status";

    wrapper.append(btn, status);

    if (record && record.synopsisDocxUrl) {
      btn.disabled = true;
      status.textContent = "Синопсис готов";
      const link = el("a", "note-chip", "Скачать .docx");
      link.href = record.synopsisDocxUrl;
      link.target = "_blank";
      link.rel = "noopener";
      wrapper.append(link);
    }
    return wrapper;
  }

  async function hydrateSynopsisSection(wrapper, runId) {
    if (!wrapper || !runId) return;
    try {
      const data = await api(`/synopsis/get?run_id=${encodeURIComponent(runId)}`, "GET", null, 120000);
      const synopsis = data.synopsis;
      if (!synopsis) return;
      updateHistoryWithSynopsis(runId, synopsis);

      const btn = wrapper.querySelector('[data-action="build-synopsis"]');
      const status = wrapper.querySelector('[data-role="synopsis-status"]');
      if (status) status.textContent = synopsis.status === "done" ? "Синопсис готов" : "Синопсис в работе";
      if (synopsis.download_url) {
        if (btn) btn.disabled = true;
        let link = wrapper.querySelector('a.note-chip');
        if (!link) {
          link = el("a", "note-chip", "Скачать .docx");
          wrapper.append(link);
        }
        link.href = synopsis.download_url;
        link.target = "_blank";
        link.rel = "noopener";
      }
    } catch (_) {
      // ignore
    }
  }

  async function deleteHistoryItem(btn) {
    const runId = btn.dataset.runId;
    const historyId = btn.dataset.historyId;
    if (!runId) return;
    try {
      await api("/runs/delete", "POST", { run_id: runId }, 120000);
      if (historyId) {
        const idx = state.history.findIndex((h) => h.id === historyId);
        if (idx !== -1) state.history.splice(idx, 1);
      } else {
        removeHistoryByRunId(runId);
        return;
      }
      if (state.activeHistoryId && state.history.every((h) => h.id !== state.activeHistoryId)) {
        state.activeHistoryId = null;
      }
      saveHistory();
      renderHistory();
    } catch (err) {
      appendError(err.message, err.payload);
    }
  }

  async function buildSynopsis(btn) {
    const runId = btn.dataset.runId;
    if (!runId) return;
    const row = btn.closest(".message-actions-row") || btn.parentElement;
    const status = row ? row.querySelector('[data-role="synopsis-status"]') : null;
    let done = false;
    try {
      btn.disabled = true;
      if (status) status.textContent = "Создаю синопсис...";
      const payload = { run_id: runId };
      const result = await api("/synopsis/build", "POST", payload, 600000);
      if (status) status.textContent = "Синопсис готов";
      updateHistoryWithSynopsis(runId, {
        id: result.synopsis_run_id || null,
        status: "done",
        download_url: result.download_url || null,
      });
      done = true;

      const link = el("a", "note-chip", "Скачать .docx");
      link.href = result.download_url;
      link.target = "_blank";
      link.rel = "noopener";
      if (row) row.append(link);
    } catch (err) {
      if (status) status.textContent = "Ошибка генерации";
      appendError(err.message, err.payload);
    } finally {
      btn.disabled = done;
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();

