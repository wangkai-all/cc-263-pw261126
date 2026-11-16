/* SandboxLab UI.  Vanilla JS on purpose: no build step, no CDN, no telemetry. */
"use strict";

const $ = (sel, root = document) => root.querySelector(sel);
const el = (tag, attrs = {}, ...children) => {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value === undefined || value === null || value === false) continue;
    if (key === "class") node.className = value;
    else if (key === "dataset") Object.assign(node.dataset, value);
    else if (key.startsWith("on") && typeof value === "function") {
      node.addEventListener(key.slice(2).toLowerCase(), value);
    } else if (key === "html") node.innerHTML = value;
    else node.setAttribute(key, value);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
};

const state = {
  upstream: null,
  sandboxes: [],
  operations: [],
  current: null,
  view: null,
  query: "",
  region: "",
  sort: "row_id",
  direction: "asc",
  includeDeleted: false,
  asOf: null,
  busy: false,
};

const fmtInt = (n) => (n === null || n === undefined ? "—" : Number(n).toLocaleString("zh-CN"));
const fmtBytes = (n) => {
  if (!n) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let i = 0;
  let v = Number(n);
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
  return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
};
const money = (cents) =>
  cents === null || cents === undefined
    ? "—"
    : "¥" + (Number(cents) / 100).toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const shortId = (id) => (id ? String(id).slice(0, 10) : "—");
const timeShort = (iso) => (iso ? String(iso).replace("T", " ").replace(/\.\d+Z$/, "Z") : "—");

function toast(message, kind = "") {
  const node = el("div", { class: `toast ${kind}` }, message);
  $("#toast-root").append(node);
  setTimeout(() => node.remove(), 5200);
}

function setStatus(text, detail = "") {
  $("#status-text").textContent = text;
  $("#status-detail").textContent = detail;
}

async function api(path, options = {}) {
  const opts = { headers: { "Content-Type": "application/json" }, ...options };
  const response = await fetch(path, opts);
  const isJson = (response.headers.get("content-type") || "").includes("application/json");
  const payload = isJson ? await response.json() : await response.text();
  if (!response.ok) {
    const error = new Error(payload?.error?.message || `请求失败 (${response.status})`);
    error.code = payload?.error?.code;
    error.details = payload?.error?.details;
    error.status = response.status;
    throw error;
  }
  return payload;
}

async function withBusy(label, fn) {
  if (state.busy) return;
  state.busy = true;
  setStatus(label);
  try {
    return await fn();
  } catch (error) {
    toast(error.message, "error");
    if (error.code === "refresh_conflicts" && error.details) {
      state.current = error.details.sandbox_id || state.current;
      await loadState().catch(() => {});
    }
    return null;
  } finally {
    state.busy = false;
    setStatus("就绪");
  }
}

/* ------------------------------------------------------------------ refresh */

async function loadState({ keepSelection = true } = {}) {
  const payload = await api("/api/state");
  state.upstream = payload.upstream;
  state.sandboxes = payload.sandboxes;
  state.operations = payload.operations;
  if (!keepSelection || (state.current && !state.sandboxes.some((s) => s.sandbox_id === state.current))) {
    state.current = null;
  }
  renderSidebar();
  if (state.current) await loadState();
  else renderWorkspace();
}

async function refreshWorkspace() {
  if (!state.current) return;
  const params = new URLSearchParams();
  if (state.query) params.set("query", state.query);
  if (state.region) params.set("region", state.region);
  if (state.sort) params.set("sort", state.sort);
  if (state.direction) params.set("direction", state.direction);
  if (state.includeDeleted) params.set("include_deleted", "1");
  if (state.asOf !== null) params.set("as_of", String(state.asOf));
  const [detail, view, ops] = await Promise.all([
    api(`/api/sandboxes/${encodeURIComponent(state.current)}`),
    api(`/api/sandboxes/${encodeURIComponent(state.current)}/rows?${params}`),
    api(`/api/operations?limit=40&sandbox_id=${encodeURIComponent(state.current)}`),
  ]);
  state.detail = detail;
  state.view = view;
  state.operations = ops.operations;
  renderWorkspace();
  renderSidebar();
}

/* ------------------------------------------------------------------ sidebar */

function renderSidebar() {
  const list = $("#sandbox-list");
  list.replaceChildren();
  $("#sandbox-count").textContent = String(state.sandboxes.filter((s) => s.status === "active").length);
  const up = state.upstream || {};
  $("#upstream-rev").textContent = `rev ${up.source_revision ?? "—"}`;
  $("#upstream-summary").replaceChildren(
    el("dt", {}, "上游记录"), el("dd", {}, fmtInt(up.row_count)),
    el("dt", {}, "共享块"), el("dd", {}, fmtInt((up.segments || []).filter((s) => !s.pruned_at).length)),
    el("dt", {}, "共享块占用"), el("dd", {}, fmtBytes(up.segment_bytes)),
    el("dt", {}, "沙箱占用"), el("dd", {}, fmtBytes(up.sandbox_bytes)),
    el("dt", {}, "已删除源记录"), el("dd", {}, fmtInt(up.erased_count)),
    el("dt", {}, "活跃沙箱"), el("dd", {}, fmtInt(up.active_sandboxes)),
  );
  $("#policy-note").textContent =
    `共享块策略：每次上游变更只新增一个内容寻址块；沙箱仅保存被改动的行。` +
    `回看窗口保留 ${up.history_retention ?? "—"} 个版本，删除记录会清空其历史内容——两者不能同时无限承诺。`;

  if (!state.sandboxes.length) {
    list.append(el("li", { class: "muted small" }, "还没有沙箱。先克隆一个开始实验。"));
    return;
  }
  for (const sandbox of state.sandboxes) {
    const active = sandbox.sandbox_id === state.current;
    const destroyed = sandbox.status === "destroyed";
    const item = el(
      "li",
      {
        class: `sandbox-item${active ? " active" : ""}${destroyed ? " is-destroyed" : ""}`,
        onclick: () => {
          if (destroyed) { toast("该沙箱已销毁，仅保留审计记录。", "error"); return; }
          state.current = sandbox.sandbox_id;
          state.asOf = null;
          refreshWorkspace().catch((e) => toast(e.message, "error"));
        },
      },
      el(
        "div",
        { class: "row1" },
        el("span", { class: "name" }, sandbox.label),
        destroyed
          ? el("span", { class: "pill danger" }, "已销毁")
          : sandbox.pending_refresh
          ? el("span", { class: "pill warn" }, "待决冲突")
          : el("span", { class: "pill ok" }, "活跃"),
      ),
      el(
        "div",
        { class: "meta" },
        destroyed
          ? el("span", {}, `销毁于 ${timeShort(sandbox.destroyed_at)}`)
          : el("span", {}, `改 ${fmtInt(sandbox.local_writes)} / 删 ${fmtInt(sandbox.local_deletes)}`),
        !destroyed && sandbox.disk_bytes !== undefined ? el("span", {}, fmtBytes(sandbox.disk_bytes)) : null,
        !destroyed && sandbox.open_conflicts ? el("span", {}, `冲突 ${sandbox.open_conflicts}`) : null,
      ),
    );
    list.append(item);
  }
}

/* ---------------------------------------------------------------- workspace */

function renderWorkspace() {
  const root = $("#workspace");
  root.replaceChildren();
  if (!state.current || !state.detail) {
    root.append(
      el(
        "div",
        { class: "empty-state" },
        el("h2", {}, "选择一个沙箱"),
        el("p", {}, "克隆是廉价的：新沙箱只引用当前共享块，不复制数据。"),
        el("p", { class: "small muted" }, "在上游点一次「模拟上游变更」，再对两个沙箱分别刷新，可以看到它们各自固定在自己的基底上。"),
      ),
    );
    return;
  }
  const d = state.detail;
  const v = state.view;
  root.append(
    el(
      "div",
      { class: "sandbox-view" },
      hero(d),
      refreshBanner(d),
      elsView(d, v),
      el(
        "div",
        { class: "split" },
        el("div", {}, exportsCard(d), historyCard(d)),
        el("div", {}, operationsCard()),
      ),
    ),
  );
}

function hero(d) {
  return el(
    "div",
    { class: "hero" },
    el(
      "div",
      { class: "hero-top" },
      el(
        "div",
        {},
        el("h2", {}, d.label),
        el("div", { class: "sub mono" }, `${d.sandbox_id} · 创建于 ${timeShort(d.created_at)} · 当前版本 seq ${d.seq}`),
      ),
      el(
        "div",
        { class: "hero-actions" },
        el("button", { class: "btn", onclick: openRefreshPreview }, "刷新预览"),
        el("button", { class: "btn primary", onclick: doRefresh }, "刷新到上游"),
        el("button", { class: "btn danger-ghost", onclick: openDestroy }, "销毁沙箱"),
      ),
    ),
    el(
      "div",
      { class: "stat-grid" },
      stat("基底块", shortId(d.base_segment), `${fmtInt(d.base_row_count)} 行 · ${d.base_shared_refs} 个沙箱共享`),
      stat("本地改动", fmtInt(d.local_writes), "仅覆盖层计费"),
      stat("本地删除", fmtInt(d.local_deletes), "可恢复，除非源记录被删"),
      stat("沙箱磁盘", fmtBytes(d.disk_bytes), `共享块 ${fmtBytes(d.base_bytes)}`),
      stat("待决冲突", fmtInt(d.open_conflicts), d.pending_refresh ? "刷新未推进" : "无"),
      stat("回看窗口", `${d.history_horizon} → ${d.seq}`, `保留 ${d.history_retention} 个版本`),
    ),
  );
}

const stat = (label, value, hint) =>
  el(
    "div",
    { class: "stat" },
    el("div", { class: "label" }, label),
    el("div", { class: "value" }, value),
    hint ? el("div", { class: "hint" }, hint) : null,
  );

function refreshBanner(d) {
  if (d.pending_refresh || d.open_conflicts) {
    return el(
      "div",
      { class: "banner danger" },
      el(
        "div",
        {},
        el("strong", {}, "刷新停在冲突上，基底没有推进。 "),
        el(
          "span",
          {},
          `本沙箱改了 ${fmtInt(d.local_writes)} 行，其中 ${fmtInt(d.open_conflicts)} 行上游也变了。逐条决定保留本地还是采用上游后，才能应用新的共享块。`,
        ),
      ),
      el("button", { class: "btn sm", onclick: openConflicts }, "处理冲突"),
    );
  }
  const current = state.upstream?.current_segment_id;
  if (current && current !== d.base_segment) {
    return el(
      "div",
      { class: "banner info" },
      el("div", {}, el("strong", {}, "上游有新版本。 "), el("span", {}, "这个沙箱仍固定在自己的旧共享块上，刷新才会导入变更。")),
      el("button", { class: "btn sm", onclick: doRefresh }, "刷新"),
    );
  }
  return null;
}

function visibleColumns(d) {
  return d.columns.filter((c) => c.visible);
}

function elsView(d, v) {
  if (!v) return el("div", { class: "muted" }, "正在加载…");
  const columns = visibleColumns(d);
  const table = el("table", {});
  table.append(
    el(
      "thead",
      {},
      el(
        "tr",
        {},
        columns.map((c) =>
          el(
            "th",
            {
              style: "cursor:pointer",
              title: "点击按此列排序",
              onclick: () => {
                if (state.sort === c.key) state.direction = state.direction === "asc" ? "desc" : "asc";
                else { state.sort = c.key; state.direction = "asc"; }
                refreshWorkspace().catch((e) => toast(e.message, "error"));
              },
            },
            c.label + (state.sort === c.key ? (state.direction === "asc" ? " ▲" : " ▼") : ""),
          ),
        ),
        el("th", {}, "操作"),
      ),
    ),
  );
  const tbody = el("tbody", {});
  if (!v.rows.length) {
    tbody.append(el("tr", {}, el("td", { class: "empty-row", colspan: columns.length + 1 }, "没有符合条件的行。")));
  }
  for (const row of v.rows) {
    const isErased = v.erased_hidden?.includes(row.row_id);
    const deleted = Boolean(row._local_deleted);
    tbody.append(
      el(
        "tr",
        {},
        columns.map((c) => {
          const value = row[c.key];
          const attrs = { class: c.key === "amount_cents" ? "num" : "" };
          if (c.key === "amount_cents") attrs.class = "num";
          if (c.key === "row_id") {
            return el(
              "td",
              { class: "mono" },
              String(value ?? "—"),
              row._local ? el("span", { class: "tag local" }, "本地") : null,
              deleted ? el("span", { class: "tag deleted" }, "已删") : null,
              isErased ? el("span", { class: "tag erased" }, "源已删") : null,
            );
          }
          return el("td", attrs, c.key === "amount_cents" ? money(value) : String(value ?? "—"));
        }),
        el(
          "td",
          { class: "actions" },
          deleted
            ? el("button", { class: "btn sm", onclick: () => restore(row.row_id) }, "恢复")
            : el(
                "button",
                { class: "btn sm", onclick: () => openEdit(row, columns) },
                "编辑",
              ),
          " ",
          deleted
            ? null
            : el("button", { class: "btn sm danger-ghost", onclick: () => softDelete(row.row_id) }, "删除"),
        ),
      ),
    );
  }
  table.append(tbody);
  return el(
    "div",
    { class: "card" },
    el(
      "div",
      { class: "section-title" },
      el(
        "div",
        { class: "toolbar" },
        el("h3", {}, `数据视图 · ${fmtInt(v.total)} 行`),
        v.cache === "hit" ? el("span", { class: "pill ok" }, "缓存命中") : null,
        v.as_of !== null && v.as_of !== undefined
          ? el("span", { class: "pill warn" }, `回看 seq ${v.as_of}`)
          : null,
        v.erased_hidden?.length
          ? el("span", { class: "pill danger" }, `隐藏已删除 ${v.erased_hidden.length} 条`)
          : null,
      ),
      el(
        "div",
        { class: "toolbar" },
        el("button", { class: "btn sm", onclick: openExport }, "导出 CSV"),
        el("button", { class: "btn sm", onclick: openColumns }, "列设置"),
      ),
    ),
    el(
      "div",
      { class: "toolbar", style: "margin-bottom:10px" },
      el("input", {
        class: "grow",
        placeholder: "搜索编号 / 名称 / 邮箱 / 备注",
        value: state.query,
        oninput: debounce((e) => { state.query = e.target.value; refreshWorkspace().catch(() => {}); }, 250),
      }),
      el(
        "select",
        {
          onchange: (e) => { state.region = e.target.value; refreshWorkspace().catch(() => {}); },
        },
        el("option", { value: "" }, "全部区域"),
        (v.regions || []).map((r) => el("option", { value: r, selected: state.region === r }, r)),
      ),
      el(
        "label",
        { class: "checkline" },
        el("input", {
          type: "checkbox",
          checked: state.includeDeleted,
          onchange: (e) => { state.includeDeleted = e.target.checked; refreshWorkspace().catch(() => {}); },
        }),
        "显示本地已删行",
      ),
      historyControl(v),
    ),
    el("div", { class: "table-wrap" }, table),
    el(
      "p",
      { class: "small muted", style: "margin-top:8px" },
      v.as_of === null || v.as_of === undefined
        ? "当前视图 = 共享块 + 本地覆盖层。"
        : `回看 seq ${v.as_of}：用保留的历史重建当时的覆盖层（窗口起点 ${v.history_horizon}）。窗口之前的状态已压缩，不提供。`,
    ),
  );
}

function historyControl(v) {
  if (!v || v.current_seq === 0) return null;
  const value = state.asOf === null ? v.current_seq : Math.min(state.asOf, v.current_seq);
  return el(
    "div",
    { class: "history-bar", style: "flex:1;min-width:220px" },
    el("span", { class: "small muted" }, "回看"),
    el("input", {
      type: "range",
      min: 0,
      max: v.current_seq,
      value,
      oninput: (e) => {
        const next = Number(e.target.value);
        state.asOf = next === v.current_seq ? null : next;
        refreshWorkspace().catch(() => {});
      },
    }),
    el("span", { class: "mono small" }, value === v.current_seq ? "最新" : `seq ${value}`),
    state.asOf !== null
      ? el("button", { class: "btn sm", onclick: () => { state.asOf = null; refreshWorkspace().catch(() => {}); } }, "回到最新")
      : null,
  );
}

const debounce = (fn, ms) => {
  let timer = null;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
};

function exportsCard(d) {
  const card = el("div", { class: "card" }, el("h3", {}, "导出"));
  if (!d.exports.length) {
    card.append(el("p", { class: "muted small" }, "还没有导出。导出的 CSV 是真实文件；源记录被删时会被吊销并删除。"));
    return card;
  }
  const list = el("div", { class: "list" });
  for (const item of d.exports) {
    const revoked = item.status === "revoked";
    list.append(
      el(
        "div",
        { class: "list-item" },
        el(
          "div",
          { class: "row1" },
          el("span", { class: "mono" }, item.export_id),
          revoked
            ? el("span", { class: "pill danger" }, "已吊销")
            : el("span", { class: "pill ok" }, "有效"),
        ),
        el(
          "div",
          { class: "muted small", style: "margin-top:4px" },
          `${fmtInt(item.row_count)} 行 · ${timeShort(item.created_at)}`,
          revoked ? ` · ${item.revoke_reason || "已吊销"}` : "",
        ),
        revoked
          ? null
          : el("a", { class: "btn sm", style: "margin-top:6px", href: `/api/exports/${item.export_id}/download` }, "下载"),
      ),
    );
  }
  card.append(list);
  return card;
}

function historyCard(d) {
  const card = el("div", { class: "card" }, el("h3", {}, "基底历史"));
  const list = el("div", { class: "list" });
  for (const entry of [...(d.base_history || [])].reverse()) {
    list.append(
      el(
        "div",
        { class: "list-item" },
        el(
          "div",
          { class: "row1" },
          el("span", { class: "mono" }, `seq ${entry.seq} · ${shortId(entry.segment_id)}`),
          el("span", { class: "muted small" }, entry.note || ""),
        ),
        el("div", { class: "muted small" }, timeShort(entry.created_at)),
      ),
    );
  }
  card.append(list);
  return card;
}

function operationsCard() {
  const card = el("div", { class: "card" }, el("h3", {}, "最近操作"));
  const list = el("div", { class: "list" });
  if (!state.operations.length) {
    card.append(el("p", { class: "muted small" }, "暂无操作。"));
    return card;
  }
  for (const op of state.operations.slice(0, 12)) {
    list.append(
      el(
        "div",
        { class: "list-item" },
        el(
          "div",
          { class: "row1" },
          el("span", {}, `${op.kind}`),
          el("span", { class: `pill ${op.status === "ok" ? "ok" : op.status === "conflict" ? "warn" : ""}` }, op.status),
        ),
        el("div", { class: "muted small", style: "margin-top:3px" },
          `${timeShort(op.started_at)}${op.sandbox_id ? " · " + op.sandbox_id : ""}`),
      ),
    );
  }
  card.append(list);
  return card;
}

/* ------------------------------------------------------------------- modals */

function openModal({ title, subtitle, body, actions, onClose }) {
  const root = $("#modal-root");
  const close = () => {
    root.replaceChildren();
    if (onClose) onClose();
  };
  const backdrop = el(
    "div",
    { class: "modal-backdrop", onclick: (e) => { if (e.target === backdrop) close(); } },
    el(
      "div",
      { class: "modal", role: "dialog", "aria-modal": "true" },
      el("h2", {}, title),
      subtitle ? el("p", { class: "muted small" }, subtitle) : null,
      body,
      el("div", { class: "modal-actions" }, el("button", { class: "btn", onclick: close }, "取消"), actions),
    ),
  );
  root.replaceChildren(backdrop);
  return { close };
}

function openEdit(row, columns) {
  const fields = {};
  const body = el("div", {});
  for (const column of columns) {
    if (!["name", "email", "region", "amount_cents", "notes"].includes(column.key)) continue;
    const input =
      column.key === "notes"
        ? el("textarea", { value: row[column.key] ?? "" })
        : el("input", {
            value: column.key === "amount_cents" ? (row[column.key] ?? 0) / 100 : row[column.key] ?? "",
            type: column.key === "amount_cents" ? "number" : "text",
            step: column.key === "amount_cents" ? "0.01" : undefined,
          });
    fields[column.key] = input;
    body.append(
      el(
        "div",
        { class: "field" },
        el("label", {}, column.label + (column.key === "amount_cents" ? "（元）" : "")),
        input,
      ),
    );
  }
  openModal({
    title: `编辑 ${row.row_id}`,
    subtitle: "改动只写入本沙箱的覆盖层，不会影响其它沙箱或上游。",
    body,
    actions: el(
      "button",
      {
        class: "btn primary",
        onclick: async () => {
          const payload = {};
          if (fields.name) payload.name = fields.name.value.trim();
          if (fields.email) payload.email = fields.email.value.trim();
          if (fields.region) payload.region = fields.region.value.trim();
          if (fields.notes) payload.notes = fields.notes.value;
          if (fields.amount_cents) payload.amount_cents = Math.round(Number(fields.amount_cents.value) * 100);
          const ok = await withBusy("写入本地覆盖层…", () =>
            api(`/api/sandboxes/${state.current}/rows/${row.row_id}`, {
              method: "PATCH",
              body: JSON.stringify(payload),
            }),
          );
          if (ok) {
            closeAll();
            toast("已写入本地覆盖层。", "ok");
            await loadState();
          }
        },
      },
      "保存",
    ),
  });
}

function closeAll() { $("#modal-root").replaceChildren(); }

async function softDelete(rowId) {
  if (!confirm(`在沙箱内删除 ${rowId}？\n\n这只影响当前沙箱，可以“恢复”。若源记录随后被要求删除，这个本地删除与历史也会一并清理。`)) return;
  const ok = await withBusy("本地删除…", () =>
    api(`/api/sandboxes/${state.current}/rows/${rowId}`, { method: "DELETE" }),
  );
  if (ok) { toast("已在本沙箱删除。", "ok"); await loadState(); }
}

async function restore(rowId) {
  const ok = await withBusy("恢复行…", () =>
    api(`/api/sandboxes/${state.current}/rows/${rowId}/restore`, { method: "POST", body: "{}" }),
  );
  if (ok) { toast("已恢复本沙箱的本地版本。", "ok"); await loadState(); }
}

function openColumns() {
  const d = state.detail;
  const body = el("div", {});
  const inputs = {};
  for (const column of d.columns) {
    if (column.key === "row_id") continue;
    const label = el("input", { value: column.label });
    const visible = el("input", { type: "checkbox", checked: column.visible });
    inputs[column.key] = { label, visible };
    body.append(
      el(
        "div",
        { class: "field", style: "flex-direction:row;align-items:center;gap:10px" },
        visible,
        el("div", { style: "flex:1" }, label),
        el("span", { class: "muted small mono" }, column.key),
      ),
    );
  }
  openModal({
    title: "列设置（仅本沙箱）",
    subtitle: "共享块的原始列不会被修改；这里是覆盖层的本地映射：改名或隐藏只对本沙箱生效。",
    body,
    actions: el(
      "button",
      {
        class: "btn primary",
        onclick: async () => {
          const columns = Object.entries(inputs).map(([key, value]) => ({
            key,
            label: value.label.value.trim(),
            visible: value.visible.checked,
          }));
          const ok = await withBusy("保存列设置…", () =>
            api(`/api/sandboxes/${state.current}/columns`, {
              method: "PATCH",
              body: JSON.stringify({ columns }),
            }),
          );
          if (ok) { closeAll(); await loadState(); }
        },
      },
      "保存",
    ),
  });
}

function openExport() {
  const body = el(
    "div",
    {},
    el(
      "p",
      { class: "small muted" },
      `将把当前视图（${state.query ? `搜索“${state.query}” · ` : ""}${state.region || "全部区域"}）导出为 CSV 文件。导出会被登记；若其中任何一行后来被要求删除，整份导出都会被吊销并删除文件。`,
    ),
  );
  openModal({
    title: "导出当前视图",
    body,
    actions: el(
      "button",
      {
        class: "btn primary",
        onclick: async () => {
          const result = await withBusy("生成导出…", () =>
            api(`/api/sandboxes/${state.current}/exports`, {
              method: "POST",
              body: JSON.stringify({
                query: state.query,
                region: state.region,
                include_deleted: state.includeDeleted,
              }),
            }),
          );
          if (result) {
            closeAll();
            toast(`已生成 ${result.row_count} 行导出。`, "ok");
            await loadState();
          }
        },
      },
      "生成",
    ),
  });
}

async function openRefreshPreview() {
  const preview = await withBusy("计算刷新预览…", () =>
    api(`/api/sandboxes/${state.current}/refresh/preview`),
  );
  if (!preview) return;
  const stats = preview.stats || {};
  const body = el(
    "div",
    {},
    el(
      "div",
      { class: "stat-grid" },
      stat("新增", fmtInt(stats.added), "上游有、基底没有"),
      stat("更新", fmtInt(stats.updated), "上游改了"),
      stat("移除", fmtInt(stats.removed), "上游没有此行"),
      stat("冲突", fmtInt(preview.conflicts.length), "本地也改过"),
    ),
    preview.up_to_date
      ? el("p", { class: "small muted", style: "margin-top:12px" }, "已经固定在最新共享块上。")
      : null,
    preview.conflicts.length
      ? el(
          "p",
          { class: "small", style: "margin-top:12px" },
          "刷新会停在冲突上：本地改动不会被静默覆盖，必须先逐条决定。",
        )
      : el("p", { class: "small muted", style: "margin-top:12px" }, "没有冲突；刷新会直接推进基底。"),
    preview.changes.length
      ? el(
          "div",
          { class: "list", style: "margin-top:10px" },
          preview.changes.slice(0, 8).map((change) =>
            el(
              "div",
              { class: "list-item" },
              el(
                "div",
                { class: "row1" },
                el("span", { class: "mono" }, change.row_id),
                change.locally_overlaid
                  ? el("span", { class: "pill warn" }, "本地已改")
                  : el("span", { class: "pill" }, change.after ? (change.before ? "更新" : "新增") : "移除"),
              ),
              el(
                "div",
                { class: "muted small mono", style: "margin-top:4px" },
                change.before ? `${shortId(change.before.name)} · ${money(change.before.amount_cents)}` : "∅",
                " → ",
                change.after ? `${shortId(change.after.name)} · ${money(change.after.amount_cents)}` : "∅",
              ),
            ),
          ),
        )
      : null,
  );
  openModal({
    title: "刷新预览",
    subtitle: `固定基底 ${shortId(preview.current_segment)} → 上游 ${shortId(preview.candidate_segment)}`,
    body,
    actions: preview.up_to_date
      ? null
      : el(
          "button",
          { class: "btn primary", onclick: async () => { closeAll(); await doRefresh(); } },
          preview.conflicts.length ? "继续（将停在冲突）" : "应用刷新",
        ),
  });
}

async function doRefresh() {
  const result = await withBusy("刷新沙箱…", () =>
    api(`/api/sandboxes/${state.current}/refresh`, { method: "POST", body: "{}" }),
  );
  if (!result) return;
  if (result.status === "up_to_date") {
    toast("已经是最新共享块。", "ok");
  } else {
    toast(`已刷新：新增 ${result.stats.added}，更新 ${result.stats.updated}，移除 ${result.stats.removed}。本地改动未覆盖。`, "ok");
  }
  await loadState();
}

async function openConflicts() {
  const payload = await withBusy("读取冲突…", () =>
    api(`/api/sandboxes/${state.current}/conflicts`),
  );
  if (!payload) return;
  const list = el("div", { class: "list" });
  for (const conflict of payload.conflicts) {
    const open = conflict.status === "open";
    list.append(
      el(
        "div",
        { class: "list-item" },
        el(
          "div",
          { class: "row1" },
          el("span", { class: "mono" }, conflict.row_id),
          el("span", { class: `pill ${open ? "warn" : "ok"}` }, conflict.status),
        ),
        el(
          "div",
          { class: "mono small", style: "margin-top:6px;display:grid;gap:2px" },
          el("span", {}, conflict.local_tombstone ? "本地：已删除" : `本地：${shortId(conflict.local_payload?.name)} · ${money(conflict.local_payload?.amount_cents)} · ${conflict.local_payload?.notes ?? ""}`),
          el("span", {}, conflict.upstream_payload ? `上游：${shortId(conflict.upstream_payload?.name)} · ${money(conflict.upstream_payload?.amount_cents)} · ${conflict.upstream_payload?.notes ?? ""}` : "上游：该行已被移除"),
        ),
        open
          ? el(
              "div",
              { class: "toolbar", style: "margin-top:8px" },
              el(
                "button",
                { class: "btn sm", onclick: () => resolveConflict(conflict.row_id, "keep") },
                "保留本地",
              ),
              el(
                "button",
                { class: "btn sm", onclick: () => resolveConflict(conflict.row_id, "take") },
                "采用上游",
              ),
            )
          : null,
      ),
    );
  }
  openModal({
    title: "刷新冲突",
    subtitle: payload.pending
      ? `候选共享块 ${shortId(payload.pending.segment_id)} 已暂存，尚未推进。未决定前基底保持原样。`
      : "没有待处理的刷新。",
    body: list,
    actions: el(
      "button",
      {
        class: "btn primary",
        onclick: async () => {
          const result = await withBusy("应用刷新…", () =>
            api(`/api/sandboxes/${state.current}/refresh/apply`, { method: "POST", body: "{}" }),
          );
          if (result) { closeAll(); toast("冲突处理完毕，基底已推进。", "ok"); await loadState(); }
        },
      },
      "应用刷新",
    ),
    onClose: () => refreshWorkspace().catch(() => {}),
  });

  async function resolveConflict(rowId, resolution) {
    const ok = await withBusy("记录决定…", () =>
      api(`/api/sandboxes/${state.current}/conflicts/${rowId}`, {
        method: "POST",
        body: JSON.stringify({ resolution }),
      }),
    );
    if (ok) await openConflicts();
  }
}

function openDestroy() {
  const reason = el("input", { placeholder: "销毁原因（写入审计）" });
  const body = el(
    "div",
    {},
    el(
      "p",
      { class: "small muted" },
      "销毁会删除本沙箱的真实数据库文件，吊销并删除它的所有导出。共享基底不受影响，其它沙箱照常读取。此操作不可撤销。",
    ),
    el("div", { class: "field" }, el("label", {}, "原因"), reason),
  );
  openModal({
    title: `销毁 ${state.detail?.label || "沙箱"}？`,
    body,
    actions: el(
      "button",
      {
        class: "btn danger",
        onclick: async () => {
          const result = await withBusy("销毁沙箱…", () =>
            api(`/api/sandboxes/${state.current}`, {
              method: "DELETE",
              body: JSON.stringify({ reason: reason.value.trim() }),
            }),
          );
          if (result) {
            state.current = null;
            state.detail = null;
            state.view = null;
            closeAll();
            toast(`已销毁；删除 ${result.removed_files.length} 个文件，吊销 ${result.revoked_exports} 份导出。`, "ok");
            await loadState({ keepSelection: false });
          }
        },
      },
      "确认销毁",
    ),
  });
}

/* ------------------------------------------------------------- erase modal */

async function openEraseModal(preselect = "") {
  const rowInput = el("input", { placeholder: "例如 acct-007", value: preselect });
  const reason = el("textarea", { placeholder: "删除依据（4–300 字，会写入审计）" });
  const confirmText = el("input", { placeholder: "输入 DELETE 以确认" });
  const ack = el("input", { type: "checkbox" });
  const previewBox = el("div", { class: "muted small" }, "输入记录编号后可预览影响范围。");
  const previewBtn = el(
    "button",
    {
      class: "btn sm",
      type: "button",
      onclick: async () => {
        const rowId = rowInput.value.trim();
        if (!rowId) return;
        previewBox.textContent = "计算中…";
        try {
          const preview = await api(`/api/erasure/preview?row_id=${encodeURIComponent(rowId)}`);
          previewBox.replaceChildren(
            el("div", {}, `上游记录：${preview.in_upstream ? "存在" : "不存在"} · 共享块命中：${preview.segments}`),
            el(
              "div",
              {},
              `受影响沙箱：${preview.affected_sandboxes.length} 个`,
              preview.affected_sandboxes.length
                ? `（${preview.affected_sandboxes.map((s) => s.label).join("、")}）`
                : "",
            ),
            el("div", {}, `需吊销导出：${preview.exports.filter((e) => e.status === "valid").length} 份`),
            preview.already_erased ? el("div", { class: "diff-del" }, "该记录已删除过，重复操作只会补齐清理。") : null,
          );
        } catch (error) {
          previewBox.textContent = error.message;
        }
      },
    },
    "预览影响",
  );
  const body = el(
    "div",
    {},
    el(
      "p",
      { class: "small muted" },
      "这是对源记录的删除：上游行、所有共享块、所有沙箱的覆盖层、读缓存与导出都会被处理。历史回看在该记录上只返回“已删除”，不再返回内容——无法同时保证无限回看与彻底删除。",
    ),
    el("div", { class: "field" }, el("label", {}, "记录编号"), rowInput, previewBtn),
    el("div", { class: "field" }, el("label", {}, "影响预览"), previewBox),
    el("div", { class: "field" }, el("label", {}, "删除依据"), reason),
    el(
      "div",
      { class: "field" },
      el("label", {}, "确认"),
      confirmText,
      el("div", { class: "help" }, "必须输入大写 DELETE。删除立即执行，不可撤销。"),
    ),
    el(
      "div",
      { class: "field checkline" },
      ack,
      el("div", {}, "我确认该记录存在合规保留义务，并已获得批准（如无保留义务请留空）。"),
    ),
  );
  openModal({
    title: "删除源记录",
    subtitle: "全链路删除：共享块重写 + 沙箱清理 + 导出吊销",
    body,
    actions: el(
      "button",
      {
        class: "btn danger",
        onclick: async () => {
          const rowId = rowInput.value.trim();
          if (confirmText.value.trim() !== "DELETE") {
            toast("请输入 DELETE 以确认。", "error");
            return;
          }
          const result = await withBusy("执行删除…", () =>
            api("/api/upstream/erase", {
              method: "POST",
              body: JSON.stringify({
                row_id: rowId,
                reason: reason.value.trim(),
                acknowledge_hold: ack.checked,
              }),
            }),
          );
          if (result) {
            closeAll();
            await loadState();
            showEraseReport(result.report);
          }
        },
      },
      "执行删除",
    ),
  });
}

function showEraseReport(report) {
  const auditBox = el("div", { class: "list-item" }, el("div", { class: "muted small" }, "正在复核残留…"));
  const body = el(
    "div",
    {},
    el(
      "div",
      { class: "stat-grid" },
      stat("共享块重写", String(report.segments_rewritten?.length ?? 0), "旧文件已删除"),
      stat("沙箱清理", String(report.sandboxes_scrubbed?.length ?? 0), "覆盖层 / 历史 / 缓存"),
      stat("导出吊销", String(report.exports_revoked?.length ?? 0), "文件已删除"),
      stat("缓存刷新", String(report.cache_entries_flushed ?? 0), "跨沙箱读缓存"),
      stat("操作日志脱敏", String(report.operation_logs_redacted ?? 0), "去掉内容，保留审计"),
    ),
    el(
      "div",
      { class: "list", style: "margin-top:12px" },
      el(
        "div",
        { class: "list-item" },
        el("div", { class: "row1" }, el("span", {}, "审计摘要"), el("span", { class: "mono" }, report.row_id)),
        el("div", { class: "muted small", style: "margin-top:4px" }, `依据：${report.reason}`),
        el("div", { class: "muted small" }, `内容摘要（保留用于确认是哪条）：${shortId(report.payload_digest)}…`),
        el("div", { class: "muted small" }, `时间：${timeShort(report.erased_at)}`),
      ),
      report.sandboxes_unreachable?.length
        ? el(
            "div",
            { class: "list-item" },
            el("div", { class: "diff-del" }, "无法就地清理的沙箱"),
            ...report.sandboxes_unreachable.map((item) =>
              el("div", { class: "muted small" }, `${item.sandbox_id}：${item.why}`),
            ),
          )
        : null,
      el(
        "div",
        { class: "list-item" },
        el("div", { class: "row1" }, el("span", {}, "后续"), el("span", { class: "pill" }, "已完成")),
        el(
          "div",
          { class: "muted small", style: "margin-top:4px" },
          "刷新撞上该记录时会直接剔除；已删除的行无法通过任何沙箱的写入或恢复“复活”；中断的清理可用「恢复未完成删除」补齐。",
        ),
      ),
      auditBox,
    ),
    el("p", { class: "small muted", style: "margin-top:10px" },
      "说明：共享块按内容寻址，删除通过“替换链 + 删除旧文件”实现。固定旧块的沙箱读取时会解析到替换块，因此不需要逐个改沙箱文件。"),
  );
  openModal({ title: "删除完成", subtitle: "每一处仍可查询的副本都已处理", body });
  // The audit endpoint re-reads every derived store; do not just trust the
  // operation report that was written before compaction.
  api(`/api/erasures/${encodeURIComponent(report.row_id)}`)
    .then((audit) => {
      const residue = audit.sandbox_residue || [];
      auditBox.replaceChildren(
        el(
          "div",
          { class: "row1" },
          el("span", {}, "残留复核"),
          el(
            "span",
            { class: `pill ${audit.clean ? "ok" : "danger"}` },
            audit.clean ? "无残留" : "仍有残留",
          ),
        ),
        el(
          "div",
          { class: "muted small", style: "margin-top:4px" },
          audit.clean
            ? "覆盖层、历史（含压缩基线）、冲突载荷、读缓存、共享块与导出均已复核。"
            : "以下位置仍可读到该记录，可用「恢复未完成删除」补齐：",
        ),
        ...residue.map((item) =>
          el(
            "div",
            { class: "muted small diff-del" },
            `${item.sandbox_id}：${[
              item.local_rows ? `本地行 ${item.local_rows}` : "",
              item.tombstones ? `墓碑载荷 ${item.tombstones}` : "",
              item.history_payloads ? `历史 ${item.history_payloads}` : "",
              item.conflict_payloads ? `冲突 ${item.conflict_payloads}` : "",
              item.baseline_copies ? `压缩基线 ${item.baseline_copies}` : "",
              item.cache_copies ? `读缓存 ${item.cache_copies}` : "",
              item.state === "文件已不在" ? "文件已不在" : "",
            ].filter(Boolean).join(" · ")}`,
          ),
        ),
      );
    })
    .catch((error) => {
      auditBox.replaceChildren(el("div", { class: "muted small" }, `复核失败：${error.message}`));
    });
}

/* -------------------------------------------------------------------- boot */

function bindGlobal() {
  $("#clone-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const input = $("#clone-label");
    const label = input.value.trim();
    if (!label) return;
    const created = await withBusy("克隆沙箱…", () =>
      api("/api/sandboxes", { method: "POST", body: JSON.stringify({ label }) }),
    );
    if (created) {
      input.value = "";
      state.current = created.sandbox_id;
      state.asOf = null;
      toast(`已克隆 ${label}：引用共享块 ${shortId(created.base_segment)}，未复制数据。`, "ok");
      await loadState();
    }
  });

  $("#btn-simulate").addEventListener("click", async () => {
    const result = await withBusy("模拟上游变更…", () =>
      api("/api/upstream/simulate", { method: "POST", body: JSON.stringify({ ops: 4 }) }),
    );
    if (result) {
      toast(`上游 rev ${result.revision}：${result.changes.length} 处变更已写入新共享块。沙箱不会自动看到。`, "ok");
      await loadState();
    }
  });

  $("#btn-recover").addEventListener("click", async () => {
    const result = await withBusy("检查未完成删除…", () =>
      api("/api/recover", { method: "POST", body: "{}" }),
    );
    if (result) {
      toast(result.recovered ? `补齐了 ${result.recovered} 条未完成删除。` : "没有未完成的删除。", "ok");
      await loadState();
    }
  });

  $("#btn-erase").addEventListener("click", () => openEraseModal());
}

(async function boot() {
  bindGlobal();
  try {
    await loadState({ keepSelection: false });
    setStatus("就绪", "选择左侧沙箱，或先克隆一个。");
  } catch (error) {
    setStatus("加载失败", error.message);
    toast(error.message, "error");
  }
})();

/* ------------------------------------------------------- trade-off explainer */

function openPolicy() {
  const rows = [
    ["存储", "每个沙箱复制全部行：简单，但 N 个沙箱 = N 份数据，删除要在 N 个文件里逐个找。",
      "共享内容寻址块 + 每沙箱覆盖层：只存引用与改动行；删除重写一个块即可覆盖所有引用者。"],
    ["隔离", "天然隔离；但上游刷新与本地改动的关系仍然要自己处理。",
      "基底固定（pinned）：沙箱看不到上游新版本，直到显式刷新；互不影响的改动只落在自己的覆盖层。"],
    ["刷新撞上本地写入", "整表覆盖会静默丢掉本地改动。",
      "先做差量检测，把“本地也改过、上游也变了”的行列为冲突，基底不推进，逐条保留本地 / 采用上游。"],
    ["历史回看", "可以保留任意久的历史，代价是空间与删除复杂度。",
      "只保留有限窗口（默认 25 个版本），更早的状态压缩后拒绝回答，不返回错误的历史。"],
    ["源记录删除", "旧快照 / 副本 / 导出都可能留有内容，很难说“彻底删除”。",
      "删除是最高优先级：重写共享块、清空所有沙箱覆盖层与历史内容、作废缓存、吊销并删除导出。历史在该记录上只返回“已删除”。"],
  ];
  const table = el("table", {});
  table.append(
    el("thead", {}, el("tr", {},
      el("th", {}, "维度"), el("th", {}, "全量复制"), el("th", {}, "共享块 + 覆盖层（本项目）"))),
  );
  const tbody = el("tbody", {});
  for (const [axis, copy, shared] of rows) {
    tbody.append(el("tr", {}, el("td", {}, axis), el("td", { class: "small" }, copy), el("td", { class: "small" }, shared)));
  }
  table.append(tbody);
  openModal({
    title: "取舍说明",
    subtitle: "两个承诺不能同时无限成立：彻底删除优先于无限回看。",
    body: el("div", {},
      el("div", { class: "table-wrap", style: "max-height:none" }, table),
      el("p", { class: "small muted", style: "margin-top:12px" },
        "实现上：共享块按内容哈希命名，删除产生新的替换块并物理删除旧文件；" +
        "沙箱保留的旧块 id 通过替换链解析到新块，所以无需逐个改写沙箱文件。"),
    ),
  });
}

document.addEventListener("click", (event) => {
  if (event.target && event.target.id === "btn-policy") openPolicy();
});
