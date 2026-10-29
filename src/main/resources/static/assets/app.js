// TicketRush demo console. Plain ES modules, no build step, talks to the same REST API as any client.

const $ = (selector) => document.querySelector(selector);
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
const money = (cents) => `$${(cents / 100).toLocaleString(undefined, { minimumFractionDigits: 2 })}`;
const when = (iso) => new Date(iso).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });

const state = {
  token: localStorage.getItem("ticketrush.token"),
  me: null,
  events: [],
  selectedId: null,
  quantity: 1,
  order: null, // { id, key, quantity, eventId, replayed, data }
  ordersPage: 0,
  view: "events",
};

// ---- API ------------------------------------------------------------------

class ApiError extends Error {
  constructor(status, problem) {
    super(problem?.detail || `Request failed (${status})`);
    this.status = status;
    this.code = problem?.code;
  }
}

async function api(path, { method = "GET", body, headers = {} } = {}) {
  const response = await fetch(path, {
    method,
    headers: {
      ...(body ? { "Content-Type": "application/json" } : {}),
      ...(state.token ? { Authorization: `Bearer ${state.token}` } : {}),
      ...headers,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  if (!response.ok) {
    if (response.status === 401 && state.token) logout(false);
    throw new ApiError(response.status, data);
  }
  return { data, headers: response.headers };
}

function toast(message, kind = "") {
  const node = document.createElement("div");
  node.className = `toast ${kind}`;
  node.textContent = message;
  $("#toasts").append(node);
  setTimeout(() => node.remove(), 4000);
}

const fail = (error) => toast(error.message, "bad");

// ---- Session --------------------------------------------------------------

async function loadSession() {
  if (!state.token) {
    state.me = null;
  } else {
    try {
      state.me = (await api("/api/auth/me")).data;
    } catch {
      state.me = null;
    }
  }
  renderSession();
}

const isAdmin = () => state.me?.scopes.includes("admin");

function renderSession() {
  const box = $("#session");
  if (state.me) {
    box.innerHTML = `<span><strong>${esc(state.me.username)}</strong></span>
      ${isAdmin() ? '<span class="chip">admin</span>' : ""}
      <button id="logout">Log out</button>`;
    $("#logout").onclick = () => logout(true);
  } else {
    box.innerHTML = `<button id="login">Log in</button><button class="primary" id="signup">Sign up</button>`;
    $("#login").onclick = () => openAuth("login");
    $("#signup").onclick = () => openAuth("register");
  }
  document.querySelectorAll(".admin-only").forEach((el) => (el.hidden = !isAdmin()));
  if (!isAdmin() && state.view === "admin") show("events");
  renderReservePanel();
}

function logout(announce) {
  state.token = null;
  state.me = null;
  state.order = null;
  localStorage.removeItem("ticketrush.token");
  renderSession();
  if (announce) toast("Logged out");
}

let authMode = "login";

function openAuth(mode) {
  authMode = mode;
  const registering = mode === "register";
  $("#auth-title").textContent = registering ? "Create an account" : "Log in";
  $("#auth-submit").textContent = registering ? "Sign up" : "Log in";
  $("#auth-switch").textContent = registering ? "Have an account? Log in" : "New here? Sign up";
  $("#auth-form [name=password]").autocomplete = registering ? "new-password" : "current-password";
  $("#auth-error").hidden = true;
  $("#auth-dialog").showModal();
}

$("#auth-switch").onclick = () => openAuth(authMode === "login" ? "register" : "login");
$("#auth-cancel").onclick = () => $("#auth-dialog").close();
$("#auth-form").onsubmit = async (event) => {
  event.preventDefault();
  const credentials = Object.fromEntries(new FormData(event.target));
  try {
    if (authMode === "register") await api("/api/auth/register", { method: "POST", body: credentials });
    const { data } = await api("/api/auth/login", { method: "POST", body: credentials });
    state.token = data.accessToken;
    localStorage.setItem("ticketrush.token", state.token);
    $("#auth-dialog").close();
    event.target.reset();
    await loadSession();
    toast(`Welcome, ${state.me.username}`);
  } catch (error) {
    $("#auth-error").textContent = error.message;
    $("#auth-error").hidden = false;
  }
};

// ---- Events ---------------------------------------------------------------

async function loadEvents() {
  try {
    state.events = (await api("/api/events?size=50")).data.items;
    renderEvents();
    renderStatsOptions();
    updateReservePanelStock();
  } catch (error) {
    $("#event-list").innerHTML = `<div class="empty">Could not load events: ${esc(error.message)}</div>`;
  }
}

function saleChip(event) {
  if (event.availableTickets === 0) return '<span class="chip bad">Sold out</span>';
  if (new Date(event.saleStartsAt) > new Date()) return '<span class="chip neutral">Opens soon</span>';
  return '<span class="chip ok">On sale</span>';
}

function renderEvents() {
  const list = $("#event-list");
  if (!state.events.length) {
    list.innerHTML = `<div class="empty">No upcoming events yet.${isAdmin() ? " Create one in the Admin tab." : ""}</div>`;
    return;
  }
  list.innerHTML = state.events
    .map((e) => {
      const pct = e.totalTickets ? (e.availableTickets / e.totalTickets) * 100 : 0;
      const level = e.availableTickets === 0 ? "out" : pct < 20 ? "low" : "";
      return `<article class="event ${e.id === state.selectedId ? "selected" : ""}" data-id="${e.id}">
        <div class="event-top">
          <div>
            <div class="event-name">${esc(e.name)}</div>
            <div class="event-meta">${esc(e.venue)} · ${when(e.startsAt)}</div>
          </div>
          <div class="price">${money(e.priceCents)}</div>
        </div>
        <div class="meter ${level}"><span style="width:${pct}%"></span></div>
        <div class="event-foot">
          <span>${e.availableTickets.toLocaleString()} / ${e.totalTickets.toLocaleString()} left</span>
          ${saleChip(e)}
        </div>
      </article>`;
    })
    .join("");
  list.querySelectorAll(".event").forEach((card) => {
    card.onclick = () => {
      state.selectedId = Number(card.dataset.id);
      state.quantity = 1;
      renderEvents();
      renderReservePanel();
    };
  });
}

const selectedEvent = () => state.events.find((e) => e.id === state.selectedId);

function renderReservePanel() {
  const panel = $("#reserve-panel");
  const event = selectedEvent();
  if (!event) {
    panel.innerHTML = '<p class="muted">Pick an event to reserve tickets.</p>';
    return;
  }
  const opensIn = new Date(event.saleStartsAt) - new Date();
  panel.innerHTML = `
    <h2>${esc(event.name)}</h2>
    <dl class="kv">
      <dt>Venue</dt><dd>${esc(event.venue)}</dd>
      <dt>Starts</dt><dd>${when(event.startsAt)}</dd>
      <dt>Sale opens</dt><dd>${when(event.saleStartsAt)}</dd>
      <dt>Price</dt><dd>${money(event.priceCents)} each, max ${event.perUserLimit} per customer</dd>
      <dt>Left</dt><dd id="panel-stock">${event.availableTickets.toLocaleString()}</dd>
    </dl>
    <div class="qty">
      <button id="qty-down" aria-label="Fewer">−</button>
      <output id="qty">${state.quantity}</output>
      <button id="qty-up" aria-label="More">+</button>
      <span class="muted small" id="qty-total">${money(event.priceCents * state.quantity)}</span>
    </div>
    <button class="primary" id="reserve" style="width:100%" ${opensIn > 0 ? "disabled" : ""}>
      ${opensIn > 0 ? "Sale hasn't opened yet" : state.me ? "Reserve tickets" : "Log in to reserve"}
    </button>
    <div id="order-slot"></div>`;
  const setQty = (q) => {
    state.quantity = Math.min(Math.max(q, 1), event.perUserLimit);
    $("#qty").textContent = state.quantity;
    $("#qty-total").textContent = money(event.priceCents * state.quantity);
  };
  $("#qty-down").onclick = () => setQty(state.quantity - 1);
  $("#qty-up").onclick = () => setQty(state.quantity + 1);
  $("#reserve").onclick = () => (state.me ? reserve(event, crypto.randomUUID()) : openAuth("login"));
  renderOrderCard();
}

function updateReservePanelStock() {
  const event = selectedEvent();
  const cell = $("#panel-stock");
  if (event && cell) cell.textContent = event.availableTickets.toLocaleString();
}

// ---- Reservation flow -----------------------------------------------------

async function reserve(event, idempotencyKey, quantity = state.quantity) {
  try {
    const { data, headers } = await api(`/api/events/${event.id}/reservations`, {
      method: "POST",
      body: { quantity },
      headers: { "Idempotency-Key": idempotencyKey },
    });
    const replayed = headers.get("Idempotent-Replayed") === "true";
    state.order = { id: data.orderId, key: idempotencyKey, quantity, eventId: event.id, replayed, data: null };
    toast(replayed ? "Same Idempotency-Key: the original order was returned, nothing new was reserved" : "Reserved! Waiting for the order to be confirmed…");
    pollOrder();
  } catch (error) {
    fail(error);
  }
}

async function pollOrder() {
  const order = state.order;
  if (!order) return;
  try {
    order.data = (await api(`/api/orders/${order.id}`)).data;
  } catch (error) {
    fail(error);
    return;
  }
  renderOrderCard();
  if (order.data.status === "PROCESSING" && state.order === order) setTimeout(pollOrder, 500);
}

const STATUS_CHIP = {
  PROCESSING: "neutral",
  PENDING_PAYMENT: "warn",
  PAID: "ok",
  EXPIRED: "bad",
  CANCELLED: "neutral",
  REJECTED: "bad",
};

function renderOrderCard() {
  const slot = $("#order-slot");
  const order = state.order;
  if (!slot || !order || order.eventId !== state.selectedId) return;
  const d = order.data;
  const status = d?.status ?? "PROCESSING";
  slot.innerHTML = `
    <div class="order-card">
      <div class="row" style="margin:0;justify-content:space-between">
        <strong>Your order</strong>
        <span class="chip ${STATUS_CHIP[status]}">${status.replace("_", " ")}</span>
      </div>
      <dl class="kv">
        <dt>Order</dt><dd><code>${esc(order.id)}</code></dd>
        <dt>Idempotency key</dt><dd><code>${esc(order.key)}</code></dd>
        ${d?.amountCents != null ? `<dt>Total</dt><dd>${money(d.amountCents)} for ${d.quantity} ticket(s)</dd>` : ""}
        ${status === "PENDING_PAYMENT" ? `<dt>Pay within</dt><dd class="countdown" id="countdown"></dd>` : ""}
      </dl>
      <div class="row">
        ${status === "PENDING_PAYMENT" ? '<button class="primary" id="pay">Pay now</button><button class="danger" id="cancel">Cancel</button>' : ""}
        <button id="replay" title="Sends the identical request again with the same Idempotency-Key">Retry same request</button>
      </div>
    </div>`;
  $("#pay")?.addEventListener("click", () => orderAction("payment", "Payment received. Enjoy the show!"));
  $("#cancel")?.addEventListener("click", () => orderAction("cancellation", "Order cancelled, tickets released"));
  $("#replay").onclick = () => reserve(selectedEvent(), order.key, order.quantity);
  tickCountdown();
}

function tickCountdown() {
  const node = $("#countdown");
  const expiresAt = state.order?.data?.expiresAt;
  if (!node || !expiresAt) return;
  const left = Math.max(0, new Date(expiresAt) - new Date());
  node.textContent = `${Math.floor(left / 60000)}:${String(Math.floor((left % 60000) / 1000)).padStart(2, "0")}`;
}

async function orderAction(action, message) {
  try {
    state.order.data = (await api(`/api/orders/${state.order.id}/${action}`, { method: "POST" })).data;
    renderOrderCard();
    toast(message);
    loadEvents();
  } catch (error) {
    fail(error);
  }
}

// ---- Order history --------------------------------------------------------

async function loadOrders() {
  const table = $("#order-table");
  if (!state.me) {
    table.innerHTML = '<div class="empty">Log in to see your orders.</div>';
    return;
  }
  const status = $("#order-filter").value;
  try {
    const { data } = await api(`/api/orders?page=${state.ordersPage}&size=10${status ? `&status=${status}` : ""}`);
    if (!data.items.length) {
      table.innerHTML = '<div class="empty">No orders yet.</div>';
      return;
    }
    const names = Object.fromEntries(state.events.map((e) => [e.id, e.name]));
    table.innerHTML = `<table>
        <thead><tr><th>Placed</th><th>Event</th><th>Tickets</th><th>Total</th><th>Status</th><th>Order</th></tr></thead>
        <tbody>${data.items
          .map(
            (o) => `<tr>
              <td>${when(o.createdAt)}</td>
              <td>${esc(names[o.eventId] ?? `Event #${o.eventId}`)}</td>
              <td>${o.quantity}</td>
              <td>${money(o.amountCents)}</td>
              <td><span class="chip ${STATUS_CHIP[o.status]}">${o.status.replace("_", " ")}</span></td>
              <td class="mono">${esc(o.orderId.slice(0, 8))}…</td>
            </tr>`,
          )
          .join("")}</tbody>
      </table>
      <div class="pager">
        <span class="muted small">Page ${data.page + 1} of ${Math.max(data.totalPages, 1)} · ${data.totalItems} orders</span>
        <button id="prev" ${data.page === 0 ? "disabled" : ""}>Previous</button>
        <button id="next" ${data.page + 1 >= data.totalPages ? "disabled" : ""}>Next</button>
      </div>`;
    $("#prev").onclick = () => { state.ordersPage--; loadOrders(); };
    $("#next").onclick = () => { state.ordersPage++; loadOrders(); };
  } catch (error) {
    fail(error);
  }
}

$("#order-filter").onchange = () => { state.ordersPage = 0; loadOrders(); };

// ---- Admin ----------------------------------------------------------------

const localInput = (date) => new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 16);

function resetEventForm() {
  const form = $("#create-event");
  form.saleStartsAt.value = localInput(new Date());
  form.startsAt.value = localInput(new Date(Date.now() + 30 * 86400000));
}

$("#create-event").onsubmit = async (submit) => {
  submit.preventDefault();
  const f = submit.target;
  try {
    const { data } = await api("/api/events", {
      method: "POST",
      body: {
        name: f.name.value,
        venue: f.venue.value,
        totalTickets: Number(f.totalTickets.value),
        priceCents: Math.round(Number(f.price.value) * 100),
        perUserLimit: Number(f.perUserLimit.value),
        saleStartsAt: new Date(f.saleStartsAt.value).toISOString(),
        startsAt: new Date(f.startsAt.value).toISOString(),
      },
    });
    toast(`Created "${data.name}" with ${data.totalTickets} tickets`);
    await loadEvents();
    $("#stats-event").value = data.id;
    loadStats();
  } catch (error) {
    fail(error);
  }
};

function renderStatsOptions() {
  const select = $("#stats-event");
  const current = select.value;
  select.innerHTML = state.events.map((e) => `<option value="${e.id}">${esc(e.name)} (#${e.id})</option>`).join("");
  if (current && state.events.some((e) => String(e.id) === current)) select.value = current;
}

$("#stats-event").onchange = () => loadStats();

async function loadStats() {
  const id = $("#stats-event").value;
  const box = $("#stats");
  if (!id) {
    box.innerHTML = '<div class="empty">Create an event to see its sales.</div>';
    return;
  }
  try {
    const s = (await api(`/api/events/${id}/stats`)).data;
    const b = s.byStatus;
    box.innerHTML = `
      <div class="sync ${s.storesInSync ? "ok" : "bad"}">
        ${s.storesInSync ? "✓ Redis and MySQL agree" : "≠ Redis and MySQL differ (orders in flight, or drift the reconciler will fix)"}
        <span class="muted" style="margin-left:auto">Redis ${s.availableInRedis ?? "–"} · MySQL ${s.availableInDatabase}</span>
      </div>
      <div class="stats-grid">
        <div class="stat"><div class="label">Tickets sold (paid)</div><div class="value">${s.ticketsSold.toLocaleString()}</div></div>
        <div class="stat"><div class="label">Revenue</div><div class="value">${money(s.revenueCents)}</div></div>
        <div class="stat"><div class="label">Awaiting payment</div><div class="value">${b.PENDING_PAYMENT.tickets.toLocaleString()}</div></div>
        <div class="stat"><div class="label">Left to sell</div><div class="value">${s.availableInDatabase.toLocaleString()} / ${s.totalTickets.toLocaleString()}</div></div>
      </div>
      <table>
        <thead><tr><th>Status</th><th>Orders</th><th>Tickets</th><th>Amount</th></tr></thead>
        <tbody>${Object.entries(b)
          .map(([status, t]) => `<tr><td><span class="chip ${STATUS_CHIP[status]}">${status.replace("_", " ")}</span></td>
            <td>${t.orders.toLocaleString()}</td><td>${t.tickets.toLocaleString()}</td><td>${money(t.amountCents)}</td></tr>`)
          .join("")}</tbody>
      </table>`;
  } catch (error) {
    box.innerHTML = `<div class="empty">${esc(error.message)}</div>`;
  }
}

// ---- Navigation and refresh loop ------------------------------------------

function show(view) {
  state.view = view;
  document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.dataset.view === view));
  document.querySelectorAll(".view").forEach((v) => (v.hidden = v.id !== `view-${view}`));
  if (view === "orders") loadOrders();
  if (view === "admin") loadStats();
}

document.querySelectorAll(".tab").forEach((tab) => (tab.onclick = () => show(tab.dataset.view)));

setInterval(() => {
  if (document.hidden) return;
  if (state.view === "events") loadEvents();
  if (state.view === "admin") loadStats();
}, 1500);
setInterval(tickCountdown, 1000);

resetEventForm();
await loadSession();
await loadEvents();
