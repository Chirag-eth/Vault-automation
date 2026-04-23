const SAMPLE_PAYLOAD = {
  market_name: "BRE_BRE_vs_ARS_EPL_2026",
  market: {
    question: "BRE_BRE_vs_ARS_EPL_2026",
    status: "active",
    outcomes: {
      YES: { token_id: "" },
      NO: { token_id: "" },
    },
    pred_mapping: {
      market_id: "",
    },
  },
};

const payloadInput = document.getElementById("payload-input");
const responseOutput = document.getElementById("response-output");
const refreshButton = document.getElementById("refresh-button");
const matchButton = document.getElementById("match-button");
const sampleButton = document.getElementById("sample-button");
const copyResponseButton = document.getElementById("copy-response-button");
const refreshLabel = document.getElementById("refresh-label");
const toast = document.getElementById("toast");

const metricMappings = document.getElementById("metric-mappings");
const metricReviews = document.getElementById("metric-reviews");
const metricOrderbooks = document.getElementById("metric-orderbooks");
const mappingsList = document.getElementById("mappings-list");
const reviewsList = document.getElementById("reviews-list");
const orderbooksList = document.getElementById("orderbooks-list");

let refreshTimer = null;
let toastTimer = null;

function prettyJson(value) {
  return JSON.stringify(value, null, 2);
}

function setSamplePayload() {
  payloadInput.value = prettyJson(SAMPLE_PAYLOAD);
}

function showToast(message) {
  toast.textContent = message;
  toast.classList.add("visible");
  window.clearTimeout(toastTimer);
  toastTimer = window.setTimeout(() => {
    toast.classList.remove("visible");
  }, 2200);
}

function truncate(value, lead = 8, tail = 6) {
  if (!value) {
    return "n/a";
  }
  const text = String(value);
  if (text.length <= lead + tail + 3) {
    return text;
  }
  return `${text.slice(0, lead)}...${text.slice(-tail)}`;
}

function renderEmpty(target, message) {
  target.innerHTML = `<div class="empty-state">${message}</div>`;
}

function renderMappings(items) {
  if (!items.length) {
    renderEmpty(
      mappingsList,
      "No matched markets yet. Run a backfill or use the matcher form above."
    );
    return;
  }
  mappingsList.innerHTML = items
    .map(
      (item) => `
        <article class="list-item">
          <div class="item-title-row">
            <div>
              <h3 class="item-title">${item.outcome_label || "Matched market"}</h3>
              <p class="item-meta">${truncate(item.pred_market_id)} -> ${truncate(item.polymarket_market_id)}</p>
            </div>
            <span class="pill">${item.tracking_status || "upcoming"}</span>
          </div>
          <div class="mini-grid">
            <div class="mini-card">
              <span class="mini-meta">Yes</span>
              <strong>${truncate(item.yes_token_id, 10, 8)}</strong>
            </div>
            <div class="mini-card">
              <span class="mini-meta">No</span>
              <strong>${truncate(item.no_token_id, 10, 8)}</strong>
            </div>
            <div class="mini-card">
              <span class="mini-meta">Score</span>
              <strong>${item.match_score}</strong>
            </div>
          </div>
        </article>
      `
    )
    .join("");
}

function renderReviews(items) {
  if (!items.length) {
    renderEmpty(
      reviewsList,
      "Nothing in the review queue. Ambiguous or missing matches will surface here."
    );
    return;
  }
  reviewsList.innerHTML = items
    .map(
      (item) => `
        <article class="list-item">
          <div class="item-title-row">
            <div>
              <h3 class="item-title">${truncate(item.pred_market_id)}</h3>
              <p class="item-meta">${item.reason}</p>
            </div>
            <span class="pill" data-tone="warning">Review</span>
          </div>
          <div class="mini-grid">
            <div class="mini-card">
              <span class="mini-meta">Top score</span>
              <strong>${item.top_score}</strong>
            </div>
            <div class="mini-card">
              <span class="mini-meta">Candidates</span>
              <strong>${(item.candidate_market_ids || []).length}</strong>
            </div>
          </div>
        </article>
      `
    )
    .join("");
}

function orderbookDepth(snapshotSide) {
  return {
    bids: Array.isArray(snapshotSide?.bids) ? snapshotSide.bids.length : 0,
    asks: Array.isArray(snapshotSide?.asks) ? snapshotSide.asks.length : 0,
  };
}

function renderOrderbooks(items) {
  if (!items.length) {
    renderEmpty(
      orderbooksList,
      "No orderbook snapshots yet. Start the orderbook listener to see live depth here."
    );
    return;
  }
  orderbooksList.innerHTML = items
    .map((item) => {
      const yesDepth = orderbookDepth(item.orderbook_snapshot?.yes);
      const noDepth = orderbookDepth(item.orderbook_snapshot?.no);
      return `
        <article class="list-item">
          <div class="item-title-row">
            <div>
              <h3 class="item-title">${truncate(item.polymarket_market_id)}</h3>
              <p class="item-meta">Latest Yes/No depth snapshot</p>
            </div>
            <span class="pill">${yesDepth.bids + yesDepth.asks + noDepth.bids + noDepth.asks} levels</span>
          </div>
          <div class="mini-grid">
            <div class="mini-card">
              <span class="mini-meta">Best bid yes</span>
              <strong>${item.best_bid_yes || "0"}</strong>
            </div>
            <div class="mini-card">
              <span class="mini-meta">Best ask yes</span>
              <strong>${item.best_ask_yes || "0"}</strong>
            </div>
            <div class="mini-card">
              <span class="mini-meta">Best bid no</span>
              <strong>${item.best_bid_no || "0"}</strong>
            </div>
            <div class="mini-card">
              <span class="mini-meta">Best ask no</span>
              <strong>${item.best_ask_no || "0"}</strong>
            </div>
          </div>
        </article>
      `;
    })
    .join("");
}

async function fetchState() {
  refreshLabel.textContent = "Refreshing...";
  try {
    const response = await fetch("/api/state", {
      headers: { Accept: "application/json" },
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Failed to fetch dashboard state");
    }
    metricMappings.textContent = payload.summary?.mappings ?? 0;
    metricReviews.textContent = payload.summary?.reviews ?? 0;
    metricOrderbooks.textContent = payload.summary?.orderbooks ?? 0;
    renderMappings(payload.mappings || []);
    renderReviews(payload.reviews || []);
    renderOrderbooks(payload.orderbooks || []);
    refreshLabel.textContent = "Auto refresh every 10s";
  } catch (error) {
    refreshLabel.textContent = "Refresh failed";
    showToast(error.message || "Failed to load dashboard state");
  }
}

async function submitMatch() {
  matchButton.disabled = true;
  matchButton.textContent = "Matching...";
  try {
    const parsed = JSON.parse(payloadInput.value);
    const response = await fetch("/api/match", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify(parsed),
    });
    const payload = await response.json();
    responseOutput.textContent = prettyJson(payload);
    if (!response.ok) {
      throw new Error(payload.error || "Match request failed");
    }
    showToast("Match request completed");
    await fetchState();
  } catch (error) {
    responseOutput.textContent = prettyJson({ error: error.message || String(error) });
    showToast(error.message || "Match request failed");
  } finally {
    matchButton.disabled = false;
    matchButton.textContent = "Match Market";
  }
}

async function copyResponse() {
  try {
    await navigator.clipboard.writeText(responseOutput.textContent);
    showToast("Response copied");
  } catch (_error) {
    showToast("Copy failed");
  }
}

function bindEvents() {
  refreshButton.addEventListener("click", fetchState);
  matchButton.addEventListener("click", submitMatch);
  sampleButton.addEventListener("click", () => {
    setSamplePayload();
    showToast("Sample payload loaded");
  });
  copyResponseButton.addEventListener("click", copyResponse);
}

function startAutoRefresh() {
  window.clearInterval(refreshTimer);
  refreshTimer = window.setInterval(fetchState, 10000);
}

function init() {
  setSamplePayload();
  bindEvents();
  fetchState();
  startAutoRefresh();
}

init();
