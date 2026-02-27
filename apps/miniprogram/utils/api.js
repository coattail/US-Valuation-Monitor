const DEFAULT_API_BASE = "http://localhost:9040";
const API_BASE_STORAGE_KEY = "usvm-api-base";
const LOCAL_API_BASE_FALLBACKS = ["http://localhost:9040", "http://127.0.0.1:9040"];
const REQUEST_TIMEOUT_MS = 8000;

function normalizeApiBase(url) {
  return String(url || "")
    .trim()
    .replace(/\/+$/, "");
}

function getApiBase() {
  const custom = normalizeApiBase(wx.getStorageSync(API_BASE_STORAGE_KEY) || "");
  if (!custom) return DEFAULT_API_BASE;
  return custom;
}

function getToken() {
  return wx.getStorageSync("usvm-dev-token") || "";
}

function buildQuery(query) {
  if (!query || typeof query !== "object") return "";
  const pairs = [];
  Object.keys(query).forEach((key) => {
    const value = query[key];
    if (value === undefined || value === null || value === "") return;
    pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
  });
  return pairs.length ? `?${pairs.join("&")}` : "";
}

function buildApiBaseCandidates() {
  const custom = getApiBase();
  const candidates = [custom, ...LOCAL_API_BASE_FALLBACKS].map((item) => normalizeApiBase(item));
  const seen = new Set();
  return candidates.filter((item) => {
    if (!item || seen.has(item)) return false;
    seen.add(item);
    return true;
  });
}

function requestByBase(base, path, method, data, queryString) {
  return new Promise((resolve, reject) => {
    wx.request({
      url: `${base}${path}${queryString}`,
      method,
      data,
      timeout: REQUEST_TIMEOUT_MS,
      header: {
        "Content-Type": "application/json",
        "X-Dev-Token": getToken(),
      },
      success(res) {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve({ data: res.data, base });
        } else {
          reject(new Error(`HTTP ${res.statusCode}`));
        }
      },
      fail(error) {
        reject(error || new Error("request:fail"));
      },
    });
  });
}

async function request(path, method = "GET", data = null, query = null) {
  const queryString = buildQuery(query);
  const candidates = buildApiBaseCandidates();
  let lastError = null;

  for (const base of candidates) {
    try {
      const result = await requestByBase(base, path, method, data, queryString);
      if (base !== getApiBase()) {
        setApiBase(base);
      }
      return result.data;
    } catch (error) {
      lastError = error;
    }
  }

  const attempts = candidates.join(", ");
  const detail = lastError && lastError.errMsg ? lastError.errMsg : String(lastError || "unknown error");
  throw new Error(`API unreachable. tried=[${attempts}] detail=${detail}`);
}

function devLogin(userId = "mini-user") {
  return request("/api/auth/dev-login", "POST", { userId });
}

function setApiBase(url) {
  wx.setStorageSync(API_BASE_STORAGE_KEY, normalizeApiBase(url || ""));
}

async function probeApiConnection() {
  const candidates = buildApiBaseCandidates();
  let lastError = null;
  for (const base of candidates) {
    try {
      await requestByBase(base, "/healthz", "GET", null, "");
      setApiBase(base);
      return { ok: true, apiBase: base };
    } catch (error) {
      lastError = error;
    }
  }
  const detail = lastError && lastError.errMsg ? lastError.errMsg : String(lastError || "unknown error");
  return { ok: false, apiBase: getApiBase(), detail };
}

function getApiBaseConfig() {
  return {
    apiBase: getApiBase(),
    storageKey: API_BASE_STORAGE_KEY,
    defaultApiBase: DEFAULT_API_BASE,
  };
}

module.exports = {
  request,
  devLogin,
  setApiBase,
  getApiBaseConfig,
  probeApiConnection,
};
