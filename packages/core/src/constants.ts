import type { IndexMeta } from "./types.ts";

export const CORE_INDICES: IndexMeta[] = [
  {
    id: "sp500",
    symbol: "SPY",
    group: "core",
    displayName: "S&P 500",
    description: "Large-cap US equities proxy via SPY.",
    order: 1,
  },
  {
    id: "nasdaq100",
    symbol: "QQQ",
    group: "core",
    displayName: "Nasdaq 100",
    description: "US mega-cap growth proxy via QQQ.",
    order: 2,
  },
  {
    id: "dow30",
    symbol: "DIA",
    group: "core",
    displayName: "Dow Jones 30",
    description: "Dow 30 proxy via DIA.",
    order: 3,
  },
  {
    id: "russell2000",
    symbol: "IWM",
    group: "core",
    displayName: "Russell 2000",
    description: "US small-cap proxy via IWM.",
    order: 4,
  },
  {
    id: "sp400",
    symbol: "IJH",
    group: "core",
    displayName: "S&P MidCap 400",
    description: "US mid-cap proxy via IJH.",
    order: 5,
  },
  {
    id: "us_total_market",
    symbol: "VTI",
    group: "core",
    displayName: "US Total Market",
    description: "Broad US market proxy via VTI.",
    order: 6,
  },
];

export const SECTOR_INDICES: IndexMeta[] = [
  {
    id: "sector_communication",
    symbol: "XLC",
    group: "sector",
    displayName: "Communication Services",
    description: "S&P sector proxy via XLC.",
    order: 101,
  },
  {
    id: "sector_consumer_discretionary",
    symbol: "XLY",
    group: "sector",
    displayName: "Consumer Discretionary",
    description: "S&P sector proxy via XLY.",
    order: 102,
  },
  {
    id: "sector_consumer_staples",
    symbol: "XLP",
    group: "sector",
    displayName: "Consumer Staples",
    description: "S&P sector proxy via XLP.",
    order: 103,
  },
  {
    id: "sector_energy",
    symbol: "XLE",
    group: "sector",
    displayName: "Energy",
    description: "S&P sector proxy via XLE.",
    order: 104,
  },
  {
    id: "sector_financials",
    symbol: "XLF",
    group: "sector",
    displayName: "Financials",
    description: "S&P sector proxy via XLF.",
    order: 105,
  },
  {
    id: "sector_healthcare",
    symbol: "XLV",
    group: "sector",
    displayName: "Health Care",
    description: "S&P sector proxy via XLV.",
    order: 106,
  },
  {
    id: "sector_industrials",
    symbol: "XLI",
    group: "sector",
    displayName: "Industrials",
    description: "S&P sector proxy via XLI.",
    order: 107,
  },
  {
    id: "sector_materials",
    symbol: "XLB",
    group: "sector",
    displayName: "Materials",
    description: "S&P sector proxy via XLB.",
    order: 108,
  },
  {
    id: "sector_real_estate",
    symbol: "XLRE",
    group: "sector",
    displayName: "Real Estate",
    description: "S&P sector proxy via XLRE.",
    order: 109,
  },
  {
    id: "sector_technology",
    symbol: "XLK",
    group: "sector",
    displayName: "Information Technology",
    description: "S&P sector proxy via XLK.",
    order: 110,
  },
  {
    id: "sector_utilities",
    symbol: "XLU",
    group: "sector",
    displayName: "Utilities",
    description: "S&P sector proxy via XLU.",
    order: 111,
  },
];

export const THEME_INDICES: IndexMeta[] = [
  {
    id: "igv",
    symbol: "IGV",
    group: "theme",
    displayName: "Expanded Tech-Software",
    description: "North American software and interactive media/services proxy via IGV.",
    order: 201,
  },
  {
    id: "soxx",
    symbol: "SOXX",
    group: "theme",
    displayName: "NYSE Semiconductor",
    description: "US-listed semiconductor equities proxy via SOXX.",
    order: 202,
  },
  {
    id: "smh",
    symbol: "SMH",
    group: "theme",
    displayName: "MVIS US Listed Semiconductor 25",
    description: "US-listed semiconductor production and equipment proxy via SMH.",
    order: 203,
  },
  {
    id: "dram",
    symbol: "DRAM",
    group: "theme",
    displayName: "AI Memory & Data Center",
    description: "Memory and data-center semiconductor theme proxy via DRAM; history begins at fund inception.",
    order: 204,
  },
];

export const ALL_INDICES: IndexMeta[] = [...CORE_INDICES, ...SECTOR_INDICES, ...THEME_INDICES].sort(
  (a, b) => a.order - b.order
);

export const INDEX_MAP: Record<string, IndexMeta> = Object.fromEntries(
  ALL_INDICES.map((item) => [item.id, item])
);

export const DEFAULT_ALERT_RULE = {
  metric: "percentile_full",
  upper: 85,
  lower: 15,
  cooldownTradingDays: 5,
} as const;

export const DEFAULT_THEME = "fresh" as const;
