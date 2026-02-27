#!/usr/bin/env python3
"""
German Compound Momentum Screener
==================================
Universe : DAX (40) + MDAX (60) + SDAX (70) = ~170 stocks
Strategy : Compound Momentum — combines 12-month, 6-month and 3-month
           price momentum into a single composite score.
           Stocks with RSL < 1.0 (below 130-day SMA) are excluded.
History  : Every run appends a snapshot to history.json so the
           webpage can display rank-trend charts over time.

Data source : Yahoo Finance via yfinance
Tickers     : Auto-fetched via pytickersymbols (always up-to-date)

Run locally:
  pip install yfinance pandas pytickersymbols
  python momentum_screener.py
"""

import json
import os
import time
import datetime
import warnings
warnings.filterwarnings("ignore")

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("ERROR: Run: pip install yfinance pandas")
    raise

# ── Config ────────────────────────────────────────────────────────────────────
TOP_N             = 20
RSL_PERIOD        = 130      # days for SMA filter
OUTPUT_JSON       = "screener_data.json"
HISTORY_JSON      = "history.json"
PREV_RANKS_FILE   = "prev_ranks.json"

# Momentum lookback windows (trading days)
MOM_12M = 252
MOM_6M  = 126
MOM_3M  = 63
MOM_SKIP = 21   # skip most recent month (standard academic convention)

# Composite weights
W_12M = 0.40
W_6M  = 0.35
W_3M  = 0.25

# ── Complete hardcoded ticker universe ────────────────────────────────────────
# DAX (40) + MDAX (50) + SDAX (70) = 160 stocks
# Source: Deutsche Börse / Wikipedia, Yahoo Finance .DE tickers (Xetra)
# Last updated: 2025. Update symbols here if index composition changes.

TICKERS = [
    # ── DAX 40 ────────────────────────────────────────────────────────────────
    ("Adidas",                  "ADS.DE"),
    ("Airbus",                  "AIR.DE"),
    ("Allianz",                 "ALV.DE"),
    ("BASF",                    "BAS.DE"),
    ("Bayer",                   "BAYN.DE"),
    ("Beiersdorf",              "BEI.DE"),
    ("BMW",                     "BMW.DE"),
    ("Brenntag",                "BNR.DE"),
    ("Commerzbank",             "CBK.DE"),
    ("Continental",             "CON.DE"),
    ("Covestro",                "1COV.DE"),
    ("Daimler Truck",           "DTG.DE"),
    ("Deutsche Bank",           "DBK.DE"),
    ("Deutsche Börse",          "DB1.DE"),
    ("Deutsche Post",           "DHL.DE"),
    ("Deutsche Telekom",        "DTE.DE"),
    ("E.ON",                    "EOAN.DE"),
    ("Fresenius",               "FRE.DE"),
    ("Hannover Rück",           "HNR1.DE"),
    ("Heidelberg Materials",    "HEIG.DE"),
    ("Henkel",                  "HNKG.DE"),
    ("Infineon",                "IFX.DE"),
    ("Mercedes-Benz",           "MBG.DE"),
    ("Merck KGaA",              "MRK.DE"),
    ("MTU Aero Engines",        "MTX.DE"),
    ("Munich Re",               "MUV2.DE"),
    ("Porsche AG",              "P911.DE"),
    ("Porsche SE",              "PAH3.DE"),
    ("Qiagen",                  "QIA.DE"),
    ("Rheinmetall",             "RHM.DE"),
    ("RWE",                     "RWE.DE"),
    ("SAP",                     "SAP.DE"),
    ("Sartorius",               "SRT3.DE"),
    ("Siemens",                 "SIE.DE"),
    ("Siemens Energy",          "ENR.DE"),
    ("Siemens Healthineers",    "SHL.DE"),
    ("Symrise",                 "SY1.DE"),
    ("Volkswagen",              "VOW3.DE"),
    ("Vonovia",                 "VNA.DE"),
    ("Zalando",                 "ZAL.DE"),

    # ── MDAX 50 ───────────────────────────────────────────────────────────────
    ("Aixtron",                 "AIXA.DE"),
    ("Aroundtown",              "AT1.DE"),
    ("Aurubis",                 "NDA.DE"),
    ("Bechtle",                 "BC8.DE"),
    ("Befesa",                  "BFSA.DE"),
    ("Bilfinger",               "GBF.DE"),
    ("Carl Zeiss Meditec",      "AFX.DE"),
    ("CTS Eventim",             "EVD.DE"),
    ("Delivery Hero",           "DHER.DE"),
    ("Deutsche Lufthansa",      "LHA.DE"),
    ("Encavis",                 "ECV.DE"),
    ("Evonik Industries",       "EVK.DE"),
    ("Evotec",                  "EVT.DE"),
    ("Fraport",                 "FRA.DE"),
    ("Freenet",                 "FNTN.DE"),
    ("Fresenius Medical Care",  "FME.DE"),
    ("Fuchs Petrolub",          "FPE3.DE"),
    ("GEA Group",               "G1A.DE"),
    ("Gerresheimer",            "GXI.DE"),
    ("Hella",                   "HLE.DE"),
    ("HelloFresh",              "HFG.DE"),
    ("Hensoldt",                "HAG.DE"),
    ("Hochtief",                "HOT.DE"),
    ("Hugo Boss",               "BOSS.DE"),
    ("Jenoptik",                "JEN.DE"),
    ("Jungheinrich",            "JUN3.DE"),
    ("K+S",                     "SDF.DE"),
    ("Kion Group",              "KGX.DE"),
    ("Knorr-Bremse",            "KBX.DE"),
    ("Krones",                  "KRN.DE"),
    ("Lanxess",                 "LXS.DE"),
    ("LEG Immobilien",          "LEG.DE"),
    ("Nemetschek",              "NEM.DE"),
    ("Nordex",                  "NDX1.DE"),
    ("PUMA",                    "PUM.DE"),
    ("Rational",                "RAA.DE"),
    ("Redcare Pharmacy",        "RDC.DE"),
    ("RTL Group",               "RRTL.DE"),
    ("Scout24",                 "G24.DE"),
    ("Siltronic",               "WAF.DE"),
    ("Stabilus",                "STM.DE"),
    ("Ströer",                  "SAX.DE"),
    ("TAG Immobilien",          "TEG.DE"),
    ("Talanx",                  "TLX.DE"),
    ("TeamViewer",              "TMV.DE"),
    ("Thyssenkrupp",            "TKA.DE"),
    ("Traton",                  "8TRA.DE"),
    ("TUI",                     "TUI1.DE"),
    ("United Internet",         "UTDI.DE"),
    ("Wacker Chemie",           "WCH.DE"),

    # ── SDAX 70 ───────────────────────────────────────────────────────────────
    ("1&1",                     "1U1.DE"),
    ("Adesso",                  "ADS5.DE"),
    ("Adtran Networks",         "ADTN.DE"),
    ("Amadeus Fire",            "AAD.DE"),
    ("Atoss Software",          "AOF.DE"),
    ("Auto1 Group",             "AG1.DE"),
    ("BayWa",                   "BYW6.DE"),
    ("Borussia Dortmund",       "BVB.DE"),
    ("Cancom",                  "COK.DE"),
    ("Ceconomy",                "CEC.DE"),
    ("Cewe Stiftung",           "CWC.DE"),
    ("CompuGroup Medical",      "COP.DE"),
    ("Dermapharm",              "DMP.DE"),
    ("Deutsche Beteiligungs",   "DBAN.DE"),
    ("Deutsche Pfandbriefbank", "PBB.DE"),
    ("Deutsche Wohnen",         "DWNI.DE"),
    ("Deutz",                   "DEZ.DE"),
    ("Drägerwerk",              "DRW3.DE"),
    ("Dürr",                    "DUE.DE"),
    ("DWS Group",               "DWS.DE"),
    ("Eckert & Ziegler",        "EUZ.DE"),
    ("Elmos Semiconductor",     "ELG.DE"),
    ("Energiekontor",           "EKT.DE"),
    ("Fielmann",                "FIE.DE"),
    ("Flatexdegiro",            "FTK.DE"),
    ("GFT Technologies",        "GFT.DE"),
    ("Grand City Properties",   "GYC.DE"),
    ("Grenke",                  "GLJ.DE"),
    ("Hamborner REIT",          "HABA.DE"),
    ("Heidelberger Druck",      "HDD.DE"),
    ("Hornbach Holding",        "HBH.DE"),
    ("Hypoport",                "HYQ.DE"),
    ("Indus Holding",           "INH.DE"),
    ("Ionos Group",             "IOS.DE"),
    ("Jost Werke",              "JST.DE"),
    ("Klöckner & Co",           "KCO.DE"),
    ("Kontron",                 "KTN.DE"),
    ("KSB",                     "KSB.DE"),
    ("KWS Saat",                "KWS.DE"),
    ("Metro",                   "B4B3.DE"),
    ("Mutares",                 "MUX.DE"),
    ("Nagarro",                 "NA9.DE"),
    ("Norma Group",             "NOEJ.DE"),
    ("Patrizia",                "PAT.DE"),
    ("Pfeiffer Vacuum",         "PFV.DE"),
    ("PNE",                     "PNE3.DE"),
    ("ProSiebenSat.1",          "PSM.DE"),
    ("PVA TePla",               "TPE.DE"),
    ("SAF-Holland",             "SFQ.DE"),
    ("Salzgitter",              "SZG.DE"),
    ("Schaeffler",              "SHA.DE"),
    ("Schott Pharma",           "1SXP.DE"),
    ("SFC Energy",              "F3C.DE"),
    ("SGL Carbon",              "SGL.DE"),
    ("Stratec",                 "SBS.DE"),
    ("Südzucker",               "SZU.DE"),
    ("Süss Microtec",           "SMHN.DE"),
    ("Synlab",                  "SYAB.DE"),
    ("Takkt",                   "TTK.DE"),
    ("Thyssenkrupp Nucera",     "NCH2.DE"),
    ("Varta",                   "VAR1.DE"),
    ("Verbio",                  "VBK.DE"),
    ("Vossloh",                 "VOS.DE"),
    ("Wacker Neuson",           "WAC.DE"),
    ("Wüstenrot & Württemb.",   "WUW.DE"),
]


def get_german_tickers():
    """Returns the hardcoded list of (name, yahoo_ticker) tuples."""
    return TICKERS


# ── Momentum helpers ──────────────────────────────────────────────────────────
def momentum_return(prices: pd.Series, lookback: int, skip: int = MOM_SKIP) -> float:
    """Return % price change from `lookback` days ago to `skip` days ago."""
    if len(prices) < lookback + skip:
        return float('nan')
    p_start = prices.iloc[-(lookback + skip)]
    p_end   = prices.iloc[-skip]
    if p_start <= 0 or pd.isna(p_start):
        return float('nan')
    return ((p_end / p_start) - 1.0) * 100.0


def rsl_score(prices: pd.Series, period: int = RSL_PERIOD) -> float:
    """Levy RSL = current price / N-day SMA."""
    if len(prices) < period:
        return float('nan')
    current = prices.iloc[-1]
    sma     = prices.iloc[-period:].mean()
    if sma <= 0:
        return float('nan')
    return current / sma


# ── Main screener ─────────────────────────────────────────────────────────────
def run_screener():
    now = datetime.datetime.now(datetime.timezone.utc)
    date_str = now.strftime("%Y-%m-%d")

    print("=" * 65)
    print("  German Compound Momentum Screener")
    print("  Universe: DAX + MDAX + SDAX")
    print(f"  Running at: {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 65)

    # ── 1. Get tickers ────────────────────────────────────────────────────────
    tickers = get_german_tickers()
    print(f"\n  Universe: {len(tickers)} stocks (DAX 40 + MDAX 50 + SDAX 70)\n")

    # ── 2. Download price history ─────────────────────────────────────────────
    end_date   = datetime.datetime.today()
    start_date = end_date - datetime.timedelta(days=420)  # ~14 months

    results = []
    skipped = []

    print(f"\nDownloading price data ({len(tickers)} tickers)...\n")

    for i, (name, symbol) in enumerate(tickers):
        print(f"[{i+1:>3}/{len(tickers)}] {symbol:<18}", end="", flush=True)

        try:
            stock = yf.Ticker(symbol)
            hist  = stock.history(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                auto_adjust=True
            )

            if hist.empty or len(hist) < 60:
                skipped.append({"name": name, "ticker": symbol,
                                 "reason": "Insufficient data", "days": len(hist)})
                print("✗  Not enough data")
                time.sleep(0.3)
                continue

            prices = hist["Close"].dropna()
            days   = len(prices)

            # Compute momentum scores
            m12 = momentum_return(prices, MOM_12M)
            m6  = momentum_return(prices, MOM_6M)
            m3  = momentum_return(prices, MOM_3M)
            rsl = rsl_score(prices, RSL_PERIOD)

            if any(pd.isna(x) for x in [m12, m6, m3, rsl]):
                skipped.append({"name": name, "ticker": symbol,
                                 "reason": f"Insufficient history ({days} days)", "days": days})
                print(f"✗  Only {days} days of history")
                time.sleep(0.3)
                continue

            results.append({
                "name":    name,
                "ticker":  symbol,
                "price":   round(float(prices.iloc[-1]), 2),
                "rsl":     round(rsl, 4),
                "mom_12m": round(m12, 2),
                "mom_6m":  round(m6, 2),
                "mom_3m":  round(m3, 2),
            })

            print(f"✓  RSL={rsl:.3f}  12m={m12:+.1f}%  6m={m6:+.1f}%  3m={m3:+.1f}%")

        except Exception as e:
            skipped.append({"name": name, "ticker": symbol,
                             "reason": f"Error: {str(e)[:50]}", "days": 0})
            print(f"✗  {str(e)[:45]}")

        time.sleep(0.3)

    print(f"\n{'─'*65}")
    print(f"  Valid: {len(results)}   Skipped: {len(skipped)}")
    print(f"{'─'*65}")

    if len(results) < TOP_N:
        print(f"⚠  Only {len(results)} valid — need at least {TOP_N}. Aborting.")
        return

    # ── 3. Filter: RSL must be ≥ 1.0 (above 130-day SMA) ────────────────────
    before_filter = len(results)
    results = [r for r in results if r["rsl"] >= 1.0]
    print(f"\nRSL filter (≥1.0): {before_filter} → {len(results)} stocks")

    # ── 4. Rank each momentum window ─────────────────────────────────────────
    n = len(results)
    m12_vals = sorted([r["mom_12m"] for r in results])
    m6_vals  = sorted([r["mom_6m"]  for r in results])
    m3_vals  = sorted([r["mom_3m"]  for r in results])

    def pct_rank(val, sorted_list):
        pos = sorted_list.index(val)
        return (pos / max(n - 1, 1)) * 100

    for r in results:
        r12 = pct_rank(r["mom_12m"], m12_vals)
        r6  = pct_rank(r["mom_6m"],  m6_vals)
        r3  = pct_rank(r["mom_3m"],  m3_vals)
        r["composite"] = round(r12 * W_12M + r6 * W_6M + r3 * W_3M, 2)

    # ── 5. Sort & assign ranks ────────────────────────────────────────────────
    results.sort(key=lambda x: x["composite"], reverse=True)
    for i, r in enumerate(results):
        r["rank"] = i + 1

    top20 = results[:TOP_N]

    # ── 6. Load previous ranks & compute changes ──────────────────────────────
    prev_ranks = {}
    if os.path.exists(PREV_RANKS_FILE):
        with open(PREV_RANKS_FILE) as f:
            prev_ranks = json.load(f)

    for r in top20:
        r["prev_rank"] = prev_ranks.get(r["ticker"], None)

    # Save new prev_ranks
    new_prev = {r["ticker"]: r["rank"] for r in top20}
    with open(PREV_RANKS_FILE, "w") as f:
        json.dump(new_prev, f)

    # ── 7. Write screener_data.json (current snapshot) ───────────────────────
    output = {
        "updated":         now.strftime("%Y-%m-%d %H:%M UTC"),
        "date":            date_str,
        "universe":        "DAX + MDAX + SDAX",
        "total_screened":  len(results),
        "total_attempted": len(tickers),
        "skipped_count":   len(skipped),
        "top20":           top20,
        "skipped":         skipped,
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\n✅  Saved → {OUTPUT_JSON}")

    # ── 8. Append to history.json ─────────────────────────────────────────────
    history = []
    if os.path.exists(HISTORY_JSON):
        with open(HISTORY_JSON, encoding="utf-8") as f:
            history = json.load(f)

    # Avoid duplicate entries for the same date
    history = [h for h in history if h.get("date") != date_str]

    # Snapshot: store rank + composite score + price for each top20 stock
    snapshot = {
        "date": date_str,
        "stocks": [
            {
                "ticker":    r["ticker"],
                "name":      r["name"],
                "rank":      r["rank"],
                "composite": r["composite"],
                "rsl":       r["rsl"],
                "mom_12m":   r["mom_12m"],
                "mom_6m":    r["mom_6m"],
                "mom_3m":    r["mom_3m"],
                "price":     r["price"],
            }
            for r in top20
        ]
    }
    history.append(snapshot)

    # Keep last 104 weeks (2 years)
    history = history[-104:]

    with open(HISTORY_JSON, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)
    print(f"✅  History snapshot appended → {HISTORY_JSON}  ({len(history)} weeks stored)")

    # ── 9. Print summary ──────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  TOP {TOP_N} — GERMAN COMPOUND MOMENTUM")
    print(f"{'='*65}")
    for r in top20:
        prev = f"(prev #{r['prev_rank']})" if r["prev_rank"] else "(new)"
        print(f"  #{r['rank']:>2}  {r['ticker']:<18} Score={r['composite']:5.1f}  "
              f"12m={r['mom_12m']:+6.1f}%  RSL={r['rsl']:.3f}  {prev}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    run_screener()
