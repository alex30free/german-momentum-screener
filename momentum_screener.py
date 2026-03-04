#!/usr/bin/env python3
"""
Compound Momentum Screener for Swedish Stocks
============================================
Ranks all OMX Stockholm stocks by COMPOUND MOMENTUM SCORE:
    Score = Return(3M) + Return(6M) + Return(12M)

Each return is the raw percentage price change over the window.
Top 20 stocks by compound score are saved to momentum_data.json.

Optional filters (disabled by default — set to True to enable):
    FILTER_BY_SIZE    — exclude stocks below MIN_MARKET_CAP_MSEK
    FILTER_BY_FSCORE  — exclude stocks with Piotroski F-Score below MIN_FSCORE

Updated every Friday evening after Nasdaq Stockholm closes,
via GitHub Actions (.github/workflows/update_momentum.yml).
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

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

# ── Optional filters ── (set to True to activate)
FILTER_BY_SIZE   = False   # Filter by minimum market capitalisation
FILTER_BY_FSCORE = False   # Filter by minimum Piotroski F-Score

# Size filter threshold (in million SEK)
# Examples: 500 = small cap+, 2000 = mid cap+, 10000 = large cap only
MIN_MARKET_CAP_MSEK = 500

# Piotroski F-Score filter threshold (0–9 scale)
# 0–2 = weak, 3–5 = medium, 6–9 = strong
# Recommended: 5 or 6 to keep only financially healthy companies
MIN_FSCORE = 5


# ─────────────────────────────────────────────────────────────────────────────
# FULL SWEDISH STOCK UNIVERSE
# ─────────────────────────────────────────────────────────────────────────────

TICKERS = [
    ("AAK", "AAK.ST"), ("ABB", "ABB.ST"), ("AFRY", "AFRY.ST"), ("AQ Group", "AQ.ST"),
    ("AcadeMedia", "ACAD.ST"), ("Acast", "ACAST.ST"), ("Acrinova B", "ACRI-B.ST"),
    ("Active Biotech", "ACTI.ST"), ("AddLife", "ALIF-B.ST"), ("Addnode", "ANOD-B.ST"),
    ("Addtech", "ADDT-B.ST"), ("Alfa Laval", "ALFA.ST"), ("Alimak", "ALIG.ST"),
    ("Alleima", "ALLEI.ST"), ("Alligo", "ALLIGO-B.ST"), ("Alvotech SDB", "ALVO-SDB.ST"),
    ("Ambea", "AMBEA.ST"), ("Arctic Paper", "ARP.ST"), ("Arion Banki", "ARION-SDB.ST"),
    ("Arjo", "ARJO-B.ST"), ("Arla Plast", "ARPL.ST"), ("Assa Abloy", "ASSA-B.ST"),
    ("AstraZeneca", "AZN.ST"), ("Atlas Copco B", "ATCO-B.ST"), ("Atrium Ljungberg", "ATRLJ-B.ST"),
    ("Attendo", "ATT.ST"), ("Autoliv", "ALIV-SDB.ST"), ("Avanza Bank", "AZA.ST"),
    ("Axfood", "AXFO.ST"), ("B3 Consulting", "B3.ST"), ("BE Group", "BEGR.ST"),
    ("BHG Group", "BHG.ST"), ("BICO Group", "BICO.ST"), ("BTS Group", "BTS-B.ST"),
    ("Balco Group", "BALCO.ST"), ("Beijer Alma", "BEIA-B.ST"), ("Beijer Ref", "BEIJ-B.ST"),
    ("Bergman & Beving", "BERG-B.ST"), ("Betsson", "BETS-B.ST"), ("Better Collective", "BETCO.ST"),
    ("Bilia", "BILI-A.ST"), ("Billerud", "BILL.ST"), ("BioArctic", "BIOA-B.ST"),
    ("BioGaia", "BIOG-B.ST"), ("Bjorn Borg", "BORG.ST"), ("Boliden", "BOL.ST"),
    ("Bonava B", "BONAV-B.ST"), ("Bonesupport", "BONEX.ST"), ("Boozt", "BOOZT.ST"),
    ("Boule Diagnostics", "BOUL.ST"), ("Bravida", "BRAV.ST"), ("Bufab", "BUFAB.ST"),
    ("Bulten", "BULTEN.ST"), ("Bure Equity", "BURE.ST"), ("Byggmax", "BMAX.ST"),
    ("C-RAD", "CRAD-B.ST"), ("CTEK", "CTEK.ST"), ("CTT Systems", "CTT.ST"),
    ("Camurus", "CAMX.ST"), ("Castellum", "CAST.ST"), ("Catena", "CATE.ST"),
    ("CellaVision", "CEVI.ST"), ("Cibus Nordic", "CIBUS.ST"), ("Cint Group", "CINT.ST"),
    ("Clas Ohlson", "CLAS-B.ST"), ("Cloetta", "CLA-B.ST"), ("CoinShares", "CS.ST"),
    ("Coor Service Management", "COOR.ST"), ("Corem Property B", "CORE-B.ST"),
    ("Dedicare", "DEDI.ST"), ("Dios Fastigheter", "DIOS.ST"), ("Dometic", "DOM.ST"),
    ("Duni", "DUNI.ST"), ("Dustin Group", "DUST.ST"), ("Dynavox Group", "DYVOX.ST"),
    ("EQT", "EQT.ST"), ("Eastnine", "EAST.ST"), ("Elanders", "ELAN-B.ST"),
    ("Electrolux B", "ELUX-B.ST"), ("Electrolux Professional B", "EPRO-B.ST"),
    ("Elekta", "EKTA-B.ST"), ("Eltel", "ELTEL.ST"), ("Embracer", "EMBRAC-B.ST"),
    ("Enea", "ENEA.ST"), ("Engcon", "ENGCON-B.ST"), ("Eniro", "ENRO.ST"),
    ("Eolus", "EOLU-B.ST"), ("Epiroc B", "EPI-B.ST"), ("Ericsson B", "ERIC-B.ST"),
    ("Essity B", "ESSITY-B.ST"), ("Evolution", "EVO.ST"), ("FM Mattsson", "FMM-B.ST"),
    ("Fabege", "FABG.ST"), ("Fagerhult", "FAG.ST"), ("Fast Balder", "BALD-B.ST"),
    ("Fastpartner A", "FPAR-A.ST"), ("Fenix Outdoor", "FOI-B.ST"), ("Fingerprint Cards", "FING-B.ST"),
    ("Formpipe Software", "FPIP.ST"), ("G5 Entertainment", "G5EN.ST"), ("Garo", "GARO.ST"),
    ("Genova Property", "GPG.ST"), ("Getinge", "GETI-B.ST"), ("Granges", "GRNG.ST"),
    ("HMS Networks", "HMS.ST"), ("Handelsbanken B", "SHB-B.ST"), ("Hanza", "HANZA.ST"),
    ("Heba", "HEBA-B.ST"), ("Hemnet", "HEM.ST"), ("Hennes and Mauritz", "HM-B.ST"),
    ("Hexagon", "HEXA-B.ST"), ("Hexatronic", "HTRO.ST"), ("Hexpol", "HPOL-B.ST"),
    ("Holmen B", "HOLM-B.ST"), ("Hufvudstaden A", "HUFV-A.ST"), ("Humana", "HUM.ST"),
    ("Husqvarna B", "HUSQ-B.ST"), ("ITAB Shop Concept", "ITAB.ST"),
    ("Industrivarden C", "INDU-C.ST"), ("Indutrade", "INDT.ST"), ("Instalco", "INSTAL.ST"),
    ("International Petroleum", "IPCO.ST"), ("Intrum", "INTRUM.ST"), ("Investor B", "INVE-B.ST"),
    ("Invisio", "IVSO.ST"), ("Inwido", "INWI.ST"), ("JM", "JM.ST"),
    ("K-Fast Holding", "KFAST-B.ST"), ("Kinnevik B", "KINV-B.ST"), ("KnowIT", "KNOW.ST"),
    ("Lagercrantz", "LAGR-B.ST"), ("Latour", "LATO-B.ST"), ("Lifco", "LIFCO-B.ST"),
    ("Lime Technologies", "LIME.ST"), ("Lindab", "LIAB.ST"), ("Loomis", "LOOMIS.ST"),
    ("Lundbergforetagen", "LUND-B.ST"), ("Lundin Gold", "LUG.ST"), ("Lundin Mining", "LUMI.ST"),
    ("MEKO", "MEKO.ST"), ("Malmbergs Elektriska", "MEAB-B.ST"), ("MedCap", "MCAP.ST"),
    ("Medicover", "MCOV-B.ST"), ("Midsona B", "MSON-B.ST"), ("Mildef Group", "MILDEF.ST"),
    ("Mips", "MIPS.ST"), ("Modern Times Group B", "MTG-B.ST"), ("Momentum Group", "MMGR-B.ST"),
    ("Munters", "MTRS.ST"), ("Mycronic", "MYCR.ST"), ("NCAB Group", "NCAB.ST"),
    ("NCC B", "NCC-B.ST"), ("NIBE Industrier", "NIBE-B.ST"), ("NOTE", "NOTE.ST"),
    ("NP3 Fastigheter", "NP3.ST"), ("Nederman", "NMAN.ST"), ("Nelly Group", "NELLY.ST"),
    ("Neobo Fastigheter", "NEOBO.ST"), ("New Wave", "NEWA-B.ST"), ("Nobia", "NOBI.ST"),
    ("Nolato", "NOLA-B.ST"), ("Nordea Bank", "NDA-SE.ST"), ("Nordnet", "SAVE.ST"),
    ("Nyfosa", "NYF.ST"), ("OEM International", "OEM-B.ST"), ("Orexo", "ORX.ST"),
    ("Orron Energy", "ORRON.ST"), ("Pandox", "PNDX-B.ST"), ("Peab", "PEAB-B.ST"),
    ("Platzer Fastigheter", "PLAZ-B.ST"), ("Pricer", "PRIC-B.ST"), ("Proact IT", "PACT.ST"),
    ("Profoto", "PRFO.ST"), ("Ratos B", "RATO-B.ST"), ("RaySearch Laboratories", "RAY-B.ST"),
    ("Rejlers", "REJL-B.ST"), ("Revolutionrace", "RVRC.ST"), ("Rottneros", "RROS.ST"),
    ("Rusta", "RUSTA.ST"), ("SCA B", "SCA-B.ST"), ("SEB C", "SEB-C.ST"),
    ("SKF B", "SKF-B.ST"), ("SSAB B", "SSAB-B.ST"), ("Saab", "SAAB-B.ST"),
    ("Sagax B", "SAGA-B.ST"), ("Sandvik", "SAND.ST"), ("Scandi Standard", "SCST.ST"),
    ("Scandic Hotels", "SHOT.ST"), ("Sdiptech", "SDIP-B.ST"), ("Sectra", "SECT-B.ST"),
    ("Securitas", "SECU-B.ST"), ("Sinch", "SINCH.ST"), ("Skanska", "SKA-B.ST"),
    ("SkiStar", "SKIS-B.ST"), ("Storskogen", "STOR-B.ST"), ("Stora Enso R", "STE-R.ST"),
    ("Sweco B", "SWEC-B.ST"), ("Swedbank", "SWED-A.ST"), ("Swedish Logistic Property", "SLP-B.ST"),
    ("Swedish Orphan Biovitrum", "SOBI.ST"), ("Systemair", "SYSR.ST"), ("TF Bank", "TFBANK.ST"),
    ("Tele2 B", "TEL2-B.ST"), ("Telia Company", "TELIA.ST"), ("Thule", "THULE.ST"),
    ("TietoEVRY", "TIETOS.ST"), ("Trelleborg", "TREL-B.ST"), ("Troax Group", "TROAX.ST"),
    ("Truecaller", "TRUE-B.ST"), ("VBG Group", "VBG-B.ST"), ("VNV Global", "VNV.ST"),
    ("Vitec Software", "VIT-B.ST"), ("Vitrolife", "VITR.ST"), ("Volati", "VOLO.ST"),
    ("Volvo B", "VOLV-B.ST"), ("Volvo Car", "VOLCAR-B.ST"), ("Wallenstam", "WALL-B.ST"),
    ("Wihlborgs Fastigheter", "WIHL.ST"), ("XANO Industri", "XANO-B.ST"),
    ("Xvivo Perfusion", "XVIVO.ST"), ("Yubico", "YUBICO.ST"), ("eWork", "EWRK.ST"),
    ("Oresund", "ORES.ST"),
]

OUTPUT_JSON     = "momentum_data.json"
PREV_RANKS_FILE = "momentum_prev_ranks.json"

# Approximate trading days per calendar period
DAYS_3M  = 65    # ~3 months
DAYS_6M  = 130   # ~6 months
DAYS_12M = 260   # ~12 months

# SEK per USD (approximate — used to convert market cap from USD to SEK)
# Yahoo Finance returns market cap in USD regardless of listing currency
USD_TO_SEK = 10.5


# ─────────────────────────────────────────────────────────────────────────────
# PIOTROSKI F-SCORE
# ─────────────────────────────────────────────────────────────────────────────

def compute_fscore(info: dict) -> int | None:
    """
    Compute a simplified Piotroski F-Score (0–9) from Yahoo Finance fundamentals.

    The original Piotroski (2000) score uses 9 binary signals across three groups:
      Profitability (4 signals): ROA, Operating Cash Flow, Change in ROA, Accruals
      Leverage / Liquidity (3 signals): Change in Leverage, Change in Current Ratio, No new shares issued
      Operating Efficiency (2 signals): Change in Gross Margin, Change in Asset Turnover

    Since Yahoo Finance does not provide prior-year balance sheet data directly via
    yfinance .info, we compute a *proxy F-Score* using available TTM and balance sheet
    fields. Signals that cannot be computed are scored conservatively as 0 (not awarded).

    Returns the integer score (0–9), or None if too few signals can be computed.
    """
    score = 0
    signals_available = 0

    def safe(key, default=None):
        v = info.get(key)
        return v if v is not None and v == v else default  # also guards NaN

    # ── GROUP A: PROFITABILITY ────────────────────────────────────────────────

    # A1 — ROA > 0  (Net income / Total assets)
    net_income   = safe('netIncomeToCommon')
    total_assets = safe('totalAssets')
    if net_income is not None and total_assets and total_assets > 0:
        signals_available += 1
        roa = net_income / total_assets
        if roa > 0:
            score += 1

    # A2 — Operating Cash Flow > 0
    operating_cf = safe('operatingCashflow')
    if operating_cf is not None:
        signals_available += 1
        if operating_cf > 0:
            score += 1

    # A3 — Cash flow from operations > Net income (Accruals quality)
    if operating_cf is not None and net_income is not None:
        signals_available += 1
        if operating_cf > net_income:
            score += 1

    # A4 — Positive gross profit margin
    gross_margins = safe('grossMargins')
    if gross_margins is not None:
        signals_available += 1
        if gross_margins > 0:
            score += 1

    # ── GROUP B: LEVERAGE / LIQUIDITY ─────────────────────────────────────────

    # B1 — Debt ratio not too high  (Total Debt / Total Assets < 0.6)
    total_debt = safe('totalDebt', 0)
    if total_assets and total_assets > 0:
        signals_available += 1
        debt_ratio = (total_debt or 0) / total_assets
        if debt_ratio < 0.6:
            score += 1

    # B2 — Current ratio >= 1  (Current Assets / Current Liabilities)
    current_ratio = safe('currentRatio')
    if current_ratio is not None:
        signals_available += 1
        if current_ratio >= 1.0:
            score += 1

    # B3 — No recent share dilution  (shares outstanding stable or falling)
    # Proxy: use sharesOutstanding vs floatShares — if float ≈ shares, no preferred/warrants overhang
    shares_out   = safe('sharesOutstanding')
    float_shares = safe('floatShares')
    if shares_out and float_shares and shares_out > 0:
        signals_available += 1
        dilution_ratio = float_shares / shares_out
        if dilution_ratio >= 0.80:   # float is at least 80% of total = limited dilution overhang
            score += 1

    # ── GROUP C: OPERATING EFFICIENCY ─────────────────────────────────────────

    # C1 — Positive operating margin
    operating_margins = safe('operatingMargins')
    if operating_margins is not None:
        signals_available += 1
        if operating_margins > 0:
            score += 1

    # C2 — Positive return on equity
    return_on_equity = safe('returnOnEquity')
    if return_on_equity is not None:
        signals_available += 1
        if return_on_equity > 0:
            score += 1

    # Need at least 5 of the 9 signals to have a reliable score
    if signals_available < 5:
        return None

    return score


# ─────────────────────────────────────────────────────────────────────────────
# MARKET CAP HELPER
# ─────────────────────────────────────────────────────────────────────────────

def get_market_cap_msek(info: dict) -> float | None:
    """
    Return market capitalisation in million SEK, or None if unavailable.
    Yahoo Finance returns market cap in USD — we convert using USD_TO_SEK.
    """
    mc = info.get('marketCap')
    if mc is None or mc != mc or mc <= 0:
        return None
    return (mc * USD_TO_SEK) / 1_000_000   # USD → SEK → million SEK


# ─────────────────────────────────────────────────────────────────────────────
# MOMENTUM CALCULATION
# ─────────────────────────────────────────────────────────────────────────────

def compute_momentum(name, symbol, prices):
    """
    Returns (mom_3m, mom_6m, mom_12m) as percentage returns, or None if data insufficient.
    """
    n = len(prices)
    if n < DAYS_12M + 5:
        return None

    price_now = float(prices.iloc[-1])

    def pct_return(days_back):
        idx = max(0, n - 1 - days_back)
        p_past = float(prices.iloc[idx])
        if p_past == 0 or p_past != p_past:
            return None
        return ((price_now / p_past) - 1.0) * 100.0

    m3  = pct_return(DAYS_3M)
    m6  = pct_return(DAYS_6M)
    m12 = pct_return(DAYS_12M)

    if m3 is None or m6 is None or m12 is None:
        return None

    return m3, m6, m12


# ─────────────────────────────────────────────────────────────────────────────
# PREV RANKS
# ─────────────────────────────────────────────────────────────────────────────

def load_prev_ranks():
    if os.path.exists(PREV_RANKS_FILE):
        with open(PREV_RANKS_FILE) as f:
            return json.load(f)
    return {}


def save_prev_ranks(top20):
    ranks = {r["ticker"]: r["rank"] for r in top20}
    with open(PREV_RANKS_FILE, "w") as f:
        json.dump(ranks, f)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    now        = datetime.datetime.now(datetime.timezone.utc)
    end_date   = datetime.datetime.today()
    start_date = end_date - datetime.timedelta(days=420)

    total = len(TICKERS)

    print("=" * 65)
    print("  Compound Momentum Screener — OMX Stockholm")
    print("  Universe: " + str(total) + " tickers")
    print("  Running at: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("  Windows: 3M (" + str(DAYS_3M) + "d)  6M (" + str(DAYS_6M) + "d)  12M (" + str(DAYS_12M) + "d)")
    print("  Filter — Size:    " + ("ON  (min " + str(MIN_MARKET_CAP_MSEK) + " MSEK)" if FILTER_BY_SIZE   else "OFF"))
    print("  Filter — FScore:  " + ("ON  (min F=" + str(MIN_FSCORE) + ")"              if FILTER_BY_FSCORE else "OFF"))
    print("=" * 65)
    print("")

    results = []
    skipped = []

    for i, (name, symbol) in enumerate(TICKERS):
        print("[" + str(i + 1).rjust(3) + "/" + str(total) + "] " + symbol.ljust(20), end="", flush=True)

        try:
            stock = yf.Ticker(symbol)
            hist  = stock.history(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                auto_adjust=True
            )

            if hist.empty or len(hist) == 0:
                skipped.append({"name": name, "ticker": symbol, "reason": "No price data", "days_available": 0})
                print("✗  No price data")
                time.sleep(0.3)
                continue

            prices = hist["Close"].dropna()
            days   = len(prices)

            mom = compute_momentum(name, symbol, prices)
            if mom is None:
                skipped.append({
                    "name": name, "ticker": symbol,
                    "reason": "Insufficient history (need " + str(DAYS_12M + 5) + "d, have " + str(days) + "d)",
                    "days_available": days
                })
                print("✗  Only " + str(days) + "/" + str(DAYS_12M + 5) + " trading days")
                time.sleep(0.3)
                continue

            m3, m6, m12 = mom
            compound = round(m3 + m6 + m12, 2)

            # Fetch fundamentals (needed for size + fscore filters, and always stored)
            info = stock.info

            # ── Market cap ────────────────────────────────────────────────────
            market_cap_msek = get_market_cap_msek(info)

            if FILTER_BY_SIZE:
                if market_cap_msek is None:
                    skipped.append({
                        "name": name, "ticker": symbol,
                        "reason": "Filtered: market cap unavailable",
                        "days_available": days
                    })
                    print("✗  Filtered: market cap unavailable")
                    time.sleep(0.3)
                    continue
                if market_cap_msek < MIN_MARKET_CAP_MSEK:
                    skipped.append({
                        "name": name, "ticker": symbol,
                        "reason": "Filtered: market cap " + str(round(market_cap_msek)) + " MSEK < " + str(MIN_MARKET_CAP_MSEK) + " MSEK minimum",
                        "days_available": days
                    })
                    print("✗  Filtered: " + str(round(market_cap_msek)) + " MSEK < min " + str(MIN_MARKET_CAP_MSEK) + " MSEK")
                    time.sleep(0.3)
                    continue

            # ── Piotroski F-Score ─────────────────────────────────────────────
            fscore = compute_fscore(info)

            if FILTER_BY_FSCORE:
                if fscore is None:
                    skipped.append({
                        "name": name, "ticker": symbol,
                        "reason": "Filtered: F-Score could not be computed (insufficient fundamental data)",
                        "days_available": days
                    })
                    print("✗  Filtered: F-Score unavailable")
                    time.sleep(0.3)
                    continue
                if fscore < MIN_FSCORE:
                    skipped.append({
                        "name": name, "ticker": symbol,
                        "reason": "Filtered: F-Score " + str(fscore) + " < minimum " + str(MIN_FSCORE),
                        "days_available": days
                    })
                    print("✗  Filtered: F-Score=" + str(fscore) + " < min " + str(MIN_FSCORE))
                    time.sleep(0.3)
                    continue

            results.append({
                "name":             name,
                "ticker":           symbol,
                "price":            round(float(prices.iloc[-1]), 2),
                "mom_3m":           round(m3, 2),
                "mom_6m":           round(m6, 2),
                "mom_12m":          round(m12, 2),
                "compound_score":   compound,
                "market_cap_msek":  round(market_cap_msek) if market_cap_msek else None,
                "fscore":           fscore,
            })

            cap_str    = (str(round(market_cap_msek)) + "MSEK").rjust(10) if market_cap_msek else "    N/A MSEK"
            fscore_str = ("F=" + str(fscore)) if fscore is not None else "F=N/A"
            print("✓  3M=" + str(round(m3, 1)).rjust(7) + "%"
                  "  6M=" + str(round(m6, 1)).rjust(7) + "%"
                  "  12M=" + str(round(m12, 1)).rjust(7) + "%"
                  "  COMPOUND=" + str(compound).rjust(8) + "%"
                  "  " + cap_str + "  " + fscore_str)

        except Exception as e:
            skipped.append({"name": name, "ticker": symbol, "reason": "Error: " + str(e)[:60], "days_available": 0})
            print("✗  " + str(e)[:50])

        time.sleep(0.3)

    print("")
    print("-" * 65)
    print("  Valid: " + str(len(results)) + "  Skipped: " + str(len(skipped)))
    print("-" * 65)

    # Sort descending by compound score
    results.sort(key=lambda x: x["compound_score"], reverse=True)
    for i, r in enumerate(results):
        r["rank"] = i + 1

    top20 = results[:20]

    # Attach previous ranks
    prev_ranks = load_prev_ranks()
    for r in top20:
        r["prev_rank"] = prev_ranks.get(r["ticker"], None)

    save_prev_ranks(top20)

    # Print top 20
    print("")
    print("=" * 65)
    print("  TOP 20 — COMPOUND MOMENTUM RANKING")
    print("=" * 65)
    for r in top20:
        prev = "(prev #" + str(r["prev_rank"]) + ")" if r["prev_rank"] else "(new)"
        cap_str = (str(r["market_cap_msek"]) + " MSEK") if r["market_cap_msek"] else "N/A"
        fs_str  = ("F=" + str(r["fscore"])) if r["fscore"] is not None else "F=N/A"
        print("  #" + str(r["rank"]).rjust(2)
              + "  " + r["name"].ljust(30)
              + "  Score=" + str(r["compound_score"]).rjust(8) + "%"
              + "  Cap=" + cap_str.rjust(12)
              + "  " + fs_str
              + "  " + prev)

    # Determine size/fscore category labels for output
    size_filter_label = ("min " + str(MIN_MARKET_CAP_MSEK) + " MSEK") if FILTER_BY_SIZE else "disabled"
    fscore_filter_label = ("min F=" + str(MIN_FSCORE)) if FILTER_BY_FSCORE else "disabled"

    # Write JSON
    output = {
        "updated":              now.strftime("%Y-%m-%d %H:%M UTC"),
        "total_attempted":      len(TICKERS),
        "stocks_screened":      len(results),
        "skipped_count":        len(skipped),
        "filters": {
            "size_filter":      FILTER_BY_SIZE,
            "size_min_msek":    MIN_MARKET_CAP_MSEK if FILTER_BY_SIZE else None,
            "fscore_filter":    FILTER_BY_FSCORE,
            "fscore_min":       MIN_FSCORE if FILTER_BY_FSCORE else None,
        },
        "top20":                top20,
        "skipped":              skipped,
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print("")
    print("✅  Saved → " + OUTPUT_JSON)
    print("    Universe screened : " + str(len(results)) + " stocks")
    print("    Size filter       : " + size_filter_label)
    print("    F-Score filter    : " + fscore_filter_label)
    print("    Updated           : " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))


if __name__ == "__main__":
    main()
