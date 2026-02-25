"""
EFP Wealth — Landing Page Generator
Reads equity curves from the walk-forward results and generates a professional
landing page with hero metrics, equity chart, and value proposition sections.
"""

import json
import urllib.request
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

DATA_DIR = Path("C:/TradingData/greysky/data")
OUTPUT_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

LIBS_DIR = DATA_DIR / 'libs'
CHARTJS_URLS = [
    ('chart.umd.min.js', 'https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js'),
    ('chartjs-adapter-date-fns.bundle.min.js',
     'https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js'),
]


def get_chartjs_inline():
    LIBS_DIR.mkdir(parents=True, exist_ok=True)
    sources = []
    for fname, url in CHARTJS_URLS:
        cached = LIBS_DIR / fname
        if not cached.exists():
            print(f"  Downloading {fname}...")
            urllib.request.urlretrieve(url, cached)
        sources.append(cached.read_text(encoding='utf-8'))
    return sources[0], sources[1]


def compute_drawdown(equity):
    peak = equity.cummax()
    return (equity - peak) / peak


def compute_annual_returns(equity):
    annual = equity.resample('YE').last().pct_change().dropna()
    return {int(d.year): round(v * 100, 1) for d, v in annual.items()}


def generate_landing():
    print("Generating EFP Wealth landing page...")

    chartjs_src, adapter_src = get_chartjs_inline()

    eq_path = DATA_DIR / 'all_equity_curves.csv'
    curves = pd.read_csv(eq_path, index_col=0, parse_dates=True)

    wf = curves['WalkForward'].dropna()
    nifty = curves['NIFTY_100pct'].loc[wf.index[0]:wf.index[-1]].dropna()

    # --- Compute metrics ---
    wf_start, wf_end = wf.index[0], wf.index[-1]
    years = (wf_end - wf_start).days / 365.25
    total_ret = wf.iloc[-1] / wf.iloc[0] - 1
    cagr = (1 + total_ret) ** (1 / years) - 1
    dd = compute_drawdown(wf)
    max_dd = dd.min()
    rets = wf.pct_change().dropna()
    sharpe = rets.mean() / rets.std() * np.sqrt(252) if rets.std() > 0 else 0
    calmar = cagr / abs(max_dd) if max_dd != 0 else 0
    multiple = wf.iloc[-1] / wf.iloc[0]

    nifty_total = nifty.iloc[-1] / nifty.iloc[0] - 1
    nifty_cagr = (1 + nifty_total) ** (1 / years) - 1
    alpha = cagr - nifty_cagr

    nifty_rets = nifty.pct_change().dropna()
    common = rets.index.intersection(nifty_rets.index)
    monthly_wf = wf.resample('ME').last().pct_change().dropna()
    monthly_nifty = nifty.resample('ME').last().pct_change().dropna()
    common_m = monthly_wf.index.intersection(monthly_nifty.index)
    cov = np.cov(monthly_wf.loc[common_m].values, monthly_nifty.loc[common_m].values)
    beta = cov[0, 1] / cov[1, 1] if cov[1, 1] > 0 else 0

    hero = {
        'cagr': round(cagr * 100, 1),
        'max_dd': round(max_dd * 100, 1),
        'sharpe': round(sharpe, 2),
        'calmar': round(calmar, 2),
        'alpha': round(alpha * 100, 1),
        'beta': round(beta, 2),
        'multiple': round(multiple, 1),
        'period': f"{wf_start.strftime('%b %Y')} - {wf_end.strftime('%b %Y')}",
        'years': round(years, 1),
    }

    # --- Equity curve data (weekly, normalized to 100) ---
    norm_wf = wf / wf.iloc[0] * 100
    norm_nifty = nifty / nifty.iloc[0] * 100
    wf_weekly = norm_wf.resample('W').last().dropna()
    nifty_weekly = norm_nifty.resample('W').last().dropna()

    chart_data = {
        'wf': {
            'dates': [d.strftime('%Y-%m-%d') for d in wf_weekly.index],
            'values': [round(v, 1) for v in wf_weekly.values],
        },
        'nifty': {
            'dates': [d.strftime('%Y-%m-%d') for d in nifty_weekly.index],
            'values': [round(v, 1) for v in nifty_weekly.values],
        },
    }

    # --- Annual returns ---
    wf_annual = compute_annual_returns(wf)
    nifty_annual = compute_annual_returns(nifty)
    all_years = sorted(set(list(wf_annual.keys()) + list(nifty_annual.keys())))

    annual_data = {
        'years': all_years,
        'wf': wf_annual,
        'nifty': nifty_annual,
    }

    # --- Monthly win rate ---
    win_rate = round((monthly_wf.loc[common_m] > 0).mean() * 100, 0)

    # --- Build HTML ---
    now = datetime.now().strftime('%Y-%m-%d')

    html = build_landing_html(hero, chart_data, annual_data, win_rate, now)

    # Inline Chart.js
    html = html.replace('<!-- CHARTJS_INLINE -->',
        f'<script>{chartjs_src}</script>\n<script>{adapter_src}</script>')

    output_path = OUTPUT_DIR / 'landing.html'
    output_path.write_text(html, encoding='utf-8')
    print(f"Landing page saved to {output_path}")
    print(f"File size: {output_path.stat().st_size / 1024:.0f} KB")


def build_landing_html(hero, chart_data, annual_data, win_rate, now):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>EFP Wealth — Quantitative Portfolio Management</title>
<!-- CHARTJS_INLINE -->
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #ffffff; color: #1B2A4A; line-height: 1.6; }}
a {{ color: #C59A2C; text-decoration: none; }}
a:hover {{ text-decoration: underline; }}

/* Nav */
.nav {{ background: #1B2A4A; padding: 16px 0; position: sticky; top: 0; z-index: 100; }}
.nav-inner {{ max-width: 1200px; margin: 0 auto; padding: 0 24px; display: flex; justify-content: space-between; align-items: center; }}
.nav-brand {{ color: #C59A2C; font-size: 20px; font-weight: 700; letter-spacing: 0.5px; }}
.nav-brand span {{ color: #ffffff; font-weight: 400; }}
.nav-links {{ display: flex; gap: 24px; align-items: center; }}
.nav-links a {{ color: #cbd5e1; font-size: 14px; font-weight: 500; }}
.nav-links a:hover {{ color: #ffffff; text-decoration: none; }}
.nav-btn {{ background: #C59A2C; color: #1B2A4A; padding: 8px 20px; border-radius: 6px; font-weight: 600; font-size: 14px; }}
.nav-btn:hover {{ background: #d4a93b; text-decoration: none; }}

/* Hero */
.hero {{ background: linear-gradient(135deg, #1B2A4A 0%, #0f1b33 100%); padding: 80px 0 60px; }}
.hero-inner {{ max-width: 1200px; margin: 0 auto; padding: 0 24px; text-align: center; }}
.hero h1 {{ color: #ffffff; font-size: 44px; font-weight: 700; margin-bottom: 16px; line-height: 1.2; }}
.hero h1 em {{ color: #C59A2C; font-style: normal; }}
.hero-sub {{ color: #94a3b8; font-size: 18px; max-width: 700px; margin: 0 auto 40px; }}

/* Metric strip */
.metric-strip {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; max-width: 900px; margin: 0 auto 40px; }}
.metric-box {{ background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; padding: 24px 16px; text-align: center; }}
.metric-box .val {{ font-size: 36px; font-weight: 700; color: #ffffff; }}
.metric-box .val.gold {{ color: #C59A2C; }}
.metric-box .val.green {{ color: #34d399; }}
.metric-box .val.red {{ color: #f87171; }}
.metric-box .lbl {{ font-size: 12px; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 4px; }}
.hero-period {{ color: #475569; font-size: 13px; margin-top: 16px; }}

.hero-cta {{ display: inline-block; background: #C59A2C; color: #1B2A4A; padding: 14px 40px; border-radius: 8px; font-size: 16px; font-weight: 700; margin-top: 32px; transition: background 0.2s; }}
.hero-cta:hover {{ background: #d4a93b; text-decoration: none; }}
.btn-deepdive {{ display: inline-block; background: transparent; color: #C59A2C; border: 2px solid #C59A2C; padding: 12px 36px; border-radius: 8px; font-size: 16px; font-weight: 700; margin-top: 32px; margin-left: 16px; transition: all 0.2s; }}
.btn-deepdive:hover {{ background: #C59A2C; color: #1B2A4A; text-decoration: none; }}
.cta-buttons {{ display: flex; justify-content: center; align-items: center; flex-wrap: wrap; gap: 16px; margin-top: 32px; }}
.cta-buttons .hero-cta, .cta-buttons .btn-deepdive {{ margin-top: 0; margin-left: 0; }}
.deepdive-wrap {{ text-align: center; margin-top: 40px; }}

/* Sections */
.section {{ max-width: 1200px; margin: 0 auto; padding: 80px 24px; }}
.section-alt {{ background: #f8fafc; }}
.section h2 {{ font-size: 32px; font-weight: 700; color: #1B2A4A; margin-bottom: 12px; }}
.section-sub {{ color: #64748b; font-size: 16px; margin-bottom: 40px; max-width: 600px; }}

/* Chart card */
.chart-card {{ background: #0a0e17; border-radius: 16px; padding: 32px; margin-bottom: 40px; border: 1px solid #1e293b; }}
.chart-card h3 {{ color: #94a3b8; font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 16px; }}
canvas {{ max-height: 380px; }}

/* How it works */
.pillars {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 24px; }}
.pillar {{ background: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 32px 24px; text-align: center; transition: box-shadow 0.2s; }}
.pillar:hover {{ box-shadow: 0 8px 30px rgba(27,42,74,0.1); }}
.pillar-icon {{ font-size: 40px; margin-bottom: 16px; }}
.pillar h3 {{ font-size: 18px; font-weight: 700; color: #1B2A4A; margin-bottom: 8px; }}
.pillar p {{ color: #64748b; font-size: 14px; line-height: 1.6; }}

/* Stats row */
.stats-row {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 40px 0; }}
.stat-card {{ background: #1B2A4A; border-radius: 12px; padding: 28px 20px; text-align: center; }}
.stat-card .val {{ font-size: 28px; font-weight: 700; color: #C59A2C; }}
.stat-card .lbl {{ font-size: 12px; color: #94a3b8; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }}

/* Trust signals */
.trust-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; }}
.trust-item {{ display: flex; gap: 16px; align-items: flex-start; padding: 20px; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; }}
.trust-check {{ color: #C59A2C; font-size: 24px; flex-shrink: 0; margin-top: 2px; }}
.trust-item h4 {{ font-size: 15px; font-weight: 600; color: #1B2A4A; margin-bottom: 4px; }}
.trust-item p {{ font-size: 13px; color: #64748b; }}

/* CTA section */
.cta-section {{ background: linear-gradient(135deg, #1B2A4A 0%, #0f1b33 100%); padding: 80px 0; text-align: center; }}
.cta-section h2 {{ color: #ffffff; font-size: 32px; margin-bottom: 12px; }}
.cta-section p {{ color: #94a3b8; font-size: 16px; margin-bottom: 32px; }}

/* Footer */
.footer {{ background: #0f1b33; padding: 40px 0; }}
.footer-inner {{ max-width: 1200px; margin: 0 auto; padding: 0 24px; }}
.footer-top {{ display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 32px; }}
.footer-brand {{ color: #C59A2C; font-size: 18px; font-weight: 700; }}
.footer-brand span {{ color: #94a3b8; font-weight: 400; }}
.footer-links {{ display: flex; gap: 24px; }}
.footer-links a {{ color: #64748b; font-size: 13px; }}
.footer-links a:hover {{ color: #94a3b8; }}
.footer-disc {{ color: #475569; font-size: 11px; line-height: 1.6; border-top: 1px solid #1e293b; padding-top: 24px; }}

/* Mobile */
@media (max-width: 768px) {{
    .hero h1 {{ font-size: 28px; }}
    .hero-sub {{ font-size: 15px; }}
    .metric-strip {{ grid-template-columns: repeat(2, 1fr); gap: 10px; }}
    .metric-box .val {{ font-size: 24px; }}
    .metric-box {{ padding: 16px 12px; }}
    .pillars {{ grid-template-columns: 1fr; }}
    .stats-row {{ grid-template-columns: repeat(2, 1fr); }}
    .trust-grid {{ grid-template-columns: 1fr; }}
    .section h2 {{ font-size: 24px; }}
    .section {{ padding: 48px 16px; }}
    .hero {{ padding: 48px 0 36px; }}
    .nav-links {{ gap: 12px; }}
    .nav-links a {{ font-size: 12px; }}
    .footer-top {{ flex-direction: column; gap: 16px; }}
    canvas {{ max-height: 260px; }}
    .cta-buttons {{ flex-direction: column; gap: 12px; }}
    .cta-buttons .hero-cta, .cta-buttons .btn-deepdive {{ width: 100%; max-width: 300px; text-align: center; }}
}}
@media (max-width: 480px) {{
    .metric-strip {{ grid-template-columns: 1fr 1fr; gap: 8px; }}
    .metric-box .val {{ font-size: 22px; }}
    .stats-row {{ grid-template-columns: 1fr 1fr; gap: 8px; }}
    .hero-cta {{ padding: 12px 28px; font-size: 14px; }}
    .btn-deepdive {{ padding: 10px 24px; font-size: 14px; }}
}}
</style>
</head>
<body>

<!-- Navigation -->
<nav class="nav">
    <div class="nav-inner">
        <div class="nav-brand">EFP <span>Wealth</span></div>
        <div class="nav-links">
            <a href="#performance">Performance</a>
            <a href="#approach">Approach</a>
            <a href="#why-us">Why Us</a>
            <a href="/login" class="nav-btn">Client Login</a>
        </div>
    </div>
</nav>

<!-- Hero -->
<section class="hero">
    <div class="hero-inner">
        <h1>Systematic Alpha for<br><em>Serious Capital</em></h1>
        <p class="hero-sub">
            A quantitative, walk-forward validated multi-factor portfolio for Indian equities.
            Regime-aware allocation. SEBI registered. No guesswork.
        </p>

        <div class="metric-strip">
            <div class="metric-box">
                <div class="val gold">{hero['cagr']}%</div>
                <div class="lbl">CAGR</div>
            </div>
            <div class="metric-box">
                <div class="val">{hero['sharpe']}</div>
                <div class="lbl">Sharpe Ratio</div>
            </div>
            <div class="metric-box">
                <div class="val red">{hero['max_dd']}%</div>
                <div class="lbl">Max Drawdown</div>
            </div>
            <div class="metric-box">
                <div class="val green">+{hero['alpha']}%</div>
                <div class="lbl">Alpha vs NIFTY 50</div>
            </div>
        </div>

        <div class="hero-period">
            Walk-forward out-of-sample results &middot; {hero['period']} ({hero['years']} years)
        </div>

        <a href="/register" class="hero-cta">Request Access</a>
    </div>
</section>

<!-- Performance -->
<section id="performance" class="section">
    <h2>Track Record</h2>
    <p class="section-sub">Growth of INR 100 invested, compared against NIFTY 50 passive benchmark.</p>

    <div class="chart-card">
        <h3>Growth of INR 100 (Log Scale)</h3>
        <canvas id="equityChart"></canvas>
    </div>

    <div class="chart-card">
        <h3>Annual Returns (%)</h3>
        <canvas id="annualChart"></canvas>
    </div>

    <div class="stats-row">
        <div class="stat-card">
            <div class="val">{hero['multiple']}x</div>
            <div class="lbl">Total Return Multiple</div>
        </div>
        <div class="stat-card">
            <div class="val">{hero['calmar']}</div>
            <div class="lbl">Calmar Ratio</div>
        </div>
        <div class="stat-card">
            <div class="val">{hero['beta']}</div>
            <div class="lbl">Beta to NIFTY</div>
        </div>
        <div class="stat-card">
            <div class="val">{win_rate:.0f}%</div>
            <div class="lbl">Monthly Win Rate</div>
        </div>
    </div>

    <div class="deepdive-wrap">
        <a href="/analytics" class="btn-deepdive">Deep Dive into Analytics &rarr;</a>
    </div>
</section>

<!-- How It Works -->
<section id="approach" class="section-alt">
    <div class="section">
        <h2>How It Works</h2>
        <p class="section-sub">Three systematic pillars. No discretion. No emotion.</p>

        <div class="pillars">
            <div class="pillar">
                <div class="pillar-icon">&#x1F4CA;</div>
                <h3>Multi-Factor Stock Selection</h3>
                <p>
                    13 factors across momentum, value, quality, and low volatility.
                    Cross-sectional z-score ranking of 250 eligible stocks from NIFTY LargeMidcap 250.
                    Top 20 positions, factor-weighted, rebalanced monthly.
                </p>
            </div>
            <div class="pillar">
                <div class="pillar-icon">&#x1F6E1;&#xFE0F;</div>
                <h3>Regime-Aware Allocation</h3>
                <p>
                    7-signal ensemble produces a continuous confidence score from -1 (bear) to +1 (bull).
                    In strong bull regimes: 90-100% equity. In bear regimes: shifts to gold and debt.
                    Went to 3% equity during COVID crash.
                </p>
            </div>
            <div class="pillar">
                <div class="pillar-icon">&#x1F527;</div>
                <h3>Walk-Forward Validation</h3>
                <p>
                    17 non-overlapping out-of-sample windows. Factor weights optimized on rolling
                    2-year training windows, tested on unseen 6-month periods. No curve fitting.
                    Survivorship-bias-free constituent data from NSE.
                </p>
            </div>
        </div>
    </div>
</section>

<!-- Why Us -->
<section id="why-us" class="section">
    <h2>Why EFP Wealth</h2>
    <p class="section-sub">Built for capital that demands rigour over narratives.</p>

    <div class="trust-grid">
        <div class="trust-item">
            <div class="trust-check">&#x2713;</div>
            <div>
                <h4>SEBI Registered</h4>
                <p>Fully registered with the Securities and Exchange Board of India. Compliant advisory framework.</p>
            </div>
        </div>
        <div class="trust-item">
            <div class="trust-check">&#x2713;</div>
            <div>
                <h4>No Survivorship Bias</h4>
                <p>Point-in-time constituent data sourced from 133 NSE official monthly PDFs. 338 stocks, 96 snapshots.</p>
            </div>
        </div>
        <div class="trust-item">
            <div class="trust-check">&#x2713;</div>
            <div>
                <h4>Transparent Methodology</h4>
                <p>Complete whitepaper detailing every factor, signal, and decision rule. No black boxes.</p>
            </div>
        </div>
        <div class="trust-item">
            <div class="trust-check">&#x2713;</div>
            <div>
                <h4>Honest Track Record</h4>
                <p>All results are out-of-sample walk-forward. No backtest optimization, no cherry-picking periods.</p>
            </div>
        </div>
    </div>
</section>

<!-- CTA -->
<section class="cta-section">
    <h2>Ready to see the full dashboard?</h2>
    <p>Request access to view live signals, detailed analytics, and monthly portfolio updates.</p>
    <div class="cta-buttons">
        <a href="/register" class="hero-cta">Request Access</a>
        <a href="/analytics" class="btn-deepdive">Deep Dive &rarr;</a>
    </div>
</section>

<!-- Footer -->
<footer class="footer">
    <div class="footer-inner">
        <div class="footer-top">
            <div>
                <div class="footer-brand">EFP <span>Wealth</span></div>
                <div style="color:#475569;font-size:12px;margin-top:4px">Quantitative Portfolio Management</div>
            </div>
            <div class="footer-links">
                <a href="/login">Client Login</a>
                <a href="/disclosure">Disclosure</a>
                <a href="mailto:contact@efpwealth.com">Contact</a>
            </div>
        </div>
        <div class="footer-disc">
            All returns shown are based on walk-forward backtested results using historical data and are hypothetical.
            Past performance does not guarantee future results and is not indicative of future returns.
            Investments in securities are subject to market risks. Read all related documents carefully before investing.
            10 bps round-trip slippage applied. Does not include management fees, taxes, or impact costs beyond slippage.
            <br><br>
            EFP Wealth is SEBI registered. Generated {now}.
        </div>
    </div>
</footer>

<!-- Chart.js data & rendering -->
<script type="application/json" id="chartData">{json.dumps(chart_data)}</script>
<script type="application/json" id="annualData">{json.dumps(annual_data)}</script>
<script>
(function() {{
    var cd = JSON.parse(document.getElementById('chartData').textContent);
    var ad = JSON.parse(document.getElementById('annualData').textContent);

    // Equity curve
    new Chart(document.getElementById('equityChart'), {{
        type: 'line',
        data: {{
            datasets: [
                {{
                    label: 'EFP Wealth Portfolio',
                    data: cd.wf.dates.map(function(d, i) {{ return {{x: d, y: cd.wf.values[i]}}; }}),
                    borderColor: '#C59A2C',
                    borderWidth: 2.5,
                    pointRadius: 0,
                    fill: false,
                }},
                {{
                    label: 'NIFTY 50 (Buy & Hold)',
                    data: cd.nifty.dates.map(function(d, i) {{ return {{x: d, y: cd.nifty.values[i]}}; }}),
                    borderColor: '#64748b',
                    borderWidth: 1.5,
                    pointRadius: 0,
                    fill: false,
                    borderDash: [5, 3],
                }}
            ]
        }},
        options: {{
            responsive: true,
            plugins: {{ legend: {{ labels: {{ color: '#94a3b8', usePointStyle: true, padding: 20 }} }} }},
            scales: {{
                x: {{ type: 'time', time: {{ unit: 'year' }}, grid: {{ color: '#1e293b' }}, ticks: {{ color: '#64748b' }} }},
                y: {{ type: 'logarithmic', grid: {{ color: '#1e293b' }}, ticks: {{ color: '#64748b' }},
                      title: {{ display: true, text: 'INR 100 indexed', color: '#64748b' }} }}
            }}
        }}
    }});

    // Annual returns
    new Chart(document.getElementById('annualChart'), {{
        type: 'bar',
        data: {{
            labels: ad.years,
            datasets: [
                {{
                    label: 'EFP Wealth',
                    data: ad.years.map(function(y) {{ return ad.wf[y] || null; }}),
                    backgroundColor: '#C59A2C',
                    borderRadius: 4,
                }},
                {{
                    label: 'NIFTY 50',
                    data: ad.years.map(function(y) {{ return ad.nifty[y] || null; }}),
                    backgroundColor: '#475569',
                    borderRadius: 4,
                }}
            ]
        }},
        options: {{
            responsive: true,
            plugins: {{ legend: {{ labels: {{ color: '#94a3b8', usePointStyle: true, padding: 20 }} }} }},
            scales: {{
                y: {{ grid: {{ color: '#1e293b' }}, ticks: {{ color: '#64748b', callback: function(v) {{ return v + '%'; }} }} }},
                x: {{ grid: {{ color: '#1e293b' }}, ticks: {{ color: '#64748b' }} }}
            }}
        }}
    }});
}})();
</script>
</body>
</html>"""


if __name__ == "__main__":
    generate_landing()
