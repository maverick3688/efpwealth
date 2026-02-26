"""
EFP Wealth — Site Data Generator
Reads equity curves from walk-forward results and outputs site_metrics.json
for use by Flask templates. Replaces the old generate_landing.py approach
of baking data into a monolithic HTML file.
"""

import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

DATA_DIR = Path("C:/TradingData/greysky/data")
OUTPUT_DIR = Path(__file__).parent / "data"


def compute_drawdown(equity):
    peak = equity.cummax()
    return (equity - peak) / peak


def compute_annual_returns(equity):
    annual = equity.resample('YE').last().pct_change().dropna()
    return {int(d.year): round(v * 100, 1) for d, v in annual.items()}


def compute_monthly_returns(equity):
    monthly = equity.resample('ME').last().pct_change().dropna()
    return {d.strftime('%Y-%m'): round(v * 100, 2) for d, v in monthly.items()}


def generate():
    print("Generating site_metrics.json...")

    eq_path = DATA_DIR / 'all_equity_curves.csv'
    curves = pd.read_csv(eq_path, index_col=0, parse_dates=True)

    wf = curves['WalkForward'].dropna()
    nifty = curves['NIFTY_100pct'].loc[wf.index[0]:wf.index[-1]].dropna()

    # --- Core metrics ---
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

    monthly_wf = wf.resample('ME').last().pct_change().dropna()
    monthly_nifty = nifty.resample('ME').last().pct_change().dropna()
    common_m = monthly_wf.index.intersection(monthly_nifty.index)
    cov = np.cov(monthly_wf.loc[common_m].values, monthly_nifty.loc[common_m].values)
    beta = cov[0, 1] / cov[1, 1] if cov[1, 1] > 0 else 0

    win_rate = round((monthly_wf.loc[common_m] > 0).mean() * 100, 0)

    hero = {
        'cagr': round(cagr * 100, 1),
        'max_dd': round(max_dd * 100, 1),
        'sharpe': round(sharpe, 2),
        'calmar': round(calmar, 2),
        'alpha': round(alpha * 100, 1),
        'beta': round(beta, 2),
        'multiple': round(multiple, 1),
        'win_rate': int(win_rate),
        'period_start': wf_start.strftime('%b %Y'),
        'period_end': wf_end.strftime('%b %Y'),
        'years': round(years, 1),
        'trades': 2722,
        'windows': 17,
    }

    # --- Equity curve (weekly, normalized to 100) ---
    norm_wf = wf / wf.iloc[0] * 100
    norm_nifty = nifty / nifty.iloc[0] * 100
    wf_weekly = norm_wf.resample('W').last().dropna()
    nifty_weekly = norm_nifty.resample('W').last().dropna()

    equity_curve = {
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

    annual_returns = {
        'years': all_years,
        'wf': {str(k): v for k, v in wf_annual.items()},
        'nifty': {str(k): v for k, v in nifty_annual.items()},
    }

    # --- Monthly returns (for heatmap on performance page) ---
    monthly_returns = compute_monthly_returns(wf)

    # --- Drawdown series (weekly) ---
    dd_weekly = dd.resample('W').last().dropna()
    drawdown = {
        'dates': [d.strftime('%Y-%m-%d') for d in dd_weekly.index],
        'values': [round(v * 100, 1) for v in dd_weekly.values],
    }

    # --- Nifty metrics for comparison ---
    nifty_dd = compute_drawdown(nifty)
    nifty_rets_daily = nifty.pct_change().dropna()
    nifty_sharpe = nifty_rets_daily.mean() / nifty_rets_daily.std() * np.sqrt(252) if nifty_rets_daily.std() > 0 else 0
    nifty_max_dd = nifty_dd.min()
    nifty_multiple = nifty.iloc[-1] / nifty.iloc[0]

    benchmark = {
        'cagr': round(nifty_cagr * 100, 1),
        'sharpe': round(nifty_sharpe, 2),
        'max_dd': round(nifty_max_dd * 100, 1),
        'multiple': round(nifty_multiple, 1),
    }

    # --- Assemble output ---
    site_data = {
        'hero': hero,
        'benchmark': benchmark,
        'equity_curve': equity_curve,
        'annual_returns': annual_returns,
        'monthly_returns': monthly_returns,
        'drawdown': drawdown,
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M'),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / 'site_metrics.json'
    output_path.write_text(json.dumps(site_data, indent=2), encoding='utf-8')
    size_kb = output_path.stat().st_size / 1024
    print(f"Written {output_path} ({size_kb:.0f} KB)")
    print(f"Hero: CAGR={hero['cagr']}%, Sharpe={hero['sharpe']}, MDD={hero['max_dd']}%, Alpha={hero['alpha']}%")


if __name__ == '__main__':
    generate()
