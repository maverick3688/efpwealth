"""
Pipeline Runner — executes daily update steps as Python functions.
Designed to run in a background thread from the Flask admin panel.
Writes progress to pipeline_status.json for live AJAX polling.
"""

import os
import sys
import json
import shutil
import traceback
from pathlib import Path
from datetime import datetime, timezone

# --- Path configuration ---
# These paths work whether running locally (Windows) or on PythonAnywhere
WEB_DIR = Path(__file__).parent
GREYSKY_DIR = WEB_DIR.parent  # C:/TradingData/greysky (local) or /home/efpwealth (PA)
CHECKPOINT_DIR = GREYSKY_DIR / 'checkpoint_v8'
DATA_DIR = GREYSKY_DIR / 'data'
STATUS_FILE = WEB_DIR / 'data' / 'pipeline_status.json'

# Detect if running on PythonAnywhere (no checkpoint_v8 directory)
HAS_FULL_CODEBASE = CHECKPOINT_DIR.exists() and DATA_DIR.exists()

# Ensure checkpoint_v8 is importable (only if available)
if HAS_FULL_CODEBASE:
    if str(CHECKPOINT_DIR) not in sys.path:
        sys.path.insert(0, str(CHECKPOINT_DIR))
    if str(GREYSKY_DIR) not in sys.path:
        sys.path.insert(0, str(GREYSKY_DIR))


# =============================================================================
# STATUS FILE MANAGEMENT
# =============================================================================

def _write_status(status):
    """Write pipeline status to JSON for polling."""
    status['updated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATUS_FILE.write_text(json.dumps(status, indent=2), encoding='utf-8')


def read_status():
    """Read current pipeline status."""
    try:
        if STATUS_FILE.exists():
            return json.loads(STATUS_FILE.read_text(encoding='utf-8'))
    except Exception:
        pass
    return {'state': 'idle'}


# =============================================================================
# CHECKPOINT BACKUP & ROLLBACK
# =============================================================================

CHECKPOINT_NAMES = [
    'checkpoint_WalkForward.json',
    'checkpoint_StockPick_100Eq.json',
    'checkpoint_Gold50_Stock50.json',
    'regime_n250_state.json',
]


def _backup_checkpoints():
    """Backup current checkpoints before a pipeline run."""
    for name in CHECKPOINT_NAMES:
        src = DATA_DIR / name
        dst = DATA_DIR / name.replace('.json', '_backup.json')
        if src.exists():
            shutil.copy2(src, dst)


def _restore_checkpoint_backups():
    """Restore checkpoints from backup files."""
    for name in CHECKPOINT_NAMES:
        backup = DATA_DIR / name.replace('.json', '_backup.json')
        dst = DATA_DIR / name
        if backup.exists():
            shutil.copy2(backup, dst)


def _rollback_data_to_date(target_date_str):
    """
    Roll back CSVs and checkpoints to just before target_date.
    This enables daily_update to re-process from target_date onward.
    """
    import pandas as pd
    target = pd.Timestamp(target_date_str)

    # 1. Truncate all_equity_curves.csv
    curves_path = DATA_DIR / 'all_equity_curves.csv'
    if curves_path.exists():
        curves = pd.read_csv(curves_path, index_col=0, parse_dates=True)
        curves = curves[curves.index < target]
        curves.to_csv(curves_path)

    # 2. Truncate wf_equity.csv
    eq_path = DATA_DIR / 'wf_equity.csv'
    if eq_path.exists():
        eq = pd.read_csv(eq_path, index_col=0, parse_dates=True)
        eq = eq[eq.index < target]
        eq.to_csv(eq_path)

    # 3. Truncate wf_trades.csv (remove trades on or after target)
    trades_path = DATA_DIR / 'wf_trades.csv'
    if trades_path.exists():
        trades = pd.read_csv(trades_path)
        if 'date' in trades.columns:
            trades = trades[pd.to_datetime(trades['date']) < target]
        trades.to_csv(trades_path, index=False)

    # 4. Truncate wf_allocations.csv
    allocs_path = DATA_DIR / 'wf_allocations.csv'
    if allocs_path.exists():
        allocs = pd.read_csv(allocs_path)
        if 'date' in allocs.columns:
            allocs = allocs[pd.to_datetime(allocs['date']) < target]
        allocs.to_csv(allocs_path, index=False)

    # 5. Truncate wf_holdings.csv
    holdings_path = DATA_DIR / 'wf_holdings.csv'
    if holdings_path.exists():
        holdings = pd.read_csv(holdings_path)
        if 'date' in holdings.columns:
            holdings = holdings[pd.to_datetime(holdings['date']) < target]
        holdings.to_csv(holdings_path, index=False)

    # 6. Restore checkpoint backups (which are from before we truncated)
    _restore_checkpoint_backups()


# =============================================================================
# MID-DAY DATA DOWNLOAD (nsepython)
# =============================================================================

def _download_midday_data():
    """
    Fetch near-real-time prices from NSE for portfolio stocks.
    Appends/updates today's row in stock parquet files.
    Returns number of stocks updated.
    """
    import pandas as pd

    stock_dir = DATA_DIR / 'stocks'
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    updated = 0

    # Get list of symbols from current signals
    signals_path = DATA_DIR / 'current_signals.json'
    if signals_path.exists():
        signals = json.loads(signals_path.read_text(encoding='utf-8'))
        symbols = [s['symbol'] for s in signals.get('portfolio', [])]
    else:
        # Fallback: load from constituents
        const_path = DATA_DIR / 'nifty250_constituents.csv'
        if const_path.exists():
            import pandas as pd
            df = pd.read_csv(const_path)
            symbols = df['Symbol'].tolist()[:30]  # Top 30 only for speed
        else:
            return 0

    try:
        from nsepython import nse_quote_ltp
    except ImportError:
        # Fallback to yfinance if nsepython not available
        try:
            import yfinance as yf
            for sym in symbols:
                try:
                    ticker = yf.Ticker(f"{sym}.NS")
                    hist = ticker.history(period='1d')
                    if len(hist) > 0:
                        row = hist.iloc[-1]
                        parquet_path = stock_dir / f'{sym}.parquet'
                        if parquet_path.exists():
                            df = pd.read_parquet(parquet_path)
                            if today not in df.index:
                                new_row = pd.DataFrame({
                                    'Open': [row['Open']], 'High': [row['High']],
                                    'Low': [row['Low']], 'Close': [row['Close']],
                                    'Volume': [int(row['Volume'])]
                                }, index=[today])
                                df = pd.concat([df, new_row])
                            else:
                                df.loc[today, 'Close'] = row['Close']
                                df.loc[today, 'High'] = max(df.loc[today, 'High'], row['High'])
                                df.loc[today, 'Low'] = min(df.loc[today, 'Low'], row['Low'])
                            df.to_parquet(parquet_path)
                            updated += 1
                except Exception:
                    continue
        except ImportError:
            pass
        return updated

    # Use nsepython for near-real-time NSE data
    for sym in symbols:
        try:
            ltp = nse_quote_ltp(sym)
            if ltp and ltp > 0:
                parquet_path = stock_dir / f'{sym}.parquet'
                if parquet_path.exists():
                    df = pd.read_parquet(parquet_path)
                    if today not in df.index:
                        new_row = pd.DataFrame({
                            'Open': [ltp], 'High': [ltp], 'Low': [ltp],
                            'Close': [ltp], 'Volume': [0]
                        }, index=[today])
                        df = pd.concat([df, new_row])
                    else:
                        df.loc[today, 'Close'] = ltp
                        df.loc[today, 'High'] = max(df.loc[today, 'High'], ltp)
                        df.loc[today, 'Low'] = min(df.loc[today, 'Low'], ltp)
                    df.to_parquet(parquet_path)
                    updated += 1
        except Exception:
            continue

    # Also update ETFs
    for nse_sym, filename in [('NIFTY 50', 'NIFTY50'), ('GOLDBEES', 'GOLDBEES')]:
        try:
            ltp = nse_quote_ltp(nse_sym)
            if ltp and ltp > 0:
                parquet_path = DATA_DIR / 'etfs' / f'{filename}.parquet'
                if parquet_path.exists():
                    df = pd.read_parquet(parquet_path)
                    if today not in df.index:
                        new_row = pd.DataFrame({
                            'Open': [ltp], 'High': [ltp], 'Low': [ltp],
                            'Close': [ltp], 'Volume': [0]
                        }, index=[today])
                        df = pd.concat([df, new_row])
                    else:
                        df.loc[today, 'Close'] = ltp
                    df.to_parquet(parquet_path)
        except Exception:
            continue

    return updated


# =============================================================================
# PIPELINE STEP EXECUTION
# =============================================================================

def _run_step(step_key, mode='daily'):
    """Execute a single pipeline step. Returns (success, message)."""
    original_cwd = os.getcwd()

    try:
        if step_key == 'download':
            if mode == 'midday':
                n = _download_midday_data()
                return True, f'Updated {n} stock prices (mid-day)'
            else:
                os.chdir(str(GREYSKY_DIR))
                import download_data
                # Reload module in case it was imported before with stale state
                import importlib
                importlib.reload(download_data)
                symbols, _ = download_data.download_constituents()
                download_data.download_stock_data(symbols)
                download_data.download_etf_and_index_data()
                return True, f'Downloaded data for {len(symbols)} stocks + ETFs'

        elif step_key == 'portfolio':
            os.chdir(str(CHECKPOINT_DIR))
            import daily_update
            import importlib
            importlib.reload(daily_update)
            daily_update.run_daily_update(dry_run=False)
            return True, 'Portfolio updated'

        elif step_key == 'dashboard':
            os.chdir(str(CHECKPOINT_DIR))
            import generate_dashboard
            import importlib
            importlib.reload(generate_dashboard)
            generate_dashboard.generate_dashboard()
            return True, 'Dashboard regenerated'

        elif step_key == 'monthly':
            monthly_script = GREYSKY_DIR / 'generate_monthly_report.py'
            if monthly_script.exists():
                os.chdir(str(GREYSKY_DIR))
                import generate_monthly_report
                import importlib
                importlib.reload(generate_monthly_report)
                generate_monthly_report.generate()
                return True, 'Monthly report regenerated'
            return True, 'Monthly report skipped (script not found)'

        elif step_key == 'metrics':
            os.chdir(str(WEB_DIR))
            import generate_site_data
            import importlib
            importlib.reload(generate_site_data)
            generate_site_data.generate()
            return True, 'Site metrics regenerated'

        elif step_key == 'copy':
            copies = [
                (GREYSKY_DIR / 'dashboard.html', WEB_DIR / 'static' / 'dashboard.html'),
                (GREYSKY_DIR / 'monthly_report.html', WEB_DIR / 'static' / 'monthly_report.html'),
                (DATA_DIR / 'current_signals.json', WEB_DIR / 'data' / 'current_signals.json'),
            ]
            copied = 0
            for src, dst in copies:
                if src.exists():
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)
                    copied += 1
            return True, f'Copied {copied} files to web/'

        elif step_key == 'deploy':
            # On PythonAnywhere, files are already in the right place
            # On local, this would do git commit+push
            return True, 'Deploy step (files already in place)'

        return False, f'Unknown step: {step_key}'

    except Exception as e:
        return False, f'{str(e)}\n{traceback.format_exc()}'
    finally:
        os.chdir(original_cwd)


# =============================================================================
# MAIN PIPELINE
# =============================================================================

PIPELINE_STEPS = [
    ('download', 'Downloading market data'),
    ('portfolio', 'Running portfolio update'),
    ('dashboard', 'Regenerating dashboard'),
    ('monthly', 'Regenerating monthly report'),
    ('metrics', 'Regenerating site metrics'),
    ('copy', 'Copying files to web'),
]


def run_pipeline(mode='daily', from_date=None, skip_download=False):
    """
    Run the daily update pipeline.

    mode: 'daily' | 'rerun' | 'midday'
    from_date: str 'YYYY-MM-DD' — if mode=='rerun', rollback to this date first
    skip_download: bool — skip the data download step
    """
    started_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    log = []
    total = len(PIPELINE_STEPS)

    # Check if full codebase is available
    if not HAS_FULL_CODEBASE:
        missing = []
        if not CHECKPOINT_DIR.exists():
            missing.append(f'checkpoint_v8 ({CHECKPOINT_DIR})')
        if not DATA_DIR.exists():
            missing.append(f'data ({DATA_DIR})')
        error_msg = (
            f'Pipeline requires the full codebase. Missing: {", ".join(missing)}. '
            f'Run the pipeline locally using run_daily.py, then deploy via git push.'
        )
        _write_status({
            'state': 'error',
            'mode': mode,
            'error': error_msg,
            'started_at': started_at,
            'log': [f'[ERROR] {error_msg}'],
        })
        return

    _write_status({
        'state': 'running',
        'mode': mode,
        'step': 0,
        'total': total,
        'current_step': 'Initializing...',
        'started_at': started_at,
        'log': log,
    })

    try:
        # Step 0: Backup checkpoints
        _backup_checkpoints()
        log.append('[OK] Checkpoints backed up')

        # Step 0b: If rerun, rollback data first
        if mode == 'rerun' and from_date:
            _write_status({
                'state': 'running', 'mode': mode,
                'step': 0, 'total': total,
                'current_step': f'Rolling back to {from_date}...',
                'started_at': started_at, 'log': log,
            })
            _rollback_data_to_date(from_date)
            log.append(f'[OK] Rolled back data to before {from_date}')

        # Run each step
        for i, (step_key, step_name) in enumerate(PIPELINE_STEPS):
            # Skip download if requested
            if skip_download and step_key == 'download':
                log.append(f'[SKIP] {step_name}')
                continue

            _write_status({
                'state': 'running', 'mode': mode,
                'step': i + 1, 'total': total,
                'current_step': step_name,
                'started_at': started_at, 'log': log,
            })

            ok, msg = _run_step(step_key, mode)
            status_text = 'OK' if ok else 'FAIL'
            log.append(f'[{status_text}] {step_name}: {msg}')

            # Abort on critical failures
            if not ok and step_key in ('download', 'portfolio'):
                raise RuntimeError(f'{step_name} failed: {msg}')

        _write_status({
            'state': 'completed',
            'mode': mode,
            'step': total, 'total': total,
            'current_step': 'All steps completed',
            'started_at': started_at,
            'completed_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'log': log,
        })

    except Exception as e:
        log.append(f'[ERROR] {str(e)}')
        _write_status({
            'state': 'error',
            'mode': mode,
            'error': str(e),
            'started_at': started_at,
            'log': log,
        })
        raise
