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
                import subprocess
                script = GREYSKY_DIR / 'download_data.py'
                if not script.exists():
                    return False, f'download_data.py not found at {script}'
                result = subprocess.run(
                    [sys.executable, str(script)],
                    cwd=str(GREYSKY_DIR), capture_output=True, text=True,
                    timeout=120, encoding='utf-8', errors='replace'
                )
                if result.returncode != 0:
                    return False, f'Download failed: {result.stderr[-500:]}'
                return True, 'Downloaded market data'

        elif step_key == 'portfolio':
            import subprocess
            script = CHECKPOINT_DIR / 'daily_update.py'
            if not script.exists():
                return False, f'daily_update.py not found at {script}'
            result = subprocess.run(
                [sys.executable, str(script)],
                cwd=str(CHECKPOINT_DIR), capture_output=True, text=True,
                timeout=300, encoding='utf-8', errors='replace'
            )
            if result.returncode != 0:
                return False, f'Portfolio update failed: {result.stderr[-500:]}'
            return True, 'Portfolio updated'

        elif step_key == 'dashboard':
            import subprocess
            script = CHECKPOINT_DIR / 'generate_dashboard.py'
            if not script.exists():
                return True, 'Dashboard script not found (skipped)'
            result = subprocess.run(
                [sys.executable, str(script)],
                cwd=str(CHECKPOINT_DIR), capture_output=True, text=True,
                timeout=120, encoding='utf-8', errors='replace'
            )
            if result.returncode != 0:
                return False, f'Dashboard failed: {result.stderr[-500:]}'
            return True, 'Dashboard regenerated'

        elif step_key == 'monthly':
            import subprocess
            script = GREYSKY_DIR / 'generate_monthly_report.py'
            if not script.exists():
                return True, 'Monthly report script not found (skipped)'
            result = subprocess.run(
                [sys.executable, str(script)],
                cwd=str(GREYSKY_DIR), capture_output=True, text=True,
                timeout=120, encoding='utf-8', errors='replace'
            )
            if result.returncode != 0:
                return False, f'Monthly report failed: {result.stderr[-500:]}'
            return True, 'Monthly report regenerated'

        elif step_key == 'metrics':
            import subprocess
            script = WEB_DIR / 'generate_site_data.py'
            if not script.exists():
                return True, 'Site metrics script not found (skipped)'
            result = subprocess.run(
                [sys.executable, str(script)],
                cwd=str(WEB_DIR), capture_output=True, text=True,
                timeout=60, encoding='utf-8', errors='replace'
            )
            if result.returncode != 0:
                return False, f'Site metrics failed: {result.stderr[-500:]}'
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
            import subprocess
            today = datetime.now().strftime('%Y-%m-%d')

            # Stage changed web files
            files_to_stage = [
                'static/dashboard.html', 'static/monthly_report.html',
                'data/site_metrics.json', 'data/current_signals.json',
                'data/pipeline_status.json',
            ]
            subprocess.run(
                ['git', 'add'] + files_to_stage,
                cwd=str(WEB_DIR), capture_output=True, text=True,
                encoding='utf-8', errors='replace'
            )

            # Check if there are changes to commit
            status_result = subprocess.run(
                ['git', 'status', '--porcelain'],
                cwd=str(WEB_DIR), capture_output=True, text=True,
                encoding='utf-8', errors='replace'
            )
            if not status_result.stdout.strip():
                return True, 'No changes to deploy'

            # Commit
            commit_msg = f'Daily update {today} ({mode})'
            result = subprocess.run(
                ['git', 'commit', '-m', commit_msg],
                cwd=str(WEB_DIR), capture_output=True, text=True,
                encoding='utf-8', errors='replace'
            )
            if result.returncode != 0:
                return False, f'Git commit failed: {result.stderr}'

            # Push to remote
            result = subprocess.run(
                ['git', 'push'],
                cwd=str(WEB_DIR), capture_output=True, text=True,
                timeout=60, encoding='utf-8', errors='replace'
            )
            if result.returncode != 0:
                return False, f'Git push failed: {result.stderr}'

            return True, f'Deployed to remote ({commit_msg})'

        elif step_key == 'reload_remote':
            # Pull changes and reload webapp on PythonAnywhere via API
            config_path = WEB_DIR / 'pa_config.json'
            if not config_path.exists():
                return True, 'Skipped remote reload (no pa_config.json)'

            config = json.loads(config_path.read_text(encoding='utf-8'))
            pa_user = config.get('username', '')
            pa_token = config.get('api_token', '')
            pa_domain = config.get('domain', '')

            if not all([pa_user, pa_token, pa_domain]):
                return True, 'Skipped remote reload (incomplete pa_config.json)'

            import urllib.request
            import urllib.error
            headers = {'Authorization': f'Token {pa_token}'}

            # 1. Run git pull via PythonAnywhere API (create console + run command)
            try:
                # Use the files API to trigger a bash command via console
                pull_url = f'https://www.pythonanywhere.com/api/v0/user/{pa_user}/consoles/'
                pull_data = json.dumps({
                    'executable': 'bash',
                    'arguments': '',
                    'working_directory': f'/home/{pa_user}/efpwealth'
                }).encode('utf-8')
                req = urllib.request.Request(pull_url, data=pull_data, headers={
                    **headers, 'Content-Type': 'application/json'
                }, method='POST')
                resp = urllib.request.urlopen(req, timeout=30)
                console_info = json.loads(resp.read().decode('utf-8'))
                console_id = console_info.get('id')

                if console_id:
                    # Send git pull command
                    send_url = f'https://www.pythonanywhere.com/api/v0/user/{pa_user}/consoles/{console_id}/send_input/'
                    send_data = json.dumps({
                        'input': f'cd /home/{pa_user}/efpwealth && git pull origin master\n'
                    }).encode('utf-8')
                    req = urllib.request.Request(send_url, data=send_data, headers={
                        **headers, 'Content-Type': 'application/json'
                    }, method='POST')
                    urllib.request.urlopen(req, timeout=30)

                    # Wait a bit for pull to complete
                    import time
                    time.sleep(5)

                    # Kill the console
                    kill_url = f'https://www.pythonanywhere.com/api/v0/user/{pa_user}/consoles/{console_id}/'
                    req = urllib.request.Request(kill_url, headers=headers, method='DELETE')
                    urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                # Non-fatal: log but continue to reload
                log_msg = f'Git pull warning: {str(e)}'

            # 2. Reload webapp
            try:
                reload_url = f'https://www.pythonanywhere.com/api/v0/user/{pa_user}/webapps/{pa_domain}/reload/'
                req = urllib.request.Request(reload_url, data=b'', headers=headers, method='POST')
                resp = urllib.request.urlopen(req, timeout=30)
                return True, f'Remote reloaded ({pa_domain})'
            except urllib.error.HTTPError as e:
                return False, f'Reload failed: HTTP {e.code} - {e.read().decode("utf-8", errors="replace")}'
            except Exception as e:
                return False, f'Reload failed: {str(e)}'

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
    ('deploy', 'Git commit & push'),
    ('reload_remote', 'Reloading remote webapp'),
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
