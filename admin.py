"""
Admin Portal — Blueprint for EFP Wealth
User management, pipeline control, and system status.
"""

import json
import threading
from pathlib import Path
from functools import wraps
from datetime import datetime, timezone

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, abort
from flask_login import current_user, login_required

from models import db, User, Referral

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

DATA_DIR = Path(__file__).parent / 'data'
PIPELINE_STATUS_FILE = DATA_DIR / 'pipeline_status.json'

_pipeline_lock = threading.Lock()


# --- Helpers ---

def _load_json(path):
    """Load JSON file, returning empty dict if missing."""
    try:
        return json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    except Exception:
        return {}


def _read_pipeline_status():
    """Read current pipeline status."""
    return _load_json(PIPELINE_STATUS_FILE) or {'state': 'idle'}


# --- Decorator ---

def admin_required(f):
    """Require authenticated admin user."""
    @wraps(f)
    @login_required
    def decorated(*args, **kwargs):
        if not getattr(current_user, 'is_admin', False):
            abort(403)
        return f(*args, **kwargs)
    return decorated


# --- Routes ---

@admin_bp.route('/')
@admin_required
def dashboard():
    """Admin overview — user counts, pipeline status, last update."""
    total_users = User.query.count()
    pending_users = User.query.filter_by(approved=False).count()
    approved_users = User.query.filter_by(approved=True).count()

    # Recent pending users for quick approve
    pending_list = User.query.filter_by(approved=False)\
        .order_by(User.created_at.desc()).limit(5).all()

    # Pipeline status
    pipeline_status = _read_pipeline_status()

    # Last update from site_metrics
    metrics = _load_json(DATA_DIR / 'site_metrics.json')
    last_update = metrics.get('generated_at', 'Never')

    return render_template('admin/dashboard.html',
        total_users=total_users,
        pending_users=pending_users,
        approved_users=approved_users,
        pending_list=pending_list,
        pipeline_status=pipeline_status,
        last_update=last_update,
    )


@admin_bp.route('/users')
@admin_required
def users():
    """User management — list all users with approve/revoke controls."""
    all_users = User.query.order_by(User.created_at.desc()).all()
    return render_template('admin/users.html', users=all_users)


@admin_bp.route('/users/<int:user_id>/approve', methods=['POST'])
@admin_required
def approve_user(user_id):
    """Approve a pending user."""
    user = db.session.get(User, user_id)
    if user:
        user.approved = True
        # Update referral status if referred
        if user.referred_by:
            ref = Referral.query.filter_by(
                referrer_id=user.referred_by, referred_email=user.email
            ).first()
            if ref:
                ref.status = 'approved'
        db.session.commit()
        flash(f'Approved {user.name} ({user.email})', 'success')
    else:
        flash('User not found.', 'error')
    # Redirect back to wherever the request came from
    next_url = request.form.get('next', url_for('admin.users'))
    return redirect(next_url)


@admin_bp.route('/users/<int:user_id>/revoke', methods=['POST'])
@admin_required
def revoke_user(user_id):
    """Revoke user access."""
    user = db.session.get(User, user_id)
    if user and user.id != current_user.id:  # Prevent self-revocation
        user.approved = False
        db.session.commit()
        flash(f'Revoked access for {user.name}', 'warning')
    elif user and user.id == current_user.id:
        flash('Cannot revoke your own access.', 'error')
    return redirect(url_for('admin.users'))


@admin_bp.route('/pipeline')
@admin_required
def pipeline():
    """Pipeline control panel."""
    pipeline_status = _read_pipeline_status()
    metrics = _load_json(DATA_DIR / 'site_metrics.json')
    last_update = metrics.get('generated_at', 'Never')
    return render_template('admin/pipeline.html',
        pipeline_status=pipeline_status,
        last_update=last_update,
    )


@admin_bp.route('/pipeline/run', methods=['POST'])
@admin_required
def pipeline_run():
    """Trigger daily update pipeline."""
    if not _pipeline_lock.acquire(blocking=False):
        flash('Pipeline is already running. Please wait for it to complete.', 'warning')
        return redirect(url_for('admin.pipeline'))

    mode = request.form.get('mode', 'daily')
    from_date = request.form.get('from_date', '').strip() or None
    skip_download = 'skip_download' in request.form

    def _run():
        try:
            from pipeline_runner import run_pipeline
            run_pipeline(
                mode=mode,
                from_date=from_date,
                skip_download=skip_download,
            )
        except Exception as e:
            # Write error status
            status = {
                'state': 'error',
                'error': str(e),
                'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            }
            PIPELINE_STATUS_FILE.write_text(
                json.dumps(status, indent=2), encoding='utf-8'
            )
        finally:
            _pipeline_lock.release()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    if mode == 'rerun' and from_date:
        flash(f'Pipeline started — re-running from {from_date}. Progress will update below.', 'success')
    elif mode == 'midday':
        flash('Mid-day update started with near-real-time prices.', 'success')
    else:
        flash('Daily pipeline started. Progress will update below.', 'success')

    return redirect(url_for('admin.pipeline'))


@admin_bp.route('/pipeline/status')
@admin_required
def pipeline_status():
    """AJAX endpoint returning pipeline progress as JSON."""
    return jsonify(_read_pipeline_status())
