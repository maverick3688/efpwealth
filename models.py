"""Database models for EFP Wealth."""

import secrets
import string
from datetime import datetime, timezone

from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin

db = SQLAlchemy()


def generate_referral_code(length=6):
    """Generate a unique referral code like 'EFP-A3K7M2'."""
    chars = string.ascii_uppercase + string.digits
    return 'EFP-' + ''.join(secrets.choice(chars) for _ in range(length))


class User(UserMixin, db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    approved = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # Profile & KYC
    phone = db.Column(db.String(20), nullable=True)
    pan_number = db.Column(db.String(10), nullable=True)
    kyc_status = db.Column(db.String(20), default='pending')       # pending | submitted | verified
    risk_profile = db.Column(db.String(20), default='moderate')    # conservative | moderate | aggressive

    # Referral
    referral_code = db.Column(db.String(12), unique=True, nullable=True)
    referred_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)

    # Terms acceptance
    terms_accepted_at = db.Column(db.DateTime, nullable=True)      # NULL = not accepted
    terms_version = db.Column(db.String(10), nullable=True)        # e.g. "1.0"

    # Relationships
    capital_records = db.relationship('CapitalRecord', backref='user', lazy='dynamic')
    referrals_made = db.relationship('User', backref=db.backref('referrer', remote_side=[id]))
    sent_referrals = db.relationship('Referral', backref='referrer_user', lazy='dynamic')

    def __repr__(self):
        return f'<User {self.email}>'


class CapitalRecord(db.Model):
    """Monthly snapshots of a client's capital deployed and portfolio value."""
    __tablename__ = 'capital_records'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    date = db.Column(db.Date, nullable=False)
    invested = db.Column(db.Float, nullable=False)         # total capital deployed (cumulative)
    current_value = db.Column(db.Float, nullable=False)    # current portfolio value
    note = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint('user_id', 'date', name='uq_user_date'),
    )

    def __repr__(self):
        return f'<Capital {self.user_id} {self.date}: {self.invested}→{self.current_value}>'


class Referral(db.Model):
    """Tracks referral invitations and their status."""
    __tablename__ = 'referrals'

    id = db.Column(db.Integer, primary_key=True)
    referrer_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    referred_email = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(20), default='invited')   # invited | registered | approved
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    def __repr__(self):
        return f'<Referral {self.referred_email} [{self.status}]>'
