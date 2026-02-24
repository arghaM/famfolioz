"""Page-rendering routes (HTML pages) blueprint."""

from flask import Blueprint, render_template, redirect, url_for, jsonify, request

from cas_parser.webapp import data as db
from cas_parser.webapp.auth import (
    admin_required, check_investor_access,
    get_investor_id_for_folio, get_investor_id_for_nps_subscriber,
)

pages_bp = Blueprint('pages', __name__)


@pages_bp.route('/')
def index():
    """Render the dashboard page."""
    family_name = db.get_config('family_name', 'Our Family')
    return render_template('dashboard.html', family_name=family_name)


@pages_bp.route('/upload')
@admin_required
def upload():
    """Render the upload page."""
    return render_template('upload.html')


@pages_bp.route('/investor/<int:investor_id>')
def investor_detail(investor_id):
    """Render investor detail page."""
    check_investor_access(investor_id)
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return redirect(url_for('pages.index'))
    return render_template('investor.html', investor=investor)


@pages_bp.route('/folio/<int:folio_id>')
def folio_detail(folio_id):
    """Render folio/investment detail page."""
    investor_id = get_investor_id_for_folio(folio_id)
    if investor_id:
        check_investor_access(investor_id)
    return render_template('folio.html', folio_id=folio_id)


@pages_bp.route('/map-folios')
@admin_required
def map_folios_page():
    """Render folio mapping page."""
    return render_template('map_folios.html')


@pages_bp.route('/mutual-funds')
def mutual_funds_page():
    """Render mutual fund master page."""
    return render_template('mutual_funds.html')


@pages_bp.route('/resolve-conflicts')
@admin_required
def resolve_conflicts_page():
    """Render conflict resolution page."""
    return render_template('resolve_conflicts.html')


@pages_bp.route('/investor/<int:investor_id>/goals')
def goals_page(investor_id):
    """Render goals page for an investor."""
    check_investor_access(investor_id)
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return redirect(url_for('pages.index'))
    return render_template('goals.html', investor=investor)


@pages_bp.route('/investor/<int:investor_id>/tax-harvesting')
def tax_harvesting_page(investor_id):
    """Render tax-loss harvesting page for an investor."""
    check_investor_access(investor_id)
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return redirect(url_for('pages.index'))
    return render_template('tax_harvesting.html', investor=investor)


@pages_bp.route('/settings')
@admin_required
def settings_page():
    """Render settings/admin page."""
    return render_template('settings.html')


@pages_bp.route('/settings/backup')
@admin_required
def backup_page():
    """Render dedicated backup & restore page."""
    return render_template('backup.html')


@pages_bp.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'ok'})


@pages_bp.route('/manual-assets')
def page_manual_assets():
    """Manual assets management page."""
    investor_id = request.args.get('investor_id', type=int)
    if not investor_id:
        return redirect('/')
    check_investor_access(investor_id)
    return render_template('manual_assets.html', investor_id=investor_id)


@pages_bp.route('/nps')
def page_nps():
    """NPS management page."""
    return render_template('nps.html')


@pages_bp.route('/nps/<int:subscriber_id>')
def page_nps_subscriber(subscriber_id):
    """NPS subscriber detail page."""
    inv_id = get_investor_id_for_nps_subscriber(subscriber_id)
    if inv_id:
        check_investor_access(inv_id)
    subscriber = db.get_nps_subscriber(subscriber_id=subscriber_id)
    if not subscriber:
        return redirect('/nps')
    return render_template('nps_subscriber.html', subscriber=subscriber)
