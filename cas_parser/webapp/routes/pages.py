"""Page-rendering routes (HTML pages) blueprint."""

from flask import Blueprint, render_template, redirect, url_for, jsonify, request

from cas_parser.webapp import data as db

pages_bp = Blueprint('pages', __name__)


@pages_bp.route('/')
def index():
    """Render the dashboard page."""
    family_name = db.get_config('family_name', 'Our Family')
    return render_template('dashboard.html', family_name=family_name)


@pages_bp.route('/upload')
def upload():
    """Render the upload page."""
    return render_template('upload.html')


@pages_bp.route('/investor/<int:investor_id>')
def investor_detail(investor_id):
    """Render investor detail page."""
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return redirect(url_for('pages.index'))
    return render_template('investor.html', investor=investor)


@pages_bp.route('/folio/<int:folio_id>')
def folio_detail(folio_id):
    """Render folio/investment detail page."""
    return render_template('folio.html', folio_id=folio_id)


@pages_bp.route('/map-folios')
def map_folios_page():
    """Render folio mapping page."""
    return render_template('map_folios.html')


@pages_bp.route('/mutual-funds')
def mutual_funds_page():
    """Render mutual fund master page."""
    return render_template('mutual_funds.html')


@pages_bp.route('/resolve-conflicts')
def resolve_conflicts_page():
    """Render conflict resolution page."""
    return render_template('resolve_conflicts.html')


@pages_bp.route('/investor/<int:investor_id>/goals')
def goals_page(investor_id):
    """Render goals page for an investor."""
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return redirect(url_for('pages.index'))
    return render_template('goals.html', investor=investor)


@pages_bp.route('/investor/<int:investor_id>/tax-harvesting')
def tax_harvesting_page(investor_id):
    """Render tax-loss harvesting page for an investor."""
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return redirect(url_for('pages.index'))
    return render_template('tax_harvesting.html', investor=investor)


@pages_bp.route('/settings')
def settings_page():
    """Render settings/admin page."""
    return render_template('settings.html')


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
    return render_template('manual_assets.html', investor_id=investor_id)


@pages_bp.route('/nps')
def page_nps():
    """NPS management page."""
    return render_template('nps.html')


@pages_bp.route('/nps/<int:subscriber_id>')
def page_nps_subscriber(subscriber_id):
    """NPS subscriber detail page."""
    subscriber = db.get_nps_subscriber(subscriber_id=subscriber_id)
    if not subscriber:
        return redirect('/nps')
    return render_template('nps_subscriber.html', subscriber=subscriber)
