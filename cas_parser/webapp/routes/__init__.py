"""Flask application factory and shared utilities."""

import json
import os
import sys
from decimal import Decimal

from flask import Flask


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


def create_app():
    """Create and configure the Flask application."""
    # Add parent directory to path for imports
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    app = Flask(__name__,
                template_folder=os.path.join(os.path.dirname(__file__), '..', 'templates'),
                static_folder=os.path.join(os.path.dirname(__file__), '..', 'static'))
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

    # Make app version available in all templates
    from cas_parser import __version__ as APP_VERSION
    app.jinja_env.globals['app_version'] = APP_VERSION

    # Initialize authentication (before_request, context_processor, secret key)
    from cas_parser.webapp.auth import init_auth
    init_auth(app)

    # Register all blueprints
    from cas_parser.webapp.routes.auth import auth_bp
    from cas_parser.webapp.routes.pages import pages_bp
    from cas_parser.webapp.routes.investors import investors_bp
    from cas_parser.webapp.routes.folios import folios_bp
    from cas_parser.webapp.routes.transactions import transactions_bp
    from cas_parser.webapp.routes.performance import performance_bp
    from cas_parser.webapp.routes.mutual_funds import mutual_funds_bp
    from cas_parser.webapp.routes.goals import goals_bp
    from cas_parser.webapp.routes.nps import nps_bp
    from cas_parser.webapp.routes.manual_assets import manual_assets_bp
    from cas_parser.webapp.routes.admin import admin_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(pages_bp)
    app.register_blueprint(investors_bp)
    app.register_blueprint(folios_bp)
    app.register_blueprint(transactions_bp)
    app.register_blueprint(performance_bp)
    app.register_blueprint(mutual_funds_bp)
    app.register_blueprint(goals_bp)
    app.register_blueprint(nps_bp)
    app.register_blueprint(manual_assets_bp)
    app.register_blueprint(admin_bp)

    return app
