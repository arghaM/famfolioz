"""
Flask web application for CAS PDF Parser.

Provides a web UI for uploading CAS PDFs, viewing parsed data,
and managing investor portfolios with persistence.

All routes now live in cas_parser.webapp.routes/ sub-modules.
This file creates the app via the factory and serves as the entry point:

    python3 -m cas_parser.webapp.app
"""

from cas_parser.webapp.routes import create_app

app = create_app()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
