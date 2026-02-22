"""
Backward compatibility shim.

All database functions now live in cas_parser.webapp.db/ sub-modules.
This file re-exports everything so existing imports continue to work:

    from cas_parser.webapp import data as db
    from cas_parser.webapp.data import get_all_investors
"""

from cas_parser.webapp.db import *  # noqa: F401,F403
