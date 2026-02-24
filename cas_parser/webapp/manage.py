"""CLI management commands for user administration.

Usage:
    python -m cas_parser.webapp.manage create-admin <username> [display_name]
    python -m cas_parser.webapp.manage reset-password <username>
    python -m cas_parser.webapp.manage list-users
"""

import getpass
import sys

from cas_parser.webapp.db.connection import init_db
from cas_parser.webapp.db.auth import (
    create_user, get_all_users, get_user_by_username, update_password,
)


def cmd_create_admin(args):
    if len(args) < 1:
        print("Usage: create-admin <username> [display_name]")
        sys.exit(1)
    username = args[0]
    display_name = args[1] if len(args) > 1 else username

    existing = get_user_by_username(username)
    if existing:
        print(f"Error: User '{username}' already exists.")
        sys.exit(1)

    password = getpass.getpass("Password: ")
    confirm = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Error: Passwords do not match.")
        sys.exit(1)
    if len(password) < 4:
        print("Error: Password must be at least 4 characters.")
        sys.exit(1)

    user_id = create_user(username, password, display_name, role='admin')
    print(f"Admin user '{username}' created (id={user_id}).")


def cmd_reset_password(args):
    if len(args) < 1:
        print("Usage: reset-password <username>")
        sys.exit(1)
    username = args[0]
    user = get_user_by_username(username)
    if not user:
        print(f"Error: User '{username}' not found.")
        sys.exit(1)

    password = getpass.getpass("New password: ")
    confirm = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Error: Passwords do not match.")
        sys.exit(1)
    if len(password) < 4:
        print("Error: Password must be at least 4 characters.")
        sys.exit(1)

    update_password(user['id'], password)
    print(f"Password for '{username}' has been reset.")


def cmd_list_users(_args):
    users = get_all_users()
    if not users:
        print("No users found.")
        return
    print(f"{'ID':<5} {'Username':<15} {'Display Name':<20} {'Role':<8} {'Active':<7} {'Last Login'}")
    print("-" * 80)
    for u in users:
        active = "Yes" if u['is_active'] else "No"
        last = u['last_login'] or "Never"
        print(f"{u['id']:<5} {u['username']:<15} {u['display_name']:<20} {u['role']:<8} {active:<7} {last}")


COMMANDS = {
    'create-admin': cmd_create_admin,
    'reset-password': cmd_reset_password,
    'list-users': cmd_list_users,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print("Available commands: " + ", ".join(COMMANDS.keys()))
        sys.exit(1)

    init_db()
    COMMANDS[sys.argv[1]](sys.argv[2:])


if __name__ == '__main__':
    main()
