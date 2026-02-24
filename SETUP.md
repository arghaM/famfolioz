# Famfolioz — Setup Guide

Step-by-step instructions to get Famfolioz running on your computer.
Everything runs locally — your financial data never leaves your machine.

---

## Prerequisites

You only need **Python 3.8 or newer** installed.

### Install Python

| OS | Instructions |
|----|-------------|
| **macOS** | Open Terminal and run: `brew install python3` <br>Or download from https://www.python.org/downloads/ |
| **Windows** | Download from https://www.python.org/downloads/ <br>**Important: Check "Add Python to PATH"** during installation |
| **Linux (Ubuntu/Debian)** | `sudo apt install python3 python3-venv python3-pip` |

To verify, open a terminal and run:
```
python3 --version
```
You should see something like `Python 3.10.12`. Any version 3.8+ works.

---

## Step 1: Get the Code

**Option A — From GitHub:**
```bash
git clone https://github.com/arghaM/famfolioz.git
cd famfolioz
```

**Option B — From a ZIP file:**
Unzip the file, then open a terminal in that folder.

---

## Step 2: Run Setup (one time only)

**macOS / Linux:**
```bash
bash setup_app.sh
```

**Windows:**
Just skip to Step 3 — `start.bat` will do the setup automatically on first run.

This creates an isolated Python environment and installs all dependencies.
It takes about 1-2 minutes on a decent internet connection.

---

## Step 3: Start the App

| OS | How |
|----|-----|
| **macOS** | Double-click `start.command` in Finder |
| **macOS / Linux** | Run `./start.sh` in terminal |
| **Windows** | Double-click `start.bat` |

The app starts at **http://127.0.0.1:5000** and should open in your browser automatically (on macOS).
On Windows/Linux, open that URL manually in your browser.

To stop the app, press `Ctrl+C` in the terminal window.

---

## Step 4: First-Time Setup

### 1. Create Your Admin Account

On first launch, you'll see the **Setup Wizard**. Create your admin account:
- Choose a username (e.g., "admin" or your name)
- Set a display name
- Set a password

This is the master account with full access to all features.

### 2. Upload Your CAS PDF

- Go to the **Upload** page
- Select your CDSL Consolidated Account Statement PDF
- Enter the PDF password (usually your PAN in uppercase)
- Click **Upload & Parse**

The upload page shows the last sync date per investor, so you know from what date to download your next CAS.

### 3. Create Investor Profiles and Map Folios

- After upload, go to **Map Folios** (Settings menu)
- Click **Create New Investor** — enter a name (e.g., "Rahul")
- Select the folios that belong to this investor and map them

### 4. View Your Portfolio

- Go back to the **Dashboard**
- Click on the investor card
- You'll see the Home tab with portfolio summary, growth chart, asset allocation, and alerts

### 5. Get Live NAV Prices

- On the investor page, click **Refresh NAV** (top right)
- This fetches current prices from AMFI and updates your portfolio values
- A portfolio snapshot is also saved for the growth chart

### 6. Add Other Assets (optional)

- Click **Add Assets** to manually add FDs, SGBs, NPS, PPF, stocks
- FDs can also be bulk-imported via CSV

---

## Adding Family Members

You can create member accounts for family members so they can view their own portfolios:

1. Go to **Settings** > **User Management**
2. Click **Add User**
3. Set username, display name, password, and role (`member`)
4. Link their investor profile under "Own Portfolio"
5. Optionally grant access to additional investor portfolios (custodian access)

**Roles:**
- **Admin** — full access to all investors, settings, backup, and user management
- **Member** — access to own portfolio + any custodian-granted portfolios

### CLI User Management

If you get locked out, use the command-line tool:
```bash
source venv/bin/activate
python -m cas_parser.webapp.manage list-users
python -m cas_parser.webapp.manage reset-password <username>
python -m cas_parser.webapp.manage create-admin <username>
```

---

## Docker Setup (Alternative)

If you prefer Docker over a local Python install:

```bash
docker compose up -d
```

Access at **http://localhost:5000**. Data persists in a Docker volume.

To stop: `docker compose down`

---

## Updating the App

When you receive a new version:

```bash
cd famfolioz
git pull                          # if using git
pip install -r requirements.txt   # in case new dependencies were added
```

Your data (the SQLite database) is preserved across updates.

---

## Backup & Restore

Your data is stored in `cas_parser/webapp/data.db` (a single SQLite file).

- **From the app:** Go to **Settings** > **Backup** to save a JSON backup (includes users and all configuration)
- **Manual backup:** Just copy `cas_parser/webapp/data.db` somewhere safe
- **Restore:** Settings > Restore from a backup file, or replace `data.db` with your backup copy

---

## Troubleshooting

### "python3: command not found"
Python isn't installed or not in your PATH. See the Prerequisites section above.

### "No module named flask" or similar
Setup didn't complete. Run `bash setup_app.sh` again (macOS/Linux) or delete the `venv` folder and double-click `start.bat` again (Windows).

### Port 5000 already in use
Another app (sometimes macOS AirPlay) is using port 5000. Either:
- Stop that app, or
- Run manually with a different port:
  ```bash
  source venv/bin/activate
  python3 -m cas_parser.webapp.app --port 5001
  ```

### App starts but browser shows blank page
Wait a few seconds and refresh. The server needs a moment to initialize the database on first run.

### Locked out / forgot password
Use the CLI management tool:
```bash
source venv/bin/activate
python -m cas_parser.webapp.manage reset-password <username>
```

---

## What's Stored Where

| Item | Location | Backed up by git? |
|------|----------|-------------------|
| Your financial data | `cas_parser/webapp/data.db` | No (gitignored) |
| Backup JSONs | `cas_parser/webapp/backups/` | No (gitignored) |
| App code | Everything else | Yes |
| Dependencies | `venv/` folder | No (recreated by setup) |

All your personal data stays local. Nothing is sent to any server.
