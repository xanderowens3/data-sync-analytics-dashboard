"""
Cron Update Script for Railway
==============================

This script iterates through all client folders in /clients and runs update.py for each.
Configure Railway to run this script on an hourly schedule: `0 * * * *`

Usage: python cron_update.py
"""

import os
import subprocess
from pathlib import Path

def log(msg):
    from datetime import datetime
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def main():
    clients_dir = Path(__file__).parent / "clients"
    if not clients_dir.exists():
        log("ERROR: /clients directory not found")
        return

    # Find all subdirectories in /clients
    clients = [d.name for d in clients_dir.iterdir() if d.is_dir()]
    log(f"Found {len(clients)} clients: {', '.join(clients)}")

    for client in clients:
        log(f"--- Starting update for client: {client} ---")
        try:
            # Run update.py using subprocess
            # We use sys.executable to ensure we use the same python interpreter
            import sys
            result = subprocess.run(
                [sys.executable, "update.py", "--client", client],
                capture_output=True,
                text=True
            )
            
            # Print output so it shows up in Railway logs
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(f"STDERR for {client}:\n{result.stderr}")
            
            if result.returncode == 0:
                log(f"SUCCESS: Update complete for {client}")
            else:
                log(f"FAILED: Update for {client} exited with code {result.returncode}")
                
        except Exception as e:
            log(f"ERROR: Exception while updating {client}: {e}")

    log("=== All client updates finished ===")

if __name__ == "__main__":
    main()
