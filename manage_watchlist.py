#!/usr/bin/env python3
"""
Stock Watchlist Management Tool

This script allows you to manage your custom stock watchlist through the database.
You can add new companies, remove companies, and view your current watchlist.

Usage:
    python3 manage_watchlist.py --help
    python3 manage_watchlist.py --list
    python3 manage_watchlist.py --add AAPL --name "Apple Inc" --sector "Technology" --notes "iPhone maker"
    python3 manage_watchlist.py --remove AAPL
"""

import argparse
import sys
import os
from datetime import datetime
from typing import Dict, List

# Add the stock module to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from stock.database import DatabaseManager


def load_db_config():
    """Load database configuration from environment variables."""
    return {
        'host': os.getenv('MARIADB_HOST', 'localhost'),
        'port': int(os.getenv('MARIADB_PORT', '3307')),  # Default to external port
        'name': os.getenv('MARIADB_DB', 'stock_monitor'),
        'user': os.getenv('MARIADB_USER', 'stock_user'),
        'password': os.getenv('MARIADB_PASSWORD', 'stock_password123'),
        'charset': 'utf8mb4'
    }


def create_db_connection():
    """Create database connection."""
    try:
        db_config = load_db_config()
        connection_string = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['name']}"
            f"?charset={db_config['charset']}"
        )
        
        db = DatabaseManager(connection_string)
        if db.connect():
            return db
        else:
            print("❌ Failed to connect to database")
            return None
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None


def list_watchlist():
    """List all companies in the watchlist."""
    db = create_db_connection()
    if not db:
        return
    
    try:
        watchlist = db.get_watchlist(active_only=False)
        
        if not watchlist:
            print("📋 Your watchlist is empty.")
            return
        
        print(f"📋 Your Stock Watchlist ({len(watchlist)} companies):")
        print("=" * 80)
        
        active_companies = [c for c in watchlist if c['is_active']]
        inactive_companies = [c for c in watchlist if not c['is_active']]
        
        if active_companies:
            print(f"\n✅ ACTIVE COMPANIES ({len(active_companies)}):")
            for company in active_companies:
                print(f"   {company['ticker']:<8} | {company['company_name'] or 'Unknown':<30} | {company['sector']:<20}")
                if company['notes']:
                    print(f"            Notes: {company['notes']}")
        
        if inactive_companies:
            print(f"\n❌ INACTIVE COMPANIES ({len(inactive_companies)}):")
            for company in inactive_companies:
                print(f"   {company['ticker']:<8} | {company['company_name'] or 'Unknown':<30} | {company['sector']:<20}")
        
    except Exception as e:
        print(f"❌ Error listing watchlist: {e}")
    finally:
        db.close()


def add_company(ticker: str, name: str = None, sector: str = "Custom", notes: str = None):
    """Add a company to the watchlist."""
    db = create_db_connection()
    if not db:
        return
    
    try:
        ticker = ticker.upper()
        
        # Check if company already exists
        watchlist = db.get_watchlist(active_only=False)
        existing = next((c for c in watchlist if c['ticker'] == ticker), None)
        
        if existing:
            if existing['is_active']:
                print(f"⚠️  {ticker} is already in your active watchlist")
                return
            else:
                print(f"📝 {ticker} exists but is inactive. Reactivating...")
                # You could add logic here to reactivate instead of adding new
        
        success = db.add_company_to_watchlist(ticker, name, sector, notes)
        
        if success:
            print(f"✅ Successfully added {ticker} to your watchlist!")
            if name:
                print(f"   Company: {name}")
            print(f"   Sector: {sector}")
            if notes:
                print(f"   Notes: {notes}")
            print(f"\n💡 The system will start monitoring {ticker} in the next cycle.")
        else:
            print(f"❌ Failed to add {ticker} to watchlist")
            
    except Exception as e:
        print(f"❌ Error adding company: {e}")
    finally:
        db.close()


def remove_company(ticker: str):
    """Remove a company from the watchlist."""
    db = create_db_connection()
    if not db:
        return
    
    try:
        ticker = ticker.upper()
        success = db.remove_company_from_watchlist(ticker)
        
        if success:
            print(f"✅ Successfully removed {ticker} from your watchlist!")
            print(f"💡 The system will stop monitoring {ticker} in the next cycle.")
        else:
            print(f"❌ {ticker} not found in your watchlist")
            
    except Exception as e:
        print(f"❌ Error removing company: {e}")
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description="Manage your stock monitoring watchlist",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 manage_watchlist.py --list
  python3 manage_watchlist.py --add AAPL --name "Apple Inc" --sector "Technology"
  python3 manage_watchlist.py --add MSFT --name "Microsoft Corp" --notes "Cloud computing leader"
  python3 manage_watchlist.py --remove AAPL
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--list', action='store_true', help='List all companies in watchlist')
    group.add_argument('--add', metavar='TICKER', help='Add a company to watchlist')
    group.add_argument('--remove', metavar='TICKER', help='Remove a company from watchlist')
    
    parser.add_argument('--name', help='Company name (for --add)')
    parser.add_argument('--sector', default='Custom', help='Company sector (default: Custom)')
    parser.add_argument('--notes', help='Additional notes about the company')
    
    args = parser.parse_args()
    
    if args.list:
        list_watchlist()
    elif args.add:
        add_company(args.add, args.name, args.sector, args.notes)
    elif args.remove:
        remove_company(args.remove)


if __name__ == "__main__":
    main()
