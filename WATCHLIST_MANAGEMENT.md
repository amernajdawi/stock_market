# Stock Watchlist Management

Simple guide to add and remove stocks from your AI Stock Monitor.

## Quick Start

### Add Stocks (3 Ways)

**1. Telegram Bot (Easiest)**
```
add AAPL
add GOOGL
add AMZN
```

**2. Database (phpMyAdmin)**
```sql
INSERT INTO watchlist (ticker, company_name, sector, added_at, is_active)
VALUES ("AAPL", "Apple Inc", "Technology", NOW(), 1);
```

**3. Command Line**
```bash
python3 manage_watchlist.py --add AAPL
```

### Remove Stocks

**Telegram:** `delete AAPL`  
**Database:** `UPDATE watchlist SET is_active = 0 WHERE ticker = "AAPL";`  
**Command Line:** `python3 manage_watchlist.py --remove AAPL`

---

## Ready-to-Use SQL Templates

Copy and paste these into phpMyAdmin SQL tab:

```sql
-- Tech Stocks
INSERT INTO watchlist (ticker, company_name, sector, added_at, is_active)
VALUES ("AAPL", "Apple Inc", "Technology", NOW(), 1);

INSERT INTO watchlist (ticker, company_name, sector, added_at, is_active)
VALUES ("GOOGL", "Alphabet Inc", "Technology", NOW(), 1);

INSERT INTO watchlist (ticker, company_name, sector, added_at, is_active)
VALUES ("MSFT", "Microsoft Corp", "Technology", NOW(), 1);

-- Consumer Stocks  
INSERT INTO watchlist (ticker, company_name, sector, added_at, is_active)
VALUES ("AMZN", "Amazon.com Inc", "Consumer Discretionary", NOW(), 1);

INSERT INTO watchlist (ticker, company_name, sector, added_at, is_active)
VALUES ("NFLX", "Netflix Inc", "Communication Services", NOW(), 1);

INSERT INTO watchlist (ticker, company_name, sector, added_at, is_active)
VALUES ("DIS", "Disney Company", "Communication Services", NOW(), 1);

-- Finance Stocks
INSERT INTO watchlist (ticker, company_name, sector, added_at, is_active)
VALUES ("V", "Visa Inc", "Financial Services", NOW(), 1);

INSERT INTO watchlist (ticker, company_name, sector, added_at, is_active)
VALUES ("JPM", "JPMorgan Chase", "Financial Services", NOW(), 1);
```

---

## What Happens After Adding

1. Detection: Within 2 minutes
2. Data: 90 days of history downloaded
3. Notification: Telegram message sent
4. Monitoring: Real-time price tracking starts
5. Alerts: Notifications when price drops below averages

---

## Check Your Stocks

**Telegram:** `list` or `status`  
**Database:** Go to `watchlist` table  
**Command Line:** `python3 manage_watchlist.py --list`

---

## Important Rules

- ticker: Must be valid (Yahoo Finance)
- added_at: Always use `NOW()`
- is_active: Must be `1` 
- Don't leave fields empty

---

## Problems?

**Stock not detected?** Check `is_active = 1` and `added_at` has timestamp  
**Duplicate error?** Stock already exists - use `list` to check  
**No notification?** Check logs: `docker logs stock_monitor_app`

---

## Database Access

**URL:** http://localhost:8080  
**Database:** stock_monitor  
**Table:** watchlist

---