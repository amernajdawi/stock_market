-- Automotive Stocks Monitoring System Database Initialization
-- This script creates the necessary tables for storing stock data

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS stock_monitor;
USE stock_monitor;

-- Historical daily stock data table
CREATE TABLE IF NOT EXISTS stock_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(16) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    adj_close DECIMAL(18,6),
    volume BIGINT,
    fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint on ticker and date
    UNIQUE KEY unique_ticker_date (ticker, date),
    
    -- Indexes for performance
    INDEX idx_ticker_date (ticker, date),
    INDEX idx_date (date),
    INDEX idx_fetched_at (fetched_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Latest stock snapshot table
CREATE TABLE IF NOT EXISTS stock_latest (
    ticker VARCHAR(16) PRIMARY KEY,
    price DECIMAL(18,6),
    bid DECIMAL(18,6),
    ask DECIMAL(18,6),
    timestamp DATETIME,
    fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Index for performance
    INDEX idx_latest_fetched_at (fetched_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- System status table for monitoring
CREATE TABLE IF NOT EXISTS system_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    component VARCHAR(50) NOT NULL,
    status ENUM('running', 'stopped', 'error') NOT NULL,
    last_run DATETIME,
    next_run DATETIME,
    message TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_component (component),
    INDEX idx_status (status),
    INDEX idx_last_run (last_run)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Alert history table
CREATE TABLE IF NOT EXISTS alert_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(16) NOT NULL,
    alert_type ENUM('7_day', '30_day', '90_day') NOT NULL,
    current_price DECIMAL(18,6) NOT NULL,
    average_price DECIMAL(18,6) NOT NULL,
    absolute_difference DECIMAL(18,6) NOT NULL,
    percent_difference DECIMAL(10,4) NOT NULL,
    sent_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_ticker (ticker),
    INDEX idx_alert_type (alert_type),
    INDEX idx_sent_at (sent_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Custom watchlist table for user-defined companies to monitor
CREATE TABLE IF NOT EXISTS watchlist (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(16) NOT NULL UNIQUE,
    company_name VARCHAR(255),
    sector VARCHAR(100) DEFAULT 'Custom',
    added_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    notes TEXT,
    
    INDEX idx_ticker (ticker),
    INDEX idx_sector (sector),
    INDEX idx_active (is_active),
    INDEX idx_added_at (added_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert initial automotive companies into watchlist
INSERT INTO watchlist (ticker, company_name, sector, notes) VALUES
('TSLA', 'Tesla Inc', 'Auto Manufacturers', 'Electric vehicle leader'),
('TM', 'Toyota Motor Corp', 'Auto Manufacturers', 'Japanese automotive giant'),
('F', 'Ford Motor Co', 'Auto Manufacturers', 'American automotive company'),
('GM', 'General Motors Co', 'Auto Manufacturers', 'American automotive company'),
('BMW3.DE', 'BMW AG', 'Auto Manufacturers', 'German luxury automotive'),
('MBGYY', 'Mercedes-Benz Group AG', 'Auto Manufacturers', 'German luxury automotive'),
('VWAGY', 'Volkswagen AG', 'Auto Manufacturers', 'German automotive group'),
('HMC', 'Honda Motor Co Ltd', 'Auto Manufacturers', 'Japanese automotive company'),
('NSANY', 'Nissan Motor Co Ltd', 'Auto Manufacturers', 'Japanese automotive company'),
('RACE', 'Ferrari NV', 'Auto Manufacturers', 'Italian luxury sports cars')
ON DUPLICATE KEY UPDATE
    company_name = VALUES(company_name),
    sector = VALUES(sector),
    notes = VALUES(notes);

-- Insert initial system status
INSERT INTO system_status (component, status, message) VALUES
('stock_monitor', 'stopped', 'System not yet started'),
('data_fetcher', 'stopped', 'Data fetcher not yet initialized'),
('analytics_engine', 'stopped', 'Analytics engine not yet initialized'),
('alert_system', 'stopped', 'Alert system not yet initialized')
ON DUPLICATE KEY UPDATE
    status = VALUES(status),
    message = VALUES(message),
    updated_at = CURRENT_TIMESTAMP;

-- Create views for easier querying
CREATE OR REPLACE VIEW v_stock_summary AS
SELECT 
    s.ticker,
    s.price as current_price,
    s.fetched_at as last_price_update,
    COUNT(h.id) as historical_records,
    MAX(h.date) as latest_historical_date,
    MIN(h.date) as earliest_historical_date
FROM stock_latest s
LEFT JOIN stock_daily h ON s.ticker = h.ticker
GROUP BY s.ticker, s.price, s.fetched_at;

CREATE OR REPLACE VIEW v_recent_alerts AS
SELECT 
    ticker,
    alert_type,
    current_price,
    average_price,
    absolute_difference,
    percent_difference,
    sent_at
FROM alert_history
WHERE sent_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
ORDER BY sent_at DESC;

-- Grant permissions to the application user
GRANT SELECT, INSERT, UPDATE, DELETE ON stock_monitor.* TO 'stock_user'@'%';
GRANT SELECT ON stock_monitor.* TO 'stock_user'@'%';

-- Flush privileges
FLUSH PRIVILEGES;

-- Insert sample data for testing (optional)
-- INSERT INTO stock_daily (ticker, date, open, high, low, close, adj_close, volume) VALUES
-- ('TSLA', '2024-01-01', 100.00, 105.00, 98.00, 102.50, 102.50, 1000000),
-- ('TSLA', '2024-01-02', 102.50, 108.00, 101.00, 106.00, 106.00, 1200000);

-- Show created tables
SHOW TABLES;

-- Show table structure
DESCRIBE stock_daily;
DESCRIBE stock_latest;
DESCRIBE system_status;
DESCRIBE alert_history;
