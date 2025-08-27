

import logging
from datetime import datetime, date
from typing import List, Dict, Optional, Any
from decimal import Decimal

import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Date, 
    DateTime, Numeric, BigInteger, Text, Index, UniqueConstraint, Enum, Boolean, Integer
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy import text

logger = logging.getLogger(__name__)


class DatabaseManager:
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine: Optional[Engine] = None
        self.metadata = MetaData()
        self._setup_tables()
    
    def _setup_tables(self) -> None:
        self.stock_daily = Table(
            'stock_daily',
            self.metadata,
            Column('id', BigInteger, primary_key=True, autoincrement=True),
            Column('ticker', String(16), nullable=False),
            Column('date', Date, nullable=False),
            Column('open', Numeric(18, 6)),
            Column('high', Numeric(18, 6)),
            Column('low', Numeric(18, 6)),
            Column('close', Numeric(18, 6)),
            Column('adj_close', Numeric(18, 6)),
            Column('volume', BigInteger),
            Column('fetched_at', DATETIME, nullable=False, default=datetime.utcnow),
            
            UniqueConstraint('ticker', 'date', name='unique_ticker_date'),
            
            Index('idx_ticker_date', 'ticker', 'date'),
            Index('idx_date', 'date'),
            Index('idx_fetched_at', 'fetched_at')
        )
        
        self.stock_latest = Table(
            'stock_latest',
            self.metadata,
            Column('ticker', String(16), primary_key=True),
            Column('price', Numeric(18, 6)),
            Column('bid', Numeric(18, 6)),
            Column('ask', Numeric(18, 6)),
            Column('timestamp', DATETIME),
            Column('fetched_at', DATETIME, nullable=False, default=datetime.utcnow),
            
            Index('idx_latest_fetched_at', 'fetched_at')
        )
        
        self.alert_history = Table(
            'alert_history',
            self.metadata,
            Column('id', BigInteger, primary_key=True, autoincrement=True),
            Column('ticker', String(16), nullable=False),
            Column('alert_type', Enum('7_day', '30_day', '90_day', name='alert_type_enum'), nullable=False),
            Column('current_price', Numeric(18, 6), nullable=False),
            Column('average_price', Numeric(18, 6), nullable=False),
            Column('absolute_difference', Numeric(18, 6), nullable=False),
            Column('percent_difference', Numeric(10, 4), nullable=False),
            Column('sent_at', DATETIME, nullable=False, default=datetime.utcnow),
            
            Index('idx_ticker', 'ticker'),
            Index('idx_alert_type', 'alert_type'),
            Index('idx_sent_at', 'sent_at')
        )
        
        self.watchlist = Table(
            'watchlist',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('ticker', String(16), nullable=False, unique=True),
            Column('company_name', String(255)),
            Column('sector', String(100), default='Custom'),
            Column('added_at', DATETIME, nullable=False, default=datetime.utcnow),
            Column('is_active', Boolean, default=True),
            Column('notes', Text),
            
            Index('idx_ticker', 'ticker'),
            Index('idx_sector', 'sector'),
            Index('idx_active', 'is_active'),
            Index('idx_added_at', 'added_at')
        )
    
    def connect(self) -> bool:
        try:
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Database connection established successfully")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def create_tables(self) -> bool:
        try:
            if not self.engine:
                if not self.connect():
                    return False
            
            self.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create tables: {e}")
            return False
    
    def insert_historical_data(self, ticker: str, data: pd.DataFrame) -> bool:
        try:
            if not self.engine:
                logger.error("Database not connected")
                return False
            
            records = []
            for _, row in data.iterrows():
                record = {
                    'ticker': ticker,
                    'date': row.name.date() if hasattr(row.name, 'date') else row.name,
                    'open': float(row['Open']) if pd.notna(row['Open']) else None,
                    'high': float(row['High']) if pd.notna(row['High']) else None,
                    'low': float(row['Low']) if pd.notna(row['Low']) else None,
                    'close': float(row['Close']) if pd.notna(row['Close']) else None,
                    'adj_close': float(row['Adj Close']) if pd.notna(row['Adj Close']) else None,
                    'volume': int(row['Volume']) if pd.notna(row['Volume']) else None,
                    'fetched_at': datetime.utcnow()
                }
                records.append(record)
            
            with self.engine.connect() as conn:
                for record in records:
                    stmt = f"""
                        REPLACE INTO stock_daily 
                        (ticker, date, open, high, low, close, adj_close, volume, fetched_at)
                        VALUES (:ticker, :date, :open, :high, :low, :close, :adj_close, :volume, :fetched_at)
                    """
                    conn.execute(text(stmt), record)
                conn.commit()
            
            logger.info(f"Inserted {len(records)} historical records for {ticker}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to insert historical data for {ticker}: {e}")
            return False
    
    def update_latest_price(self, ticker: str, price_data: Dict[str, Any]) -> bool:
        try:
            if not self.engine:
                logger.error("Database not connected")
                return False
            
            record = {
                'ticker': ticker,
                'price': float(price_data.get('price', 0)),  # Maintain exact precision
                'bid': float(price_data.get('bid', 0)) if price_data.get('bid') else None,
                'ask': float(price_data.get('ask', 0)) if price_data.get('ask') else None,
                'timestamp': price_data.get('timestamp'),
                'fetched_at': datetime.utcnow()
            }
            
            logger.info(f"Storing {ticker} with exact price: ${record['price']:.6f}")
            
            with self.engine.connect() as conn:
                stmt = f"""
                    REPLACE INTO stock_latest 
                    (ticker, price, bid, ask, timestamp, fetched_at)
                    VALUES (:ticker, :price, :bid, :ask, :timestamp, :fetched_at)
                """
                conn.execute(text(stmt), record)
                conn.commit()
            
            logger.info(f"Updated latest price for {ticker}: {record['price']}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to update latest price for {ticker}: {e}")
            return False
    
    def get_trading_day_averages(self, ticker: str, days: int) -> Optional[float]:
        try:
            if not self.engine:
                logger.error("Database not connected")
                return None
            
            query = f"""
                SELECT AVG(close) as avg_close
                FROM (
                    SELECT close
                    FROM stock_daily
                    WHERE ticker = :ticker
                    AND close IS NOT NULL
                    ORDER BY date DESC
                    LIMIT :days
                ) as recent_data
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"ticker": ticker, "days": days})
                row = result.fetchone()
                
                if row and row[0] is not None:
                    return float(row[0])
                else:
                    logger.warning(f"Insufficient data for {ticker} - {days} day average")
                    return None
                    
        except SQLAlchemyError as e:
            logger.error(f"Failed to get {days}-day average for {ticker}: {e}")
            return None
    
    def get_current_price(self, ticker: str) -> Optional[float]:
        try:
            if not self.engine:
                logger.error("Database not connected")
                return None
            
            query = """
                SELECT price
                FROM stock_latest
                WHERE ticker = :ticker
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"ticker": ticker})
                row = result.fetchone()
                
                if row and row[0] is not None:
                    return float(row[0])
                else:
                    logger.warning(f"No current price available for {ticker}")
                    return None
                    
        except SQLAlchemyError as e:
            logger.error(f"Failed to get current price for {ticker}: {e}")
            return None
    
    def get_current_moving_averages(self, ticker: str) -> Dict[str, Optional[float]]:
        try:
            if not self.engine:
                logger.error("Database not connected")
                return {}
            
            averages = {}
            periods = [7, 30, 90]
            
            for period in periods:
                avg_value = self.get_trading_day_averages(ticker, period)
                averages[f'{period}_day'] = avg_value
                
                if avg_value:
                    logger.info(f"{ticker} {period}-day moving average: ${avg_value:.2f}")
                else:
                    logger.warning(f"{ticker} {period}-day moving average: insufficient data")
            
            return averages
            
        except Exception as e:
            logger.error(f"Failed to get moving averages for {ticker}: {e}")
            return {}
    
    def get_all_tickers(self) -> List[str]:
        """Get all active tickers from the watchlist table."""
        try:
            if not self.engine:
                logger.error("Database not connected")
                return []
            
            query = """
                SELECT ticker
                FROM watchlist
                WHERE is_active = TRUE
                ORDER BY ticker
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                tickers = [row[0] for row in result.fetchall()]
                
            return tickers
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get tickers from watchlist: {e}")
            return []
    
    def save_alert_to_database(self, ticker: str, alert_type: str, current_price: float, 
                              average_price: float, absolute_difference: float, 
                              percent_difference: float) -> bool:
        """
        Save an alert to the database to prevent duplicate alerts on the same day.
        
        Args:
            ticker: Stock ticker symbol
            alert_type: Alert type (7_day, 30_day, 90_day)
            current_price: Current stock price
            average_price: Moving average price
            absolute_difference: Absolute price difference
            percent_difference: Percentage difference
            
        Returns:
            bool: True if alert saved successfully, False otherwise
        """
        try:
            if not self.engine:
                logger.error("Database not connected")
                return False
            
            record = {
                'ticker': ticker,
                'alert_type': alert_type,
                'current_price': current_price,
                'average_price': average_price,
                'absolute_difference': absolute_difference,
                'percent_difference': percent_difference,
                'sent_at': datetime.now()
            }
            
            with self.engine.connect() as conn:
                stmt = """
                    INSERT INTO alert_history 
                    (ticker, alert_type, current_price, average_price, absolute_difference, percent_difference, sent_at)
                    VALUES (:ticker, :alert_type, :current_price, :average_price, :absolute_difference, :percent_difference, :sent_at)
                """
                conn.execute(text(stmt), {
                    'ticker': ticker,
                    'alert_type': alert_type,
                    'current_price': current_price,
                    'average_price': average_price,
                    'absolute_difference': absolute_difference,
                    'percent_difference': percent_difference,
                    'sent_at': record['sent_at']
                })
                conn.commit()
            
            logger.info(f"Alert saved to database for {ticker} {alert_type}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save alert to database for {ticker} {alert_type}: {e}")
            return False
    
    def add_company_to_watchlist(self, ticker: str, company_name: str = None, 
                                sector: str = "Custom", notes: str = None) -> bool:
        """Add a new company to the watchlist."""
        try:
            if not self.engine:
                logger.error("Database not connected")
                return False
            
            record = {
                'ticker': ticker.upper(),
                'company_name': company_name,
                'sector': sector,
                'notes': notes,
                'added_at': datetime.now(),
                'is_active': True
            }
            
            with self.engine.connect() as conn:
                stmt = """
                    INSERT INTO watchlist (ticker, company_name, sector, notes, added_at, is_active)
                    VALUES (:ticker, :company_name, :sector, :notes, :added_at, :is_active)
                """
                conn.execute(text(stmt), record)
                conn.commit()
            
            logger.info(f"Added {ticker} to watchlist: {company_name or 'Unknown Company'}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add {ticker} to watchlist: {e}")
            return False
    
    def remove_company_from_watchlist(self, ticker: str) -> bool:
        """Remove a company from the watchlist (set inactive)."""
        try:
            if not self.engine:
                logger.error("Database not connected")
                return False
            
            with self.engine.connect() as conn:
                stmt = """
                    UPDATE watchlist 
                    SET is_active = FALSE 
                    WHERE ticker = :ticker
                """
                result = conn.execute(text(stmt), {"ticker": ticker.upper()})
                conn.commit()
                
                if result.rowcount > 0:
                    logger.info(f"Removed {ticker} from active watchlist")
                    return True
                else:
                    logger.warning(f"Ticker {ticker} not found in watchlist")
                    return False
            
        except Exception as e:
            logger.error(f"Failed to remove {ticker} from watchlist: {e}")
            return False
    
    def get_watchlist(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get all companies in the watchlist."""
        try:
            if not self.engine:
                logger.error("Database not connected")
                return []
            
            query = """
                SELECT ticker, company_name, sector, added_at, is_active, notes
                FROM watchlist
            """
            if active_only:
                query += " WHERE is_active = TRUE"
            query += " ORDER BY added_at DESC"
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                watchlist = []
                for row in result.fetchall():
                    watchlist.append({
                        'ticker': row[0],
                        'company_name': row[1],
                        'sector': row[2],
                        'added_at': row[3],
                        'is_active': row[4],
                        'notes': row[5]
                    })
                
            return watchlist
            
        except Exception as e:
            logger.error(f"Failed to get watchlist: {e}")
            return []
    
    def close(self) -> None:
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")
