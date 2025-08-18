

import logging
from datetime import datetime, date
from typing import List, Dict, Optional, Any
from decimal import Decimal

import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Date, 
    DateTime, Numeric, BigInteger, Text, Index, UniqueConstraint
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
        try:
            if not self.engine:
                logger.error("Database not connected")
                return []
            
            query = """
                SELECT DISTINCT ticker
                FROM stock_daily
                ORDER BY ticker
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                tickers = [row[0] for row in result.fetchall()]
                
            return tickers
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get tickers: {e}")
            return []
    
    def close(self) -> None:
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")
