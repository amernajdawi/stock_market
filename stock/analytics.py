
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date
import pandas as pd

logger = logging.getLogger(__name__)


class StockAnalytics:
    
    def __init__(self, database_manager):
        self.db = database_manager
        self.average_periods = [7, 30, 90]  
    
    def check_alert_already_sent_today(self, ticker: str, alert_type: str) -> bool:
        """
        Check if an alert was already sent today for a specific stock and timeframe.
        This prevents duplicate alerts regardless of price changes.
        
        Alerts reset when the market opens (09:00 Vienna time), not at midnight.
        
        Args:
            ticker: Stock ticker symbol (e.g., 'RACE')
            alert_type: Alert type (e.g., '90_day', '30_day', '7_day')
            
        Returns:
            bool: True if alert was already sent today, False if eligible for new alert
        """
        try:
            if not self.db:
                logger.error("Database manager not available")
                return False
            
            # Calculate the current market session start time
            # Market opens at 09:00 Vienna time (Europe/Vienna timezone)
            from datetime import timezone, timedelta
            import pytz
            
            # Vienna timezone
            vienna_tz = pytz.timezone('Europe/Vienna')
            utc_now = datetime.now(timezone.utc)
            vienna_now = utc_now.astimezone(vienna_tz)
            
            # Today's market open at 09:00 Vienna time
            market_open_vienna = vienna_now.replace(hour=9, minute=0, second=0, microsecond=0)
            
            # If current time is before 09:00 Vienna time, use yesterday's market open
            if vienna_now.time() < market_open_vienna.time():
                market_open_vienna = market_open_vienna - timedelta(days=1)
            
            # Convert market open time back to UTC for database comparison
            market_open_utc = market_open_vienna.astimezone(timezone.utc)
            
            logger.debug(f"Checking alerts since market open: {market_open_utc} UTC ({market_open_vienna} Vienna)")
            
            # Check if alert was sent since market opened
            query = """
                SELECT COUNT(*) as alert_count
                FROM alert_history
                WHERE ticker = :ticker 
                AND alert_type = :alert_type
                AND sent_at >= :market_open
            """
            
            try:
                with self.db.engine.connect() as conn:
                    from sqlalchemy import text
                    # Use market open time instead of calendar day
                    params = {"ticker": ticker, "alert_type": alert_type, "market_open": market_open_utc}
                    result = conn.execute(text(query), params)
                    row = result.fetchone()
                    
                    if row and row[0] > 0:
                        logger.info(f"Alert already sent since market open for {ticker} {alert_type} - skipping")
                        return True
                    else:
                        logger.info(f"No alert sent since market open for {ticker} {alert_type} - eligible for new alert")
                        return False
                        
            except Exception as db_error:
                logger.error(f"Database error checking alert history for {ticker} {alert_type}: {db_error}")
                # If database check fails, assume no alert sent (safer approach)
                return False
                    
        except Exception as e:
            logger.error(f"Error checking alert history for {ticker} {alert_type}: {e}")
            return False
    
    def calculate_averages_for_ticker(self, ticker: str) -> Dict[str, Optional[float]]:
        try:
            logger.info(f"Calculating averages for {ticker}")
            
            averages = {}
            for period in self.average_periods:
                avg_value = self.db.get_trading_day_averages(ticker, period)
                averages[f'average_{period}'] = avg_value
                
                if avg_value:
                    logger.debug(f"{ticker} {period}-day average: ${avg_value:.2f}")
                else:
                    logger.warning(f"{ticker} {period}-day average: insufficient data")
            
            return averages
            
        except Exception as e:
            logger.error(f"Error calculating averages for {ticker}: {e}")
            return {f'average_{period}': None for period in self.average_periods}
    
    def calculate_averages_for_all_tickers(self, tickers: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
        all_averages = {}
        
        for ticker in tickers:
            logger.info(f"Processing {ticker} ({tickers.index(ticker) + 1}/{len(tickers)})")
            averages = self.calculate_averages_for_ticker(ticker)
            all_averages[ticker] = averages
        
        logger.info(f"Calculated averages for {len(all_averages)} tickers")
        return all_averages
    
    def compare_price_to_averages(self, ticker: str, current_price: float, averages: Dict[str, Optional[float]]) -> Dict[str, Dict[str, float]]:
        try:
            logger.info(f"Comparing current price (${current_price:.2f}) to averages for {ticker}")
            
            alert_conditions = {}
            
            for period in self.average_periods:
                avg_key = f'average_{period}'
                avg_value = averages.get(avg_key)
                
                if avg_value is not None and current_price < avg_value:
                    absolute_diff = avg_value - current_price
                    percent_diff = (absolute_diff / avg_value) * 100
                    
                    alert_conditions[f'{period}_day'] = {
                        'average': avg_value,
                        'absolute_difference': absolute_diff,
                        'percent_difference': percent_diff,
                        'alert_triggered': True
                    }
                    
                    logger.info(f"{ticker} {period}-day alert: Current ${current_price:.2f} < Average ${avg_value:.2f} (${absolute_diff:.2f} / {percent_diff:.2f}%)")
                else:
                    if avg_value is not None:
                        logger.debug(f"{ticker} {period}-day: Current ${current_price:.2f} >= Average ${avg_value:.2f} (no alert)")
                    else:
                        logger.debug(f"{ticker} {period}-day: No average data available")
            
            return alert_conditions
            
        except Exception as e:
            logger.error(f"Error comparing price to averages for {ticker}: {e}")
            return {}
    
    def analyze_all_tickers(self, tickers: List[str]) -> Dict[str, Dict[str, any]]:
        try:
            logger.info(f"Starting complete analysis for {len(tickers)} tickers")
            
            all_averages = self.calculate_averages_for_all_tickers(tickers)
            
            analysis_results = {}
            
            for ticker in tickers:
                logger.info(f"Analyzing {ticker} ({tickers.index(ticker) + 1}/{len(tickers)})")
                
                current_price = self.db.get_current_price(ticker)
                
                if current_price is None:
                    logger.warning(f"No current price available for {ticker}, skipping analysis")
                    analysis_results[ticker] = {
                        'current_price': None,
                        'averages': all_averages.get(ticker, {}),
                        'alert_conditions': {},
                        'analysis_complete': False
                    }
                    continue
                
                alert_conditions = self.compare_price_to_averages(
                    ticker, current_price, all_averages.get(ticker, {})
                )
                
                analysis_results[ticker] = {
                    'current_price': current_price,
                    'averages': all_averages.get(ticker, {}),
                    'alert_conditions': alert_conditions,
                    'analysis_complete': True,
                    'alerts_triggered': len(alert_conditions) > 0
                }
                
                if alert_conditions:
                    logger.info(f"{ticker}: {len(alert_conditions)} alert(s) triggered")
                else:
                    logger.info(f"{ticker}: No alerts triggered")
            
            logger.info(f"Analysis complete for {len(analysis_results)} tickers")
            return analysis_results
            
        except Exception as e:
            logger.error(f"Error in complete analysis: {e}")
            return {}
    
    def generate_daily_summary(self, analysis_results: Dict[str, Dict[str, any]]) -> Dict[str, int]:
        try:
            summary = {
                'total_tickers': len(analysis_results),
                'tickers_with_alerts': 0,
                'alerts_7_day': 0,
                'alerts_30_day': 0,
                'alerts_90_day': 0,
                'tickers_analyzed': 0
            }
            
            for ticker, result in analysis_results.items():
                if result.get('analysis_complete', False):
                    summary['tickers_analyzed'] += 1
                    
                    if result.get('alerts_triggered', False):
                        summary['tickers_with_alerts'] += 1
                        
                        alert_conditions = result.get('alert_conditions', {})
                        if '7_day' in alert_conditions:
                            summary['alerts_7_day'] += 1
                        if '30_day' in alert_conditions:
                            summary['alerts_30_day'] += 1
                        if '90_day' in alert_conditions:
                            summary['alerts_90_day'] += 1
            
            logger.info(f"Daily summary generated: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating daily summary: {e}")
            return {}
    
    def get_ticker_performance_summary(self, ticker: str, days: int = 30) -> Optional[Dict[str, any]]:
        try:
            current_price = self.db.get_current_price(ticker)
            if current_price is None:
                return None
            
            averages = self.calculate_averages_for_ticker(ticker)
            
            performance = {
                'ticker': ticker,
                'current_price': current_price,
                'timestamp': datetime.now(),
                'averages': averages,
                'performance_metrics': {}
            }
            
            for period in self.average_periods:
                avg_key = f'average_{period}'
                avg_value = averages.get(avg_key)
                
                if avg_value is not None:
                    change = current_price - avg_value
                    change_percent = (change / avg_value) * 100
                    
                    performance['performance_metrics'][f'{period}_day'] = {
                        'average': avg_value,
                        'change': change,
                        'change_percent': change_percent,
                        'above_average': change > 0
                    }
            
            return performance
            
        except Exception as e:
            logger.error(f"Error getting performance summary for {ticker}: {e}")
            return None

    def analyze_single_ticker(self, ticker: str) -> Optional[Dict[str, Any]]:
        try:
            current_price = self.db.get_current_price(ticker)
            if current_price is None:
                logger.warning(f"No current price data for {ticker}")
                return None
            
            averages = self.calculate_averages_for_ticker(ticker)
            if not averages:
                logger.warning(f"No moving averages available for {ticker}")
                return None
            
            averages_dict = {}
            for period in self.average_periods:
                avg_key = f'average_{period}'
                avg_value = averages.get(avg_key)
                if avg_value is not None:
                    averages_dict[f'{period}_day'] = avg_value
            
            triggered_averages = []
            for period, avg_value in averages_dict.items():
                if current_price < avg_value:
                    triggered_averages.append(period)
            
            price_differences = {}
            for period, avg_value in averages_dict.items():
                if current_price < avg_value:
                    diff = avg_value - current_price
                    pct_diff = (diff / avg_value) * 100
                    price_differences[period] = {
                        'difference': diff,
                        'percentage': pct_diff
                    }
            
            result = {
                'ticker': ticker,
                'current_price': current_price,
                'averages': averages_dict,
                'triggered_averages': triggered_averages,
                'price_differences': price_differences,
                'alerts_triggered': len(triggered_averages) > 0,
                'timestamp': datetime.now()
            }
            
            logger.info(f"Analysis completed for {ticker}: {len(triggered_averages)} alerts triggered")
            return result
            
        except Exception as e:
            logger.error(f"Failed to analyze single ticker {ticker}: {e}")
            return None
