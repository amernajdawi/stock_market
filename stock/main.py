

import os
import sys
import logging
import logging.handlers
from datetime import datetime, time
from typing import Dict, List, Optional
import yaml
from dotenv import load_dotenv

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from stock.database import DatabaseManager
from stock.data_fetcher import StockDataFetcher
from stock.analytics import StockAnalytics
from stock.alerts import TelegramAlertSystem


load_dotenv()

def setup_logging(config: Dict) -> None:

    os.makedirs('logs', exist_ok=True)
    
    log_level = getattr(logging, config.get('level', 'INFO'))
    log_format = config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.handlers.RotatingFileHandler(
                config.get('file', 'logs/stock_monitor.log'),
                maxBytes=config.get('max_size_mb', 10) * 1024 * 1024,
                backupCount=config.get('backup_count', 5)
            )
        ]
    )


class StockMonitor:

    
    def __init__(self, config_path: str = 'config.yaml'):
        self.config = self._load_config(config_path)
        
        setup_logging(self.config.get('logging', {}))
        
        self.logger = logging.getLogger(__name__)
        
        self.db_manager = None
        self.data_fetcher = None
        self.analytics = None
        self.alert_system = None
        self.scheduler = None
        
        self._initialize_system()
    
    def _load_config(self, config_path: str) -> Dict:
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            
            config = self._replace_env_vars(config)
            
            print("Configuration loaded successfully")
            return config
            
        except Exception as e:
            print(f"Failed to load configuration: {e}")
            raise
    
    def _replace_env_vars(self, config: Dict) -> Dict:
        import re
        
        def replace_recursive(obj):
            if isinstance(obj, dict):
                return {key: replace_recursive(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [replace_recursive(item) for item in obj]
            elif isinstance(obj, str):
                matches = re.findall(r'\$\{([^}]+)\}|\$([a-zA-Z_][a-zA-Z0-9_]*)', obj)
                for match in matches:
                    var_name = match[0] or match[1]
                    env_value = os.getenv(var_name)
                    if env_value:
                        obj = obj.replace(f'${{{var_name}}}', env_value).replace(f'${var_name}', env_value)
                return obj
            else:
                return obj
        
        return replace_recursive(config)
    
    def _initialize_system(self) -> None:
        try:
            self.logger.info("Initializing stock monitoring system...")
            
            self._initialize_database()
            
            self._initialize_data_fetcher()
            
            self._initialize_analytics()
            
            self._initialize_alert_system()
            
            self._setup_scheduler()
            
            self.logger.info("System initialization complete")
            
        except Exception as e:
            self.logger.error(f"System initialization failed: {e}")
            raise
    
    def _initialize_database(self) -> None:
        try:
            db_config = self.config['database']
            connection_string = (
                f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
                f"@{db_config['host']}:{db_config['port']}/{db_config['name']}"
                f"?charset={db_config['charset']}"
            )
            
            self.db_manager = DatabaseManager(connection_string)
            
            max_retries = 5
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    if self.db_manager.connect():
                        break
                    else:
                        raise Exception("Connection failed")
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise Exception(f"Failed to connect to database after {max_retries} attempts: {e}")
                    
                    self.logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2  
            
            if not self.db_manager.create_tables():
                raise Exception("Failed to create database tables")
            
            self.logger.info("Database initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise
    
    def _initialize_data_fetcher(self) -> None:
        try:
            retry_config = self.config.get('retry', {})
            self.data_fetcher = StockDataFetcher(
                retry_attempts=retry_config.get('max_attempts', 3),
                backoff_seconds=retry_config.get('backoff_seconds', 5)
            )
            
            self.logger.info("Data fetcher initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Data fetcher initialization failed: {e}")
            raise
    
    def _initialize_analytics(self) -> None:
        """Initialize analytics engine."""
        try:
            self.analytics = StockAnalytics(self.db_manager)
            self.logger.info("Analytics engine initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Analytics initialization failed: {e}")
            raise
    
    def _initialize_alert_system(self) -> None:
        try:
            telegram_config = self.config['telegram']
            self.alert_system = TelegramAlertSystem(
                bot_token=telegram_config['bot_token'],
                chat_id=telegram_config['chat_id']
            )
            
            self.logger.info("Alert system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Alert system initialization failed: {e}")
            raise
    
    def _setup_scheduler(self) -> None:
        try:
            self.scheduler = BlockingScheduler(timezone=self.config['schedule']['timezone'])
            

            
                        # Real-time monitoring job (interval from config, default 5 minutes)
            interval_minutes = 5  # Default to 5 minutes
            if self.config['schedule'].get('real_time_monitoring', False):
                interval_minutes = self.config['schedule'].get('real_time_interval', 5)
                self.scheduler.add_job(
                    self.run_real_time_monitoring,
                    'interval',
                    minutes=interval_minutes,
                    id='real_time_monitoring',
                    name='Real-time Stock Monitoring'
                )
                self.logger.info(f"Real-time monitoring scheduled every {interval_minutes} minutes")
            else:
                self.scheduler.add_job(
                    self.run_real_time_monitoring,
                    'interval',
                    minutes=interval_minutes,
                    id='real_time_monitoring',
                    name='Real-time Stock Monitoring'
                )
                self.logger.info(f"Real-time monitoring scheduled every {interval_minutes} minutes")

            
            self.scheduler.add_job(
                self.run_startup_sequence,
                'date',
                id='startup_sequence',
                name='Startup Sequence'
            )
            
            self.logger.info(f"Scheduler initialized - real-time monitoring every {interval_minutes} minutes")
            
        except Exception as e:
            self.logger.error(f"Failed to setup scheduler: {e}")
            raise
    
    def run_startup_sequence(self) -> None:
        try:
            self.logger.info("Running startup sequence...")
            
            stock_config = self.config['stocks']
            tickers = self.data_fetcher.get_top_automotive_stocks(stock_config['count'])
            
            if not tickers:
                raise Exception("Failed to get automotive stock tickers")
            
            self.logger.info(f"Monitoring {len(tickers)} automotive stocks: {', '.join(tickers)}")
            
            self.alert_system.send_startup_notification(tickers)
            
            self.logger.info("Fetching initial historical data...")
            historical_data = self.data_fetcher.fetch_all_historical_data(
                tickers, 
                self.config['data']['historical_days']
            )
            
            for ticker, data in historical_data.items():
                if data is not None:
                    self.db_manager.insert_historical_data(ticker, data)
            
            self.logger.info("Startup sequence completed successfully")
            
        except Exception as e:
            self.logger.error(f"Startup sequence failed: {e}")
            self.alert_system.send_error_notification(str(e), "Startup sequence")
    
    def run_real_time_monitoring(self) -> None:
        try:
            self.logger.info("Starting real-time monitoring...")
            
            tickers = self.db_manager.get_all_tickers()
            if not tickers:
                self.logger.warning("No tickers to monitor in real-time.")
                return
            
            self.logger.info("Fetching LIVE current prices from Yahoo Finance with FORCE REFRESH...")
            current_prices = self.data_fetcher.force_refresh_all_prices(tickers)
            
            if not current_prices:
                self.logger.error("Failed to fetch any current prices")
                return
            
            stock_updates = []
            for ticker in tickers:
                if ticker in current_prices and current_prices[ticker] is not None:
                    current_price = current_prices[ticker].get('price')
                    previous_price = current_prices[ticker].get('previous_close', current_price)
                    
                    stock_update = {
                        'ticker': ticker,
                        'current_price': current_price,
                        'previous_price': previous_price,
                        'timestamp': current_prices[ticker].get('timestamp', datetime.now())
                    }
                    stock_updates.append(stock_update)
                    
                    self.db_manager.update_latest_price(ticker, current_prices[ticker])
                    
                    try:
                        from datetime import date
                        today = date.today()

                        if self._is_new_trading_day(ticker, today):
                            self.logger.info(f"New trading day detected for {ticker} - updating historical data")
                            historical_data = self.data_fetcher.fetch_historical_data(ticker, 150)
                            if historical_data is not None:
                                self.db_manager.insert_historical_data(ticker, historical_data)
                                self.logger.info(f"Updated historical data for {ticker} in real-time monitoring")
                    except Exception as e:
                        self.logger.warning(f"Could not update historical data for {ticker} in real-time: {e}")
                    
                    analysis_result = self.analytics.analyze_single_ticker(ticker)
                    
                    if analysis_result and analysis_result.get('alerts_triggered', False):
                        self.logger.info(f"Real-time alert triggered for {ticker} - sending immediate alert")
                        
                        full_analysis = self.analytics.analyze_all_tickers([ticker])
                        if ticker in full_analysis:
                            alert_result = full_analysis[ticker]
                            if self.alert_system.send_alert(ticker, alert_result):
                                self.logger.info(f"Real-time alert sent successfully for {ticker}")
                            else:
                                self.logger.error(f"Failed to send real-time alert for {ticker}")
                        else:
                            self.logger.warning(f"Could not get full analysis for {ticker} alert")
            
            if stock_updates:
                self.alert_system.send_real_time_update(stock_updates)
                self.logger.info("Real-time update sent successfully")
            else:
                self.logger.warning("No stock updates to send")
            
            self.logger.info(f"Real-time monitoring completed: {len(stock_updates)} stocks updated")
            
        except Exception as e:
            self.logger.error(f"Error in real-time monitoring: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _is_new_trading_day(self, ticker: str, today) -> bool:
        try:
            from stock.database import DatabaseManager
            
            if hasattr(self, 'db_manager') and self.db_manager:
                return True 
            else:
                return True
                
        except Exception as e:
            self.logger.warning(f"Error checking if new trading day for {ticker}: {e}")
            return True  
    

    
    def run_manual_alert_check(self) -> None:
        try:
            self.logger.info("Running manual alert check...")
            
            tickers = self.db_manager.get_all_tickers()
            if not tickers:
                self.logger.warning("No tickers to check for alerts.")
                return
            
            self.logger.info("Fetching current prices for alert check...")
            current_prices = self.data_fetcher.fetch_all_current_prices(tickers)
            
            for ticker, price_data in current_prices.items():
                if price_data is not None:
                    self.db_manager.update_latest_price(ticker, price_data)
            
            self.logger.info("Running analytics for alert check...")
            analysis_results = self.analytics.analyze_all_tickers(tickers)
            
            alerts_sent = 0
            for ticker, result in analysis_results.items():
                if result.get('alerts_triggered', False):
                    self.logger.info(f"Alert triggered for {ticker} - sending alert")
                    if self.alert_system.send_alert(ticker, result):
                        alerts_sent += 1
                        self.logger.info(f"Alert sent successfully for {ticker}")
                    else:
                        self.logger.error(f"Failed to send alert for {ticker}")
                else:
                    self.logger.info(f"No alerts triggered for {ticker}")
            
            self.logger.info(f"Manual alert check completed: {alerts_sent} alerts sent")
            
        except Exception as e:
            self.logger.error(f"Manual alert check failed: {e}")
            self.alert_system.send_error_notification(str(e), "Manual alert check")
    
    def run_manual_monitoring(self) -> None:
        try:
            self.logger.info("Running manual monitoring...")
            self.run_real_time_monitoring()
            self.logger.info("Manual monitoring completed")
            
        except Exception as e:
            self.logger.error(f"Manual monitoring failed: {e}")
    
    def run_manual_real_time(self) -> None:
        try:
            self.logger.info("Running manual real-time monitoring...")
            self.run_real_time_monitoring()
            self.logger.info("Manual real-time monitoring completed")
            
        except Exception as e:
            self.logger.error(f"Manual real-time monitoring failed: {e}")
    
    def start(self) -> None:
        try:
            self.logger.info("Starting stock monitoring system...")
            
            self.run_startup_sequence()
            
            self.logger.info("Starting scheduler...")
            self.scheduler.start()
            
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal")
        except Exception as e:
            self.logger.error(f"System startup failed: {e}")
            raise
        finally:
            self.shutdown()
    
    def shutdown(self) -> None:
        try:
            self.logger.info("Shutting down stock monitoring system...")
            
            if self.scheduler:
                self.scheduler.shutdown()
            
            if self.db_manager:
                self.db_manager.close()
            
            self.logger.info("System shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")


def main() -> None:
    try:
        monitor = StockMonitor()
        monitor.start()
        
    except Exception as e:
        logging.error(f"Application failed to start: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
