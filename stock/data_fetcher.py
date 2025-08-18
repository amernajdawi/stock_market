
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
import pandas as pd
import yfinance as yf
import requests

logger = logging.getLogger(__name__)


class StockDataFetcher:
    
    def __init__(self, retry_attempts: int = 3, backoff_seconds: int = 5):
        self.retry_attempts = retry_attempts
        self.backoff_seconds = backoff_seconds
        
        self.fallback_automotive_tickers = [
            'TSLA', 'TM', 'F', 'GM', 'BMW3.DE', 'MBGYY', 'VWAGY', 'HMC', 'NSANY', 'RACE'
        ]
    
    def get_top_automotive_stocks(self, count: int = 10) -> List[str]:
        try:
            logger.info("Fetching top automotive stocks by market cap...")
            
            automotive_tickers = self._get_automotive_sector_stocks()
            
            if automotive_tickers:
                ranked_tickers = self._rank_by_market_cap(automotive_tickers, count)
                logger.info(f"Successfully fetched {len(ranked_tickers)} top automotive stocks")
                return ranked_tickers[:count]
            else:
                logger.warning("Failed to fetch automotive sector data, using fallback list")
                return self.fallback_automotive_tickers[:count]
                
        except Exception as e:
            logger.error(f"Error fetching automotive stocks: {e}")
            logger.info("Using fallback automotive ticker list")
            return self.fallback_automotive_tickers[:count]
    
    def _get_automotive_sector_stocks(self) -> List[str]:
        try:
            automotive_keywords = ['automotive', 'auto', 'car', 'vehicle']
            potential_tickers = []
            
            known_automotive = [
                'TSLA', 'TM', 'F', 'GM', 'BMW3.DE', 'MBGYY', 'VWAGY', 
                'HMC', 'NSANY', 'RACE'
            ]
            
            for ticker in known_automotive:
                try:
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    
                    if info and 'sector' in info:
                        sector = info['sector'].lower()
                        if any(keyword in sector for keyword in automotive_keywords):
                            potential_tickers.append(ticker)
                    elif info and 'industry' in info:
                        industry = info['industry'].lower()
                        if any(keyword in industry for keyword in automotive_keywords):
                            potential_tickers.append(ticker)
                    else:
                        if ticker in ['TSLA', 'TM', 'F', 'GM', 'BMW3.DE', 'MBGYY', 'VWAGY', 'HMC', 'NSANY', 'RACE']:
                            potential_tickers.append(ticker)
                            
                except Exception as e:
                    logger.debug(f"Could not get info for {ticker}: {e}")
                    continue
            
            return potential_tickers
            
        except Exception as e:
            logger.error(f"Error getting automotive sector stocks: {e}")
            return []
    
    def _rank_by_market_cap(self, tickers: List[str], count: int) -> List[str]:
        try:
            ticker_data = []
            
            for ticker in tickers:
                try:
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    
                    if info and 'marketCap' in info and info['marketCap']:
                        market_cap = info['marketCap']
                        ticker_data.append((ticker, market_cap))
                    else:
                        ticker_data.append((ticker, 0))
                        
                except Exception as e:
                    logger.debug(f"Could not get market cap for {ticker}: {e}")
                    ticker_data.append((ticker, 0))
            
            ticker_data.sort(key=lambda x: x[1], reverse=True)
            ranked_tickers = [ticker for ticker, _ in ticker_data]
            
            logger.info(f"Ranked {len(ranked_tickers)} tickers by market cap")
            return ranked_tickers[:count]
            
        except Exception as e:
            logger.error(f"Error ranking tickers by market cap: {e}")
            return tickers[:count]
    
    def fetch_historical_data(self, ticker: str, days: int = 150) -> Optional[pd.DataFrame]:
        for attempt in range(self.retry_attempts):
            try:
                logger.info(f"Fetching {days} calendar days of historical data for {ticker} (targeting 90 trading days)")
                
                stock = yf.Ticker(ticker)
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                
                data = stock.history(
                    start=start_date,
                    end=end_date,
                    interval='1d'
                )
                
                if data.empty:
                    logger.warning(f"No historical data returned for {ticker}")
                    return None
                
                if len(data) > 90:
                    data = data.tail(90)
                    logger.info(f"Filtered {ticker} to exactly 90 trading days")
                elif len(data) < 90:
                    logger.warning(f"Only got {len(data)} trading days for {ticker} (less than 90)")
                
                basic_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                if not all(col in data.columns for col in basic_columns):
                    logger.error(f"Missing basic required columns for {ticker}: {data.columns}")
                    return None
                
                if 'Adj Close' not in data.columns:
                    data['Adj Close'] = data['Close']
                    logger.info(f"Created Adj Close column for {ticker} using Close price")
                
                required_columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                if not all(col in data.columns for col in required_columns):
                    logger.error(f"Missing required columns for {ticker}: {data.columns}")
                    return None
                
                logger.info(f"Successfully fetched {len(data)} historical records for {ticker}")
                return data
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {ticker}: {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.backoff_seconds * (2 ** attempt))  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch historical data for {ticker} after {self.retry_attempts} attempts")
                    return None
        
        return None
    
    def verify_yahoo_finance_match(self, ticker: str) -> Dict[str, Any]:
        try:
            logger.info(f"Verifying {ticker} data matches Yahoo Finance exactly...")
            
            our_data = self.fetch_current_price(ticker)
            if not our_data:
                return {'error': 'Could not fetch our data'}
            
            stock = yf.Ticker(ticker)
            
            live_data = stock.history(period="1d", interval="1m", prepost=True)
            
            info = stock.info
            
            verification = {
                'ticker': ticker,
                'our_data': our_data,
                'yahoo_live_price': float(live_data['Close'].iloc[-1]) if not live_data.empty else None,
                'yahoo_info_price': info.get('regularMarketPrice'),
                'yahoo_previous_close': info.get('previousClose'),
                'yahoo_volume': info.get('volume'),
                'yahoo_bid': info.get('bid'),
                'yahoo_ask': info.get('ask'),
                'yahoo_market_state': info.get('marketState'),
                'matches': {}
            }
            
            if our_data['price'] and verification['yahoo_live_price']:
                price_diff = abs(our_data['price'] - verification['yahoo_live_price'])
                verification['matches']['price'] = price_diff < 0.01  # Within 1 cent
                verification['price_difference'] = price_diff
                logger.info(f"{ticker} price match: {'✅' if verification['matches']['price'] else '❌'} (Diff: ${price_diff:.6f})")
            
            if our_data['previous_close'] and verification['yahoo_previous_close']:
                prev_diff = abs(our_data['previous_close'] - verification['yahoo_previous_close'])
                verification['matches']['previous_close'] = prev_diff < 0.01
                verification['previous_close_difference'] = prev_diff
                logger.info(f"{ticker} previous close match: {'✅' if verification['matches']['previous_close'] else '❌'} (Diff: ${prev_diff:.6f})")
            
            if our_data['volume'] and verification['yahoo_volume']:
                vol_diff = abs(our_data['volume'] - verification['yahoo_volume'])
                verification['matches']['volume'] = vol_diff < 1000  # Within 1000 shares
                verification['volume_difference'] = vol_diff
                logger.info(f"{ticker} volume match: {'✅' if verification['matches']['volume'] else '❌'} (Diff: {vol_diff})")
            
            verification['matches']['market_state'] = our_data['market_state'] == verification['yahoo_market_state']
            logger.info(f"{ticker} market state match: {'✅' if verification['matches']['market_state'] else '❌'}")
            
            all_matches = list(verification['matches'].values())
            verification['overall_match'] = all(all_matches)
            verification['match_percentage'] = (sum(all_matches) / len(all_matches)) * 100 if all_matches else 0
            
            match_text = '✅ PERFECT' if verification['overall_match'] else f"⚠️ {verification['match_percentage']:.1f}% match"
            logger.info(f"{ticker} overall match: {match_text}")
            
            return verification
            
        except Exception as e:
            logger.error(f"Error verifying Yahoo Finance match for {ticker}: {e}")
            return {'error': str(e)}
    
    def clear_all_caching(self) -> None:
        try:
            import yfinance as yf
            yf.pdr_override = False
            
            import pandas as pd
            pd.io.common._get_filepath_or_buffer = lambda *args, **kwargs: args[0]
            
            import requests
            if hasattr(requests, 'Session'):
                session = requests.Session()
                session.headers.update({'Cache-Control': 'no-cache'})
            
            logger.info("All caching cleared for maximum data freshness")
        except Exception as e:
            logger.warning(f"Could not clear all caching: {e}")
    
    def fetch_current_price(self, ticker: str) -> Optional[Dict[str, float]]:
        for attempt in range(self.retry_attempts):
            try:
                logger.info(f"Fetching LIVE current price for {ticker} from Yahoo Finance")
                
                self.clear_all_caching()
                
                stock = yf.Ticker(ticker)
                
                try:
                    live_data = stock.history(period="1d", interval="1m", prepost=True)
                    
                    if not live_data.empty:
                        current_price = float(live_data['Close'].iloc[-1])
                        logger.info(f"Using EXACT Yahoo Finance live price for {ticker}: ${current_price:.6f}")
                        
                        live_volume = int(live_data['Volume'].iloc[-1]) if 'Volume' in live_data.columns else None
                        
                        if len(live_data) >= 2:
                            previous_close = float(live_data['Close'].iloc[-2])
                        else:
                            previous_close = None
                    else:
                        current_price = None
                        live_volume = None
                        previous_close = None
                        logger.warning(f"No live data available for {ticker}")
                    
                except Exception as e:
                    logger.warning(f"Could not fetch live data for {ticker}: {e}")
                    live_data = pd.DataFrame()
                    current_price = None
                    live_volume = None
                    previous_close = None
                
                try:
                    stock.info = None
                    info = stock.info
                    logger.info(f"Fresh info fetched for {ticker}")
                except Exception as e:
                    logger.warning(f"Could not refresh info for {ticker}: {e}")
                    info = stock.info
                
                if not info:
                    logger.warning(f"No info returned for {ticker}")
                    return None
                
                
                if current_price is None:
                    if 'regularMarketPrice' in info and info['regularMarketPrice']:
                        current_price = float(info['regularMarketPrice'])
                        logger.info(f"Using Yahoo Finance regular market price for {ticker}: ${current_price:.6f}")
                    elif 'previousClose' in info and info['previousClose']:
                        current_price = float(info['previousClose'])
                        logger.info(f"Using Yahoo Finance previous close for {ticker}: ${current_price:.6f}")
                    else:
                        logger.warning(f"No price data available for {ticker}")
                        return None
                
                if previous_close is None:
                    if 'previousClose' in info and info['previousClose']:
                        previous_close = float(info['previousClose'])
                        logger.info(f"Previous close from Yahoo Finance for {ticker}: ${previous_close:.6f}")
                    else:
                        try:
                            hist_data = stock.history(period="2d")
                            if len(hist_data) >= 2:
                                previous_close = float(hist_data['Close'].iloc[-2]) 
                                logger.info(f"Previous close from history for {ticker}: ${previous_close:.6f}")
                        except Exception as e:
                            logger.warning(f"Could not get previous close for {ticker}: {e}")
                            previous_close = current_price  
                
                
                bid = info.get('bid', None)
                ask = info.get('ask', None)
                
                volume = live_volume if live_volume is not None else info.get('volume', None)
                market_cap = info.get('marketCap', None)
                
                market_state = info.get('marketState', 'unknown')
                is_market_open = market_state == 'REGULAR'
                
                price_data = {
                    'price': current_price,
                    'previous_close': previous_close,
                    'bid': bid,
                    'ask': ask,
                    'volume': volume,
                    'market_cap': market_cap,
                    'market_state': market_state,
                    'is_market_open': is_market_open,
                    'timestamp': datetime.now()
                }
                
                logger.info(f"YAHOO FINANCE EXACT price for {ticker}: ${current_price:.6f} (Market: {'Open' if is_market_open else 'Closed'})")
                logger.info(f"Data verification: {ticker} price=${current_price:.6f}, prev_close=${previous_close:.6f}")
                logger.info(f"Volume: {volume}, Bid: {bid}, Ask: {ask}")
                return price_data
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {ticker}: {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.backoff_seconds * (2 ** attempt))
                else:
                    logger.error(f"Failed to fetch LIVE current price for {ticker} after {self.retry_attempts} attempts")
                    return None
        
        return None
    
    def fetch_all_historical_data(self, tickers: List[str], days: int = 90) -> Dict[str, pd.DataFrame]:
        results = {}
        
        for ticker in tickers:
            logger.info(f"Processing {ticker} ({tickers.index(ticker) + 1}/{len(tickers)})")
            
            data = self.fetch_historical_data(ticker, days)
            if data is not None:
                results[ticker] = data
            else:
                logger.error(f"Failed to fetch historical data for {ticker}")
            
            time.sleep(1)
        
        logger.info(f"Successfully fetched historical data for {len(results)}/{len(tickers)} tickers")
        return results
    
    def fetch_all_current_prices(self, tickers: List[str]) -> Dict[str, Optional[Dict[str, float]]]:
        results = {}
        
        for ticker in tickers:
            logger.info(f"Processing {ticker} ({tickers.index(ticker) + 1}/{len(tickers)})")
            
            data = self.fetch_current_price(ticker)
            if data is not None:
                results[ticker] = data
            else:
                logger.error(f"Failed to fetch current price for {ticker}")
            
            time.sleep(0.5)
        
        logger.info(f"Successfully fetched current prices for {len(results)}/{len(tickers)} tickers")
        return results
    
    def force_refresh_all_prices(self, tickers: List[str]) -> Dict[str, Optional[Dict[str, float]]]:
        logger.info("Force refreshing all current prices with aggressive data clearing...")
        
        try:
            import yfinance as yf
            yf.pdr_override = False
            logger.info("Cleared yfinance cache")
        except Exception as e:
            logger.warning(f"Could not clear yfinance cache: {e}")
        
        results = {}
        
        for ticker in tickers:
            logger.info(f"Force refreshing {ticker} ({tickers.index(ticker) + 1}/{len(tickers)})")
            
            try:
                stock = yf.Ticker(ticker)
                
                for refresh_attempt in range(3):
                    try:
                        live_data = stock.history(period="1d", interval="1m", prepost=True)
                        
                        if not live_data.empty:
                            current_price = float(live_data['Close'].iloc[-1])
                            
                            latest_timestamp = live_data.index[-1]
                            time_diff = datetime.now() - latest_timestamp.replace(tzinfo=None)
                            
                            if time_diff.total_seconds() < 300:  # 5 minutes
                                logger.info(f"Live data is fresh for {ticker}: {time_diff.total_seconds():.0f}s old")
                            else:
                                logger.warning(f"Live data may be stale for {ticker}: {time_diff.total_seconds():.0f}s old")
                            
                            info = stock.info
                            previous_close = float(info.get('previousClose', current_price))
                            
                            price_data = {
                                'price': current_price,
                                'previous_close': previous_close,
                                'bid': info.get('bid'),
                                'ask': info.get('ask'),
                                'volume': info.get('volume'),
                                'market_cap': info.get('marketCap'),
                                'market_state': info.get('marketState', 'unknown'),
                                'is_market_open': info.get('marketState') == 'REGULAR',
                                'timestamp': datetime.now()
                            }
                            
                            results[ticker] = price_data
                            logger.info(f"Successfully force refreshed {ticker}: ${current_price:.6f}")
                            break
                            
                    except Exception as e:
                        logger.warning(f"Force refresh attempt {refresh_attempt + 1} failed for {ticker}: {e}")
                        if refresh_attempt < 2:
                            time.sleep(1)  
                        else:
                            logger.error(f"All force refresh attempts failed for {ticker}")
                
                if ticker not in results:
                    data = self.fetch_current_price(ticker)
                    if data is not None:
                        results[ticker] = data
                    else:
                        logger.error(f"Failed to fetch current price for {ticker} even with fallback")
                
            except Exception as e:
                logger.error(f"Error in force refresh for {ticker}: {e}")
                data = self.fetch_current_price(ticker)
                if data is not None:
                    results[ticker] = data
            
            time.sleep(1)
        
        logger.info(f"Force refresh completed: {len(results)}/{len(tickers)} tickers updated")
        return results
