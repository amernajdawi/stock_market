

import logging
import asyncio
import threading
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests
import re

logger = logging.getLogger(__name__)


class TelegramAlertSystem:
    
    def __init__(self, bot_token: str, chat_id: str, db_manager=None):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.db_manager = db_manager
        self.last_update_id = 0
        self.bot_running = False
        self.bot_thread = None
        
        if not self._test_connection():
            logger.error("Failed to establish Telegram connection")
        else:
            logger.info("Telegram connection established successfully")
    
    def _test_connection(self) -> bool:
        try:
            response = requests.get(f"{self.base_url}/getMe", timeout=10)
            if response.status_code == 200:
                bot_info = response.json()
                if bot_info.get('ok'):
                    logger.info(f"Connected to Telegram bot: @{bot_info['result']['username']}")
                    return True
                else:
                    logger.error(f"Telegram API error: {bot_info}")
                    return False
            else:
                logger.error(f"Telegram API HTTP error: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to test Telegram connection: {e}")
            return False
    
    def send_message(self, message: str, parse_mode: str = "HTML") -> bool:
        try:
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': parse_mode
            }
            
            response = requests.post(
                f"{self.base_url}/sendMessage",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('ok'):
                    logger.info("Telegram message sent successfully")
                    return True
                else:
                    logger.error(f"Telegram API error: {result}")
                    return False
            else:
                logger.error(f"Telegram API HTTP error: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False
    
    def send_alert(self, ticker: str, result: Dict[str, Any]) -> bool:
        try:
            current_price = result.get('current_price', 0)
            timestamp = result.get('timestamp', datetime.now())
            averages = result.get('averages', {})
            alert_conditions = result.get('alert_conditions', {})
            
            if not current_price:
                logger.error(f"No current price in result for {ticker}")
                return False
            
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S UTC") if hasattr(timestamp, 'strftime') else str(timestamp)
            details = []
            for period, condition in alert_conditions.items():
                if condition.get('alert_triggered', False):
                    avg_value = condition.get('average', 0)
                    if current_price < avg_value:
                        diff = avg_value - current_price
                        pct_diff = (diff / avg_value) * 100
                        details.append({
                            'period': period,
                            'average': avg_value,
                            'difference': diff,
                            'percentage': pct_diff
                        })
            
            if not details:
                logger.warning(f"No alert conditions met for {ticker}")
                return False
            
            details.sort(key=lambda x: x['percentage'], reverse=True)
            
            message = f"""ALERT: {ticker}

Price: ${current_price:.2f}
Time: {timestamp_str}

ALERTS:"""
            
            for detail in details:
                period_num = detail['period'].split('_')[0] if '_' in detail['period'] else detail['period']
                period_name = self._get_period_name(period_num)
                message += f"""
{period_name}: ${detail['average']:.2f}
Below: ${detail['difference']:.2f} ({detail['percentage']:.2f}%)"""
            
            message += f"""

#{ticker}"""

            logger.info(f"Attempting to send message for {ticker}:")
            logger.info(f"Message length: {len(message)}")
            logger.info(f"Message preview: {message[:200]}...")
            
            try:
                result = self.send_message(message, parse_mode=None)
                if result:
                    return True
            except Exception as e:
                logger.warning(f"Plain text failed: {e}")
            
            try:
                result = self.send_message(message, parse_mode='HTML')
                if result:
                    return True
            except Exception as e:
                logger.warning(f"HTML mode failed: {e}")
            
            try:
                result = self.send_message(message, parse_mode='Markdown')
                if result:
                    return True
            except Exception as e:
                logger.warning(f"Markdown mode failed: {e}")
            
            logger.error(f"All message sending methods failed for {ticker}")
            return False
            
        except Exception as e:
            logger.error(f"Failed to send detailed alert for {ticker}: {e}")
            return False
    
    def _build_alert_message(self, ticker: str, alert_data: Dict[str, Any]) -> str:
        try:
            current_price = alert_data.get('current_price', 0)
            timestamp = alert_data.get('timestamp', datetime.now())
            
            if isinstance(timestamp, str):
                timestamp_str = timestamp
            else:
                timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
            
            message = f"""
🚨 <b>ALERT: {ticker}</b> 🚨

💰 <b>Current:</b> ${current_price:.2f}
⏰ <b>Time:</b> {timestamp_str}

📉 <b>Below Moving Averages:</b>
"""
            
            alert_conditions = alert_data.get('alert_conditions', {})
            for period, condition in alert_conditions.items():
                if condition.get('alert_triggered', False):
                    avg_value = condition.get('average', 0)
                    abs_diff = condition.get('absolute_difference', 0)
                    pct_diff = condition.get('percent_difference', 0)
                    
                    period_name = period.replace('_', ' ').title()
                    message += f"""
   📊 <b>{period_name}:</b> ${avg_value:.2f}
   💰 Gap: ${abs_diff:.2f} ({pct_diff:.2f}%)
"""
            
            message += f"""

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ <b>DISCLAIMER</b>
   Automated alert based on technical analysis
   Price below moving average(s) - review recommended
   Not financial advice

🔍 <b>Stock Monitor System</b>
"""
            
            return message.strip()
            
        except Exception as e:
            logger.error(f"Error building alert message for {ticker}: {e}")
            return f"Error building alert message for {ticker}: {e}"
    
    def send_daily_summary(self, summary_data: Dict[str, Any]) -> bool:
        try:
            from datetime import datetime
            current_time = datetime.now()
            
            total_stocks = summary_data.get('total_stocks', 0)
            stocks_below_7 = summary_data.get('stocks_below_7', 0)
            stocks_below_30 = summary_data.get('stocks_below_30', 0)
            stocks_below_90 = summary_data.get('stocks_below_90', 0)
            total_alerts = summary_data.get('total_alerts', 0)
            market_status = summary_data.get('market_status', 'Unknown')
            
            pct_7 = (stocks_below_7 / total_stocks * 100) if total_stocks > 0 else 0
            pct_30 = (stocks_below_30 / total_stocks * 100) if total_stocks > 0 else 0
            pct_90 = (stocks_below_90 / total_stocks * 100) if total_stocks > 0 else 0
            
            message = f"""📊 <b>DAILY STOCK MONITORING SUMMARY</b> 📊

⏰ {current_time.strftime('%H:%M:%S UTC')} • {current_time.strftime('%Y-%m-%d')}
🌍 Market: {market_status}
📊 Coverage: Last 90 Trading Days

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 <b>PORTFOLIO OVERVIEW</b>
    Total Stocks: {total_stocks} 
   📊 Data Source: Yahoo Finance (Real-time)
   🔄 Monitoring: Active 24/7

📉 <b>ALERT ANALYSIS</b>
   📊 7-Day: {stocks_below_7}/{total_stocks} ({pct_7:.1f}%)
   📊 30-Day: {stocks_below_30}/{total_stocks} ({pct_30:.1f}%)
   📊 90-Day: {stocks_below_90}/{total_stocks} ({pct_90:.1f}%)
   🚨 Total Alerts: {total_alerts}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 <b>MARKET SENTIMENT</b>
   📈 Trend: {self._get_market_sentiment(stocks_below_7, stocks_below_30, stocks_below_90)}
   ⚠️ Risk: {self._get_market_risk_level(stocks_below_7, stocks_below_30, stocks_below_90)}
   💡 Opportunity: {self._get_opportunity_index(stocks_below_7, stocks_below_30, stocks_below_90)}

📊 <b>KEY INSIGHTS</b>
{self._get_key_insights(summary_data)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔔 <b>SYSTEM STATUS</b>
   ✅ Real-time Monitoring: Active
   ✅ Alert Triggers: Price below moving averages
   
   ✅ System Health: All components operational

📱 <b>NEXT STEPS</b>
   • Monitor individual stock alerts
   • Review market trends daily
   • Check specific stocks of interest
   • System updates automatically

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ <b>DISCLAIMER</b>
   Technical analysis only - not financial advice
   Do your own research and due diligence
   Past performance doesn't guarantee future results"""

            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Failed to send detailed daily summary: {e}")
            return False
    
    def _build_daily_summary_message(self, summary_data: Dict[str, Any], analysis_results: Dict[str, Dict[str, Any]]) -> str:
        try:
            total_tickers = summary_data.get('total_tickers', 0)
            tickers_with_alerts = summary_data.get('tickers_with_alerts', 0)
            alerts_7_day = summary_data.get('alerts_7_day', 0)
            alerts_30_day = summary_data.get('alerts_30_day', 0)
            alerts_90_day = summary_data.get('alerts_90_day', 0)
            tickers_analyzed = summary_data.get('tickers_analyzed', 0)
            
            message = f"""
📊 <b>DAILY STOCK MONITORING SUMMARY</b> 📊

📅 <b>Date:</b> {datetime.now().strftime('%Y-%m-%d')}
⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')} UTC

📈 <b>Overview:</b>
• Total Tickers Monitored: {total_tickers}
• Tickers Analyzed: {tickers_analyzed}
• Tickers with Alerts: {tickers_with_alerts}

🚨 <b>Alert Summary:</b>
• 7-Day Average Alerts: {alerts_7_day}
• 30-Day Average Alerts: {alerts_30_day}
• 90-Day Average Alerts: {alerts_90_day}
"""
            
            if tickers_with_alerts > 0:
                message += f"\n📋 <b>Tickers with Alerts:</b>\n"
                
                for ticker, result in analysis_results.items():
                    if result.get('alerts_triggered', False):
                        current_price = result.get('current_price', 0)
                        alert_count = len(result.get('alert_conditions', {}))
                        
                        message += f"• <b>{ticker}</b>: ${current_price:.2f} ({alert_count} alert(s))\n"
            
            if tickers_analyzed > 0:
                message += f"""

📊 <b>Performance Summary:</b>
• Alert Rate: {(tickers_with_alerts / tickers_analyzed * 100):.1f}%
• Market Status: {'Bearish' if tickers_with_alerts > tickers_analyzed / 2 else 'Mixed' if tickers_with_alerts > 0 else 'Bullish'}
"""
            
            message += f"""


📱 <b>Automated by Stock Monitor</b>
"""
            
            return message.strip()
            
        except Exception as e:
            logger.error(f"Error building daily summary message: {e}")
            return f"Error building daily summary message: {e}"
    
    def send_error_notification(self, error_message: str, context: str = "") -> bool:
        try:
            message = f"""
⚠️ <b>SYSTEM ERROR NOTIFICATION</b> ⚠️

❌ <b>Error:</b> {error_message}

{f"📝 <b>Context:</b> {context}" if context else ""}

⏰ <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

🔧 <b>Action Required:</b> Check system logs and database connection
"""
            
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Failed to send error notification: {e}")
            return False
    
    def send_startup_notification(self, tickers: List[str]) -> bool:
        try:
            message = f"""
 <b>CUSTOM STOCK WATCHLIST ACTIVATED</b> 

 <b>Status:</b> Personal monitoring system is now live

 <b>Monitoring:</b> {len(tickers)} companies from your custom watchlist
 <b>Tickers:</b> {', '.join(tickers)}

 <b>Features:</b>
   • Real-time price monitoring every 5 minutes
   • Moving averages: 7, 30, 90 trading days
   • Market-based alert reset (09:00 Vienna time)
   • Add/remove companies via database

 <b>Alerts:</b> Telegram notifications when prices drop below averages

 <b>Started:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

 <b>Tip:</b> Use manage_watchlist.py to add new companies anytime

 <b>Powered by AI Stock Monitor</b>
"""
            
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Failed to send startup notification: {e}")
            return False

    def send_real_time_update(self, stock_updates: List[Dict[str, Any]]) -> bool:
        try:
            from datetime import datetime
            current_time = datetime.now()
            
            from datetime import timezone, timedelta
            
            dst_start = current_time.replace(month=3, day=31, hour=2, minute=0, second=0, microsecond=0)
            dst_end = current_time.replace(month=10, day=31, hour=3, minute=0, second=0, microsecond=0)
            
            if dst_start <= current_time <= dst_end:
                eu_offset = 2  
                timezone_name = "CEST"
            else:
                eu_offset = 1  
                timezone_name = "CET"
            
            eu_time = current_time.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=eu_offset)))
            
            if self._is_market_open():
                market_status = '🟢 EU MARKETS OPEN'
            else:
                recent_updates = any(
                    hasattr(stock.get('timestamp', current_time), 'strftime') and
                    (current_time - stock.get('timestamp', current_time)).total_seconds() < 1800
                    for stock in stock_updates
                )
                if recent_updates:
                    market_status = '🟡 PRE-MARKET / AFTER-HOURS'
                else:
                    market_status = '🔴 EU MARKETS CLOSED'
            
            message = f""" <b>AUTOMOTIVE STOCKS LIVE UPDATE</b> 

⏰ {eu_time.strftime('%H:%M:%S')} {timezone_name} • {current_time.strftime('%H:%M:%S UTC')}
🌍 Market: {market_status}
🔄 Updates: Every 5 minutes

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""
            
            for stock in stock_updates:
                ticker = stock['ticker']
                current_price = stock['current_price']
                previous_price = stock.get('previous_price', current_price)
                
                change = current_price - previous_price
                change_pct = (change / previous_price * 100) if previous_price > 0 else 0
                
                if abs(change) < 0.01:  
                    status_emoji = "🟡"
                    change_text = "Minimal change"
                elif change > 0:
                    status_emoji = "🟢"
                    change_text = f"+${change:.2f} (+{change_pct:.2f}%)"
                else:
                    status_emoji = "🔴"
                    change_text = f"${change:.2f} ({change_pct:.2f}%)"
                
                timestamp = stock.get('timestamp', current_time)
                if hasattr(timestamp, 'strftime'):
                    time_str = timestamp.strftime('%H:%M:%S')
                else:
                    time_str = str(timestamp)[:8] if len(str(timestamp)) > 8 else str(timestamp)
                
                message += f"""
{status_emoji} <b>{ticker}</b>
   💰 ${current_price:.2f}  {change_text}
   📊 Prev: ${previous_price:.2f}  ⏰ {time_str}"""
            
            total_stocks = len(stock_updates)
            up_stocks = sum(1 for s in stock_updates if s.get('current_price', 0) > s.get('previous_price', 0) + 0.01)
            down_stocks = sum(1 for s in stock_updates if s.get('current_price', 0) < s.get('previous_price', 0) - 0.01)
            flat_stocks = total_stocks - up_stocks - down_stocks
            
            message += f"""

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 <b>MARKET SUMMARY</b>
   🟢 Up: {up_stocks}  🔴 Down: {down_stocks}  🟡 Flat: {flat_stocks}

🎯 <b>KEY INSIGHTS</b>
{self._get_real_time_insights(stock_updates)}

📱 <b>NEXT UPDATE</b>
   ⏰ In 5 minutes • 🕐 Daily Summary: 18:00 {timezone_name}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Failed to send real-time update: {e}")
            return False
    
    def _get_real_time_insights(self, stock_updates: List[Dict[str, Any]]) -> str:
        insights = []
        
        if not stock_updates:
            return "• No stock data available"
        
        changes = [(s['ticker'], abs(s.get('current_price', 0) - s.get('previous_price', 0))) 
                  for s in stock_updates if s.get('previous_price')]
        
        if changes:
            changes.sort(key=lambda x: x[1], reverse=True)
            biggest_mover = changes[0]
            insights.append(f"🏆 <b>Top Mover:</b> {biggest_mover[0]} (${biggest_mover[1]:.2f})")
        
        up_count = sum(1 for s in stock_updates if s.get('current_price', 0) > s.get('previous_price', 0))
        down_count = sum(1 for s in stock_updates if s.get('current_price', 0) < s.get('previous_price', 0))
        
        if up_count > down_count:
            insights.append("📈 <b>Sentiment:</b> 🟢 Bullish momentum")
        elif down_count > up_count:
            insights.append("📉 <b>Sentiment:</b> 🔴 Bearish pressure")
        else:
            insights.append("📊 <b>Sentiment:</b> 🟡 Mixed signals")
        
        if len(changes) > 1:
            avg_change = sum(c[1] for c in changes) / len(changes)
            if avg_change > 2.0:
                insights.append("⚡ <b>Volatility:</b> 🔴 High - significant swings")
            elif avg_change > 1.0:
                insights.append("⚡ <b>Volatility:</b> 🟡 Medium - moderate movement")
            else:
                insights.append("⚡ <b>Volatility:</b> 🟢 Low - stable prices")
        
        if not insights:
            insights.append("📊 Market showing normal trading activity")
        
        return "\n".join(insights)

    def _get_period_name(self, period: str) -> str:
        """Get human-readable period name."""
        period_map = {
            '7': '7-Day Average',
            '30': '30-Day Average', 
            '90': '90-Day Average'
        }
        return period_map.get(period, f'{period}-Day Average')
    
    def _is_market_open(self) -> bool:
        try:
            from datetime import datetime, timezone, timedelta
            now = datetime.now(timezone.utc)
            
            dst_start = now.replace(month=3, day=31, hour=2, minute=0, second=0, microsecond=0)
            dst_end = now.replace(month=10, day=31, hour=3, minute=0, second=0, microsecond=0)
            
            if dst_start <= now <= dst_end:
                eu_offset = 2  
            else:
                eu_offset = 1  
            
            eu_time = now.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=eu_offset)))
            
            market_start = eu_time.replace(hour=9, minute=0, second=0, microsecond=0)
            market_end = eu_time.replace(hour=17, minute=30, second=0, microsecond=0)
            
            is_open = market_start <= eu_time <= market_end
            
            logger.info(f"EU Market status: {eu_time.strftime('%H:%M:%S')} CET/CEST, Market {'OPEN' if is_open else 'CLOSED'}")
            
            return is_open
            
        except Exception as e:
            logger.warning(f"Error checking market status: {e}")
            from datetime import datetime, time
            now = datetime.now()
            return time(9, 0) <= now.time() <= time(17, 30)
    
    def _get_trend_analysis(self, current_price: float, averages: Dict[str, float]) -> str:
        if '7' in averages and '30' in averages and '90' in averages:
            if current_price < averages['7'] < averages['30'] < averages['90']:
                return "🔴 Strong Downtrend (Below all averages)"
            elif current_price < averages['7'] < averages['30']:
                return "🟡 Short-term Weakness (Below 7 & 30-day)"
            elif current_price < averages['7']:
                return "🟠 Minor Weakness (Below 7-day only)"
            else:
                return "🟢 Above All Averages (Strong position)"
        return "📊 Trend analysis unavailable"
    
    def _get_volatility_level(self, details: List[Dict]) -> str:
        if not details:
            return "📊 Low volatility"
        
        max_pct = max(d['percentage'] for d in details)
        if max_pct > 10:
            return "🔴 High volatility (>10% below average)"
        elif max_pct > 5:
            return "🟡 Medium volatility (5-10% below average)"
        else:
            return "🟢 Low volatility (<5% below average)"
    
    def _get_support_levels(self, averages: Dict[str, float]) -> str:
        levels = []
        for period, avg in averages.items():
            period_name = self._get_period_name(period)
            levels.append(f"${avg:.2f} ({period_name})")
        return " | ".join(levels)
    
    def _get_alert_level(self, details: List[Dict]) -> str:
        if not details:
            return "🟢 No alerts"
        
        max_pct = max(d['percentage'] for d in details)
        if max_pct > 15:
            return "🔴 HIGH - Significant price decline"
        elif max_pct > 10:
            return "🟠 MEDIUM - Notable weakness"
        elif max_pct > 5:
            return "🟡 LOW - Minor weakness"
        else:
            return "🟢 MINIMAL - Slight decline"
    
    def _get_recommendation(self, details: List[Dict]) -> str:
        if not details:
            return "Monitor for opportunities"
        
        max_pct = max(d['percentage'] for d in details)
        if max_pct > 15:
            return "Consider reviewing fundamentals - significant decline may indicate oversold conditions"
        elif max_pct > 10:
            return "Watch for potential reversal - price below key moving averages"
        elif max_pct > 5:
            return "Monitor closely - minor weakness developing"
        else:
            return "Normal market fluctuation - continue monitoring"

    def _get_market_sentiment(self, below_7: int, below_30: int, below_90: int) -> str:
        total_alerts = below_7 + below_30 + below_90
        
        if total_alerts == 0:
            return "🟢 Bullish - All stocks above averages"
        elif total_alerts <= 3:
            return "🟡 Neutral - Minor weakness in some stocks"
        elif total_alerts <= 6:
            return "🟠 Bearish - Notable weakness developing"
        else:
            return "🔴 Very Bearish - Significant weakness across portfolio"
    
    def _get_market_risk_level(self, below_7: int, below_30: int, below_90: int) -> str:
        if below_90 > 0:
            return "🔴 HIGH - Long-term trend weakness"
        elif below_30 > 0:
            return "🟠 MEDIUM - Medium-term concerns"
        elif below_7 > 0:
            return "🟡 LOW - Short-term volatility"
        else:
            return "🟢 MINIMAL - Strong market position"
    
    def _get_opportunity_index(self, below_7: int, below_30: int, below_90: int) -> str:
        if below_90 > 0:
            return "🔴 Low - Long-term weakness suggests caution"
        elif below_30 > 0:
            return "🟡 Medium - Some opportunities for value"
        elif below_7 > 0:
            return "🟢 High - Short-term dips may present opportunities"
        else:
            return "🔵 Very High - Strong momentum, consider momentum strategies"
    
    def _get_key_insights(self, summary_data: Dict[str, Any]) -> str:
        insights = []
        
        total_stocks = summary_data.get('total_stocks', 0)
        stocks_below_7 = summary_data.get('stocks_below_7', 0)
        stocks_below_30 = summary_data.get('stocks_below_30', 0)
        stocks_below_90 = summary_data.get('stocks_below_90', 0)
        
        if stocks_below_90 > 0:
            insights.append("• Long-term trend weakness detected in some stocks")
        
        if stocks_below_30 > stocks_below_7:
            insights.append("• Medium-term weakness exceeds short-term concerns")
        
        if stocks_below_7 > total_stocks * 0.5:
            insights.append("• Majority of stocks showing short-term weakness")
        
        if stocks_below_7 == 0 and stocks_below_30 == 0:
            insights.append("• Strong market momentum across all timeframes")
        
        if not insights:
            insights.append("• Market showing mixed signals - monitor individual stocks")
        
        return "\n".join(insights)
    
    def start_bot_listener(self):
        """Start the Telegram bot listener in a separate thread"""
        if not self.bot_running:
            self.bot_running = True
            self.bot_thread = threading.Thread(target=self._bot_polling_loop, daemon=True)
            self.bot_thread.start()
            logger.info("Telegram bot listener started")
    
    def stop_bot_listener(self):
        """Stop the Telegram bot listener"""
        self.bot_running = False
        if self.bot_thread:
            self.bot_thread.join(timeout=5)
        logger.info("Telegram bot listener stopped")
    
    def _bot_polling_loop(self):
        """Main polling loop for Telegram bot"""
        while self.bot_running:
            try:
                self._process_updates()
                time.sleep(2)  # Poll every 2 seconds
            except Exception as e:
                logger.error(f"Error in bot polling loop: {e}")
                time.sleep(5)  # Wait longer on error
    
    def _process_updates(self):
        """Process incoming Telegram updates"""
        try:
            url = f"{self.base_url}/getUpdates"
            params = {
                'offset': self.last_update_id + 1,
                'timeout': 1,
                'allowed_updates': ['message']
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('ok'):
                    updates = data.get('result', [])
                    for update in updates:
                        self._handle_update(update)
                        self.last_update_id = update['update_id']
        except Exception as e:
            logger.error(f"Error processing Telegram updates: {e}")
    
    def _handle_update(self, update: Dict):
        """Handle a single Telegram update"""
        try:
            message = update.get('message')
            if not message:
                return
            
            # Only process messages from the configured chat
            if str(message['chat']['id']) != str(self.chat_id):
                return
            
            text = message.get('text', '').strip()
            if not text:
                return
            
            logger.info(f"Received Telegram command: {text}")
            
            # Process the command
            self._process_command(text)
            
        except Exception as e:
            logger.error(f"Error handling Telegram update: {e}")
    
    def _process_command(self, text: str):
        """Process a bot command"""
        try:
            text = text.lower().strip()
            
            # Help command
            if text in ['help', '/help', '/start']:
                self._send_help_message()
            
            # List watchlist command
            elif text in ['list', '/list', 'watchlist', '/watchlist']:
                self._send_watchlist()
            
            # Add stock command
            elif text.startswith('add ') or text.startswith('/add '):
                ticker = text.replace('add ', '').replace('/add ', '').strip().upper()
                if ticker:
                    self._add_stock_to_watchlist(ticker)
                else:
                    self.send_message("❌ Please specify a ticker symbol. Example: add AAPL")
            
            # Delete/remove stock command  
            elif text.startswith('delete ') or text.startswith('/delete ') or text.startswith('remove ') or text.startswith('/remove '):
                ticker = text.replace('delete ', '').replace('/delete ', '').replace('remove ', '').replace('/remove ', '').strip().upper()
                if ticker:
                    self._remove_stock_from_watchlist(ticker)
                else:
                    self.send_message("❌ Please specify a ticker symbol. Example: delete AAPL")
            
            # Status command
            elif text in ['status', '/status']:
                self._send_status_message()
            
            else:
                self.send_message(f"❓ Unknown command: {text}\n\nType 'help' to see available commands.")
                
        except Exception as e:
            logger.error(f"Error processing command '{text}': {e}")
            self.send_message("❌ Error processing your command. Please try again.")
    
    def _send_help_message(self):
        """Send help message with available commands"""
        help_text = """
🤖 <b>Stock Monitor Bot Commands</b>

📋 <b>Watchlist Management:</b>
• <code>add TICKER</code> - Add stock to watchlist
• <code>delete TICKER</code> - Remove stock from watchlist  
• <code>list</code> - Show current watchlist
• <code>status</code> - Show system status

💡 <b>Examples:</b>
• <code>add AAPL</code> - Add Apple to watchlist
• <code>delete TSLA</code> - Remove Tesla from watchlist
• <code>list</code> - Show all monitored stocks

ℹ️ <b>Notes:</b>
• Commands are case-insensitive
• Stock symbols should be valid Yahoo Finance tickers
• Changes take effect immediately
• You'll receive alerts for all watchlist stocks

🔍 Type any command to get started!
"""
        self.send_message(help_text)
    
    def _send_watchlist(self):
        """Send current watchlist"""
        try:
            if not self.db_manager:
                self.send_message("❌ Database not available")
                return
            
            watchlist = self.db_manager.get_watchlist()
            
            if not watchlist:
                self.send_message("📋 Your watchlist is empty.\n\nUse <code>add TICKER</code> to add stocks!")
                return
            
            message = "📊 <b>Your Stock Watchlist</b>\n\n"
            
            for item in watchlist:
                ticker = item['ticker']
                company_name = item.get('company_name', 'Unknown Company')
                sector = item.get('sector', 'Unknown')
                added_date = item.get('added_at', 'Unknown')
                
                if isinstance(added_date, str):
                    try:
                        from datetime import datetime
                        added_date = datetime.fromisoformat(added_date.replace('Z', '+00:00'))
                        date_str = added_date.strftime('%Y-%m-%d')
                    except:
                        date_str = str(added_date)
                else:
                    date_str = added_date.strftime('%Y-%m-%d') if added_date else 'Unknown'
                
                message += f"🏢 <b>{ticker}</b> - {company_name}\n"
                message += f"   📂 {sector} | 📅 Added: {date_str}\n\n"
            
            message += f"📈 <b>Total:</b> {len(watchlist)} stocks monitored\n"
            message += "\n💡 Use <code>add TICKER</code> or <code>delete TICKER</code> to manage your list"
            
            self.send_message(message)
            
        except Exception as e:
            logger.error(f"Error sending watchlist: {e}")
            self.send_message("❌ Error retrieving watchlist")
    
    def _add_stock_to_watchlist(self, ticker: str):
        """Add stock to watchlist"""
        try:
            if not self.db_manager:
                self.send_message("❌ Database not available")
                return
            
            # Validate ticker format
            if not re.match(r'^[A-Z0-9.-]{1,16}$', ticker):
                self.send_message(f"❌ Invalid ticker format: {ticker}\n\nTicker should contain only letters, numbers, dots, and dashes (max 16 characters)")
                return
            
            # Try to get company info from Yahoo Finance
            try:
                import yfinance as yf
                stock = yf.Ticker(ticker)
                info = stock.info
                company_name = info.get('longName', info.get('shortName', ticker))
                sector = info.get('sector', 'Unknown')
            except:
                company_name = ticker
                sector = 'Unknown'
                logger.warning(f"Could not fetch company info for {ticker}")
            
            # Add to database
            success = self.db_manager.add_company_to_watchlist(
                ticker=ticker,
                company_name=company_name,
                sector=sector
            )
            
            if success:
                message = f"✅ <b>Added to Watchlist</b>\n\n"
                message += f"🏢 <b>{ticker}</b> - {company_name}\n"
                message += f"📂 Sector: {sector}\n"
                message += f"📅 Added: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                message += f"🔔 You'll now receive alerts for {ticker} when price drops below moving averages!"
                
                self.send_message(message)
                logger.info(f"Successfully added {ticker} to watchlist via Telegram")
            else:
                self.send_message(f"❌ Failed to add {ticker} to watchlist. It may already exist or there was a database error.")
                
        except Exception as e:
            logger.error(f"Error adding {ticker} to watchlist: {e}")
            self.send_message(f"❌ Error adding {ticker} to watchlist: {str(e)}")
    
    def _remove_stock_from_watchlist(self, ticker: str):
        """Remove stock from watchlist"""
        try:
            if not self.db_manager:
                self.send_message("❌ Database not available")
                return
            
            # Check if ticker exists first
            watchlist = self.db_manager.get_watchlist()
            ticker_exists = any(item['ticker'].upper() == ticker.upper() for item in watchlist)
            
            if not ticker_exists:
                self.send_message(f"❌ <b>{ticker}</b> is not in your watchlist.\n\nUse <code>list</code> to see current watchlist.")
                return
            
            # Remove from database
            success = self.db_manager.remove_company_from_watchlist(ticker)
            
            if success:
                message = f"🗑️ <b>Removed from Watchlist</b>\n\n"
                message += f"🏢 <b>{ticker}</b> has been removed\n"
                message += f"📅 Removed: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                message += f"🔕 You'll no longer receive alerts for {ticker}"
                
                self.send_message(message)
                logger.info(f"Successfully removed {ticker} from watchlist via Telegram")
            else:
                self.send_message(f"❌ Failed to remove {ticker} from watchlist. Database error occurred.")
                
        except Exception as e:
            logger.error(f"Error removing {ticker} from watchlist: {e}")
            self.send_message(f"❌ Error removing {ticker} from watchlist: {str(e)}")
    
    def _send_status_message(self):
        """Send system status message"""
        try:
            if not self.db_manager:
                self.send_message("❌ Database not available")
                return
            
            watchlist = self.db_manager.get_watchlist()
            
            message = f"📊 <b>Stock Monitor Status</b>\n\n"
            message += f"✅ <b>System:</b> Running\n"
            message += f"📋 <b>Watchlist:</b> {len(watchlist)} stocks\n"
            message += f"🔔 <b>Alerts:</b> Active\n"
            message += f"⏰ <b>Monitoring:</b> Every 5 minutes\n"
            message += f"📈 <b>Averages:</b> 7, 30, 90 trading days\n\n"
            
            if watchlist:
                tickers = [item['ticker'] for item in watchlist]
                message += f"🏢 <b>Monitored Stocks:</b>\n{', '.join(tickers)}\n\n"
            
            message += f"🕐 <b>Status Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
            message += f"💡 Use <code>help</code> to see available commands"
            
            self.send_message(message)
            
        except Exception as e:
            logger.error(f"Error sending status message: {e}")
            self.send_message("❌ Error retrieving system status")
