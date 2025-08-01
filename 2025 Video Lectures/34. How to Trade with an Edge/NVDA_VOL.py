import time
import threading
import pandas as pd
from datetime import datetime, timedelta
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import queue

class IBHistoricalData(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        EWrapper.__init__(self)
        
        # Data storage
        self.historical_data = {}
        self.option_data = {}
        self.contract_details = {}
        self.completed_requests = set()
        
        # Request tracking
        self.request_id_counter = 1000
        self.data_ready = threading.Event()
        
    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        print(f"Error {errorCode}: {errorString}")
        if errorCode in [200, 162]:  # No data available
            self.completed_requests.add(reqId)
        
    def nextValidId(self, orderId):
        print(f"Connected to IB API")
        
    def historicalData(self, reqId, bar):
        """Receive historical data bars"""
        if reqId not in self.historical_data:
            self.historical_data[reqId] = []
            
        data_point = {
            'date': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume
        }
        self.historical_data[reqId].append(data_point)
        
    def historicalDataEnd(self, reqId, start, end):
        """Historical data request completed"""
        print(f"Historical data completed for request {reqId}")
        self.completed_requests.add(reqId)
        
    def contractDetails(self, reqId, contractDetails):
        """Receive contract details"""
        contract = contractDetails.contract
        if reqId not in self.contract_details:
            self.contract_details[reqId] = []
            
        self.contract_details[reqId].append({
            'contract': contract,
            'strike': contract.strike,
            'expiry': contract.lastTradeDateOrContractMonth,
            'right': contract.right
        })
        
    def contractDetailsEnd(self, reqId):
        """Contract details request completed"""
        print(f"Contract details completed for request {reqId}")
        self.completed_requests.add(reqId)
        
    def create_stock_contract(self, symbol):
        """Create a stock contract"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract
        
    def create_option_contract(self, symbol, expiry, strike, right="P"):
        """Create an option contract"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = expiry
        contract.strike = strike
        contract.right = right
        contract.multiplier = "100"
        contract.tradingClass = symbol  # Add trading class
        return contract
        
    def get_ytd_start_date(self):
        """Get start of current year"""
        return datetime(datetime.now().year, 1, 1)
        
    def get_trading_dates(self, start_date, end_date):
        """Generate list of trading dates (weekdays)"""
        dates = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:  # Monday=0, Friday=4
                dates.append(current)
            current += timedelta(days=1)
        return dates
        
    def find_nearest_expiry(self, target_date, weeks_out=3):
        """Find the nearest Friday expiry that's weeks_out from target_date"""
        # Start from target_date and find next Friday
        days_ahead = 4 - target_date.weekday()  # Friday is weekday 4
        if days_ahead <= 0:
            days_ahead += 7
            
        # Add additional weeks
        days_ahead += (weeks_out - 1) * 7
        
        expiry = target_date + timedelta(days=days_ahead)
        return expiry.strftime("%Y%m%d")
        
    def get_available_option_contracts(self, symbol, expiry_date):
        """Query available option contracts for a given expiry"""
        option_contract = Contract()
        option_contract.symbol = symbol
        option_contract.secType = "OPT" 
        option_contract.exchange = "SMART"
        option_contract.currency = "USD"
        option_contract.lastTradeDateOrContractMonth = expiry_date
        
        req_id = self.request_id_counter
        self.request_id_counter += 1
        
        print(f"Querying available options for {symbol} expiry {expiry_date}")
        self.reqContractDetails(req_id, option_contract)
        return req_id
        
    def find_best_otm_put_from_available(self, available_contracts, target_strike):
        """Find the best OTM put from available contracts"""
        puts = [c for c in available_contracts if c['right'] == 'P']
        
        if not puts:
            return None
            
        # Find the put closest to our target strike
        best_put = min(puts, key=lambda x: abs(x['strike'] - target_strike))
        return best_put
        
    def calculate_otm_strike(self, stock_price, otm_percentage=0.90):
        """Calculate OTM put strike (typically 90% of stock price)"""
        strike = stock_price * otm_percentage
        # Round to nearest $5 for NVDA
        return round(strike / 5) * 5
        
    def fetch_stock_historical_data(self):
        """Fetch YTD historical data for NVDA"""
        nvda_contract = self.create_stock_contract("NVDA")
        
        start_date = self.get_ytd_start_date()
        end_date = datetime.now()
        
        # Calculate duration
        duration_days = (end_date - start_date).days
        duration_str = f"{duration_days} D"
        
        req_id = self.request_id_counter
        self.request_id_counter += 1
        
        print(f"Requesting NVDA historical data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        self.reqHistoricalData(
            req_id,
            nvda_contract,
            "",  # end date (empty = most recent)
            duration_str,
            "1 day",  # bar size
            "TRADES",  # what to show
            1,  # use RTH (regular trading hours)
            1,  # date format (1 = yyyyMMdd HH:mm:ss)
            False,  # keep up to date
            []  # chart options
        )
        
        return req_id
        
    def fetch_option_historical_data(self, symbol, expiry, strike, start_date, end_date):
        """Fetch historical implied volatility for specific option"""
        option_contract = self.create_option_contract(symbol, expiry, strike, "P")
        
        duration_days = min((end_date - start_date).days, 30)  # Limit to 30 days per request
        duration_str = f"{duration_days} D"
        
        req_id = self.request_id_counter
        self.request_id_counter += 1
        
        print(f"Requesting option data - Strike: {strike}, Expiry: {expiry}")
        
        self.reqHistoricalData(
            req_id,
            option_contract,
            end_date.strftime("%Y%m%d %H:%M:%S"),
            duration_str,
            "1 day",
            "OPTION_IMPLIED_VOLATILITY",  # Get implied volatility
            1,  # use RTH
            1,  # date format
            False,  # keep up to date
            []  # chart options
        )
        
        return req_id
        
    def wait_for_request(self, req_id, timeout=30):
        """Wait for a specific request to complete"""
        start_time = time.time()
        while req_id not in self.completed_requests:
            time.sleep(0.1)
            if time.time() - start_time > timeout:
                print(f"Timeout waiting for request {req_id}")
                return False
        return True
        
    def collect_ytd_data(self):
        """Main function to collect YTD NVDA and option data"""
        # Step 1: Get NVDA historical stock data
        print("Fetching NVDA historical stock data...")
        stock_req_id = self.fetch_stock_historical_data()
        
        if not self.wait_for_request(stock_req_id, 60):
            print("Failed to get stock data")
            return
            
        if stock_req_id not in self.historical_data:
            print("No stock data received")
            return
            
        stock_data = self.historical_data[stock_req_id]
        print(f"Received {len(stock_data)} days of NVDA data")
        
        # Step 2: For each stock price, get corresponding option data
        results = []
        
        for i, stock_bar in enumerate(stock_data):
            try:
                date_str = stock_bar['date']
                stock_price = stock_bar['close']
                
                # Parse date
                if len(date_str) == 8:  # YYYYMMDD format
                    trade_date = datetime.strptime(date_str, "%Y%m%d")
                else:  # YYYYMMDD HH:MM:SS format
                    trade_date = datetime.strptime(date_str.split()[0], "%Y%m%d")
                
                # Calculate target OTM strike
                target_otm_strike = self.calculate_otm_strike(stock_price)
                 
                 # Find appropriate expiry (3 weeks out from trade date)
                 expiry = self.find_nearest_expiry(trade_date, weeks_out=3)
                 
                 print(f"Processing {trade_date.strftime('%Y-%m-%d')}: NVDA=${stock_price:.2f}, Target Strike=${target_otm_strike}")
                 
                 # First, get available option contracts for this expiry
                 contracts_req_id = self.get_available_option_contracts("NVDA", expiry)
                 
                 if self.wait_for_request(contracts_req_id, 30):
                     if contracts_req_id in self.contract_details and self.contract_details[contracts_req_id]:
                         available_contracts = self.contract_details[contracts_req_id]
                         
                         # Find the best available put close to our target
                         best_put = self.find_best_otm_put_from_available(available_contracts, target_otm_strike)
                         
                         if best_put:
                             actual_strike = best_put['strike']
                             print(f"  → Found put option: Strike ${actual_strike}")
                             
                             # Get option data for this specific contract
                             option_req_id = self.fetch_option_historical_data(
                                 "NVDA", expiry, actual_strike, trade_date, trade_date + timedelta(days=1)
                             )
                             
                             if self.wait_for_request(option_req_id, 30):
                                 if option_req_id in self.historical_data and self.historical_data[option_req_id]:
                                     option_bar = self.historical_data[option_req_id][0]
                                     implied_vol = option_bar.get('close', 0)
                                     
                                     result = {
                                         'Date': trade_date.strftime('%Y-%m-%d'),
                                         'NVDA_Price': stock_price,
                                         'OTM_Put_Strike': actual_strike,
                                         'Option_Expiry': expiry,
                                         'Implied_Vol': implied_vol
                                     }
                                     results.append(result)
                                     print(f"  → IV: {implied_vol:.4f}")
                                 else:
                                     print(f"  → No historical IV data available")
                                     result = {
                                         'Date': trade_date.strftime('%Y-%m-%d'),
                                         'NVDA_Price': stock_price,
                                         'OTM_Put_Strike': actual_strike,
                                         'Option_Expiry': expiry,
                                         'Implied_Vol': None
                                     }
                                     results.append(result)
                             else:
                                 print(f"  → Historical data request timeout")
                         else:
                             print(f"  → No suitable put options found for expiry {expiry}")
                             # Record with no option data
                             result = {
                                 'Date': trade_date.strftime('%Y-%m-%d'),
                                 'NVDA_Price': stock_price,
                                 'OTM_Put_Strike': target_otm_strike,
                                 'Option_Expiry': expiry,
                                 'Implied_Vol': None
                             }
                             results.append(result)
                     else:
                         print(f"  → No option contracts found for expiry {expiry}")
                         result = {
                             'Date': trade_date.strftime('%Y-%m-%d'),
                             'NVDA_Price': stock_price,
                             'OTM_Put_Strike': target_otm_strike,
                             'Option_Expiry': expiry,
                             'Implied_Vol': None
                         }
                         results.append(result)
                 else:
                     print(f"  → Contract details request timeout")
                    
                # Add small delay to avoid rate limiting
                time.sleep(0.5)
                
                # Process in batches to avoid overwhelming the API
                if i > 0 and i % 20 == 0:
                    print(f"Processed {i+1}/{len(stock_data)} days. Saving intermediate results...")
                    self.save_results(results, f"nvda_vol_ytd_partial_{i}.csv")
                    
            except Exception as e:
                print(f"Error processing data point: {e}")
                continue
                
        return results
        
    def save_results(self, results, filename="nvda_vol_ytd_data.csv"):
        """Save results to CSV"""
        if results:
            df = pd.DataFrame(results)
            df.to_csv(filename, index=False)
            print(f"Saved {len(results)} records to {filename}")
        else:
            print("No results to save")

def main():
    print("NVDA YTD Historical Volatility Data Collector")
    print("=" * 50)
    
    # Create IB API instance
    app = IBHistoricalData()
    
    # Connect to IB
    print("Connecting to Interactive Brokers...")
    app.connect("127.0.0.1", 7497, clientId=1)  # 7497 for TWS, 7496 for Gateway
    
    # Start the socket in a thread
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()
    
    # Wait for connection
    time.sleep(3)
    
    try:
        # Collect YTD data
        results = app.collect_ytd_data()
        
        # Save final results
        if results:
            app.save_results(results, "nvda_vol_ytd_data.csv")
            print(f"\nData collection completed! {len(results)} records saved.")
            
            # Show summary
            df = pd.DataFrame(results)
            print(f"\nData Summary:")
            print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
            print(f"NVDA price range: ${df['NVDA_Price'].min():.2f} - ${df['NVDA_Price'].max():.2f}")
            if 'Implied_Vol' in df.columns:
                valid_iv = df['Implied_Vol'].dropna()
                if len(valid_iv) > 0:
                    print(f"IV range: {valid_iv.min():.4f} - {valid_iv.max():.4f}")
                    print(f"Records with IV data: {len(valid_iv)}/{len(df)}")
        else:
            print("No data collected")
            
    except KeyboardInterrupt:
        print("\nStopping data collection...")
        
    finally:
        app.disconnect()
        print("Disconnected from IB API")

if __name__ == "__main__":
    main()
