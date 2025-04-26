import logging
import time
import random
import sys
from pathlib import Path
import yaml
import math
import traceback  # Added for better error reporting
import threading

from backpack_exchange_sdk.authenticated import AuthenticationClient
from backpack_exchange_sdk.public import PublicClient
import colorlog

def pair_worker(pair: dict, cfg: dict):
    short_name = pair["short_account"]["name"]
    long_name  = pair["long_account"]["name"]
    delay_max  = float(cfg.get("pair_start_delay_max", 0))
    cycle_wait = int(cfg.get("cycle_wait_time", 300))

    # начальная задержка перед первым циклом
    if delay_max > 0:
        initial_delay = random.uniform(0, delay_max)
        logging.info(f"Pair {short_name}/{long_name}: initial delay {initial_delay:.2f}s")
        time.sleep(initial_delay)

    # бесконечный цикл работы этой пары
    while True:
        logging.info(f"Pair {short_name}/{long_name}: starting new cycle")
        try:
            success = process_pair(pair, cfg)
            if not success:
                logging.warning(f"Pair {short_name}/{long_name}: cycle finished with errors")
        except Exception as e:
            logging.error(f"Pair {short_name}/{long_name}: exception in cycle: {e}", exc_info=True)

        logging.info(f"Pair {short_name}/{long_name}: sleeping {cycle_wait}s until next cycle")
        time.sleep(cycle_wait)

# -------------------- Logger Setup --------------------
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)
log_filename = f"backpack_liquidation_{time.strftime('%Y%m%d_%H%M%S')}.log"
log_path = logs_dir / log_filename

console_handler = colorlog.StreamHandler()
console_handler.setFormatter(
    colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
        log_colors={'DEBUG':'cyan','INFO':'green','WARNING':'yellow','ERROR':'red','CRITICAL':'red,bg_white'}
    )
)
file_handler = logging.FileHandler(log_path)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = colorlog.getLogger()
logger.addHandler(console_handler)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)

# -------------------- Trader Wrapper --------------------
class BackpackTrader:
    def __init__(self, api_key: str, api_secret: str):
        self.auth = AuthenticationClient(api_key, api_secret)
        self.pub = PublicClient()

    def get_order_book(self, symbol: str) -> dict:
        try:
            return self.pub.get_depth(symbol)
        except Exception as e:
            logging.error(f"Failed to get order book for {symbol}: {e}")
            return {"asks": [], "bids": []}

    def place_limit_order(self, symbol: str, side: str, price: float, quantity: float, reduce_only: bool=False) -> dict:
        try:
            price_str = f"{price:.2f}"
            resp = self.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
            markets = resp.get("data", resp) if isinstance(resp, dict) else resp
            step = next((float(m.get("baseIncrement", 0.01)) for m in markets if m.get("symbol")==symbol), 0.01)
            decimals = len(str(step).split('.')[-1])
            qty_str = f"{quantity:.{decimals}f}"
            logging.info(f"Placing Limit Order: {side} {symbol} @ price={price_str}, qty={qty_str}, reduceOnly={reduce_only}")
            
            return self.auth.execute_order(
                orderType="Limit",
                side=side,
                symbol=symbol,
                price=price_str,
                quantity=qty_str,
                reduceOnly=reduce_only,
                timeInForce="GTC",
                autoBorrow=True,
                autoBorrowRepay=True,
                autoLend=True,
                autoLendRedeem=True,
                selfTradePrevention="RejectTaker"
            )
        except Exception as e:
            logging.error(f"Failed to place limit order: {e}")
            return {"error": str(e)}
            
    def place_market_order(self, symbol: str, side: str, quote_quantity: float) -> dict:
        try:
            qty = round(quote_quantity, 3)
            qty_str = f"{qty:.3f}"
            logging.info(f"Placing Market Order: {side} {symbol} quoteQuantity={qty_str}")
            return self.auth.execute_order(
                orderType="Market",
                side=side,
                symbol=symbol,
                quoteQuantity=qty_str,
                autoBorrow=True,
                autoBorrowRepay=True,
                autoLend=True,
                autoLendRedeem=True,
                selfTradePrevention="RejectTaker"
            )
        except Exception as e:
            logging.error(f"Failed to place market order: {e}")
            return {"error": str(e)}

    def place_take_profit_order(self, symbol: str, side: str, stop_price: float, quantity: float = None) -> dict:
        """Place a limit order with reduceOnly=True to function as a take-profit order."""
        try:
            price_str = f"{stop_price:.2f}"
            logging.info(f"Placing Take Profit Limit Order: {side} {symbol} @ price={price_str}")
            
            # Get decimals for quantity formatting
            resp = self.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
            markets = resp.get("data", resp) if isinstance(resp, dict) else resp
            step = next((float(m.get("baseIncrement", 0.01)) for m in markets if m.get("symbol")==symbol), 0.01)
            decimals = len(str(step).split('.')[-1])
            
            # Format quantity if provided
            qty_str = None
            if quantity is not None:
                qty_str = f"{quantity:.{decimals}f}"
                logging.info(f"Using quantity {qty_str} for take profit order")
            
            # Use place_limit_order with reduceOnly=True
            return self.place_limit_order(
                symbol=symbol,
                side=side,
                price=stop_price,
                quantity=quantity,
                reduce_only=True
            )
        except Exception as e:
            logging.error(f"Failed to place take profit order: {e}")
            return {"error": str(e)}

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        """Fixed method to cancel an order"""
        try:
            logging.info(f"Cancelling order {order_id} for {symbol}")
            return self.auth._send_request(
                "DELETE",
                "api/v1/order",
                "orderCancel",
                {"symbol": symbol, "orderId": order_id}
            )
        except Exception as e:
            logging.error(f"Failed to cancel order: {e}")
            return {"error": str(e)}

    def check_order_status(self, symbol: str, order_id: str) -> str:
        try:
            resp = self.auth._send_request(
                "GET",
                "api/v1/order",
                "orderQuery",
                {"symbol": symbol, "orderId": order_id}
            )
            status = ""
            if isinstance(resp, dict):
                status = resp.get("status") or resp.get("data", {}).get("status", "")
            logging.info(f"Order status for {order_id}: {status}")
            return status
        except Exception as e:
            if "RESOURCE_NOT_FOUND" in str(e):
                # This could mean the order was filled and is no longer active
                # Check position to confirm
                pos = self.get_position(symbol)
                if pos and float(pos.get("netQuantity", 0)) != 0:
                    logging.info(f"Order {order_id} not found but position exists - likely filled")
                    return "FILLED"
            logging.warning(f"Failed to check order status: {e}")
            return "ERROR"

    def get_position(self, symbol: str) -> dict:
        try:
            resp = self.auth._send_request("GET", "api/v1/position", "positionQuery", {})
            positions = resp.get("data", resp) if isinstance(resp, dict) else resp
            
            if not positions or not isinstance(positions, list):
                logging.debug(f"No positions data returned or invalid format: {positions}")
                # Don't return empty dict immediately, retry once
                time.sleep(1)
                try:
                    resp = self.auth._send_request("GET", "api/v1/position", "positionQuery", {})
                    positions = resp.get("data", resp) if isinstance(resp, dict) else resp
                    if not positions or not isinstance(positions, list):
                        logging.debug(f"Still no positions data after retry: {positions}")
                        return {}
                except Exception:
                    return {}
                
            for pos in positions:
                sym = pos.get("symbol", "")
                if sym == symbol or sym.replace("_","-") == symbol or sym.replace("-","_") == symbol:
                    return pos
            
            # No position found for symbol
            # This is normal if no position exists, so use debug level
            logging.debug(f"No position found for {symbol}")
            return {}
        except Exception as e:
            logging.error(f"Failed to get position: {e}")
            return {}

    def get_available_margin(self) -> float:
        max_attempts = 5
        for i in range(1, max_attempts+1):
            try:
                resp = self.auth._send_request("GET", "api/v1/capital/collateral", "collateralQuery", {})
                data = resp.get("data", resp) if isinstance(resp, dict) else resp
                items = data.get("collateral", data) if isinstance(data, dict) else data
                
                if not items or not isinstance(items, list):
                    logging.warning(f"No margin data returned or invalid format: {items}")
                    continue
                    
                for itm in items:
                    if itm.get("symbol") == "USDC":
                        margin = float(itm.get("availableQuantity", 0) or 0)
                        logging.info(f"Available margin: {margin} USDC (attempt {i}/{max_attempts})")
                        return margin
                return 0.0
            except Exception as e:
                logging.warning(f"Margin fetch attempt {i} error: {e}")
                time.sleep(1)
        logging.error("Failed to fetch margin, returning 0")
        return 0.0
# -------------------- Core Logic --------------------
def process_pair(pair_cfg: dict, cfg: dict) -> None:
    try:
        # Configs
        symbol       = cfg["symbol"]
        leverage     = float(cfg.get("leverage", 1))
        maker_off    = float(cfg.get("maker_offset", {}).get("short", 0.0005))
        limit_to     = int(cfg.get("limit_order_timeout", 30))
        retries      = int(cfg.get("limit_order_retries", 10))
        tp_off_long  = float(cfg.get("take_profit_offset", {}).get("long", 0))
        tp_off_short = float(cfg.get("take_profit_offset", {}).get("short", 0))
        check_int    = int(cfg.get("check_interval", 10))
        gmin         = float(cfg.get("general_delay", {}).get("min", 0))
        gmax         = float(cfg.get("general_delay", {}).get("max", 0))

        def log_sleep(desc: str) -> None:
            d = random.uniform(gmin, gmax)
            logging.info(f"Sleeping {desc}: {d:.2f}s")
            time.sleep(d)

        # Parent for deposits/withdrawals
        parent = AuthenticationClient(cfg["api"]["key"], cfg["api"]["secret"])
        initial_dep = float(cfg.get("initial_deposit", 0))
        sweep_tries = int(cfg.get("sweep_attempts", 8))

        # Initialize traders
        sa_cfg = pair_cfg["short_account"]
        la_cfg = pair_cfg["long_account"]
        short_acc_name = sa_cfg.get("name", "ShortAccount")
        long_acc_name = la_cfg.get("name", "LongAccount")
        
        short_tr = BackpackTrader(sa_cfg["api_key"], sa_cfg["api_secret"])
        long_tr  = BackpackTrader(la_cfg["api_key"], la_cfg["api_secret"])
        
        logging.info(f"Processing pair: {short_acc_name} (short) and {long_acc_name} (long) for {symbol}")

        # 1) Deposit initial USDC into both subaccounts
        for side in ("short_account", "long_account"):
            acc = pair_cfg[side]
            acc_name = sa_cfg.get("name", "ShortAccount") if side == "short_account" else la_cfg.get("name", "LongAccount")
            for attempt in range(1, sweep_tries+1):
                try:
                    parent.request_withdrawal(
                        address=acc["address"], blockchain="Solana",
                        quantity=f"{initial_dep:.6f}", symbol="USDC"
                    )
                    logging.info(f"Deposited {initial_dep} USDC to {acc_name}")
                    log_sleep("after deposit")
                    break
                except Exception as e:
                    logging.warning(f"Deposit to {acc_name} attempt {attempt}/{sweep_tries} error: {e}")
                    time.sleep(1)

        # 2) Maker limit order on SHORT with handling of partial fills
        short_position_opened = False
        target_short_size = 0  # Target size of short position
        
        for attempt in range(1, retries+1):
            try:
                # Check if position already exists (partial fill from previous attempts)
                current_pos = short_tr.get_position(symbol)
                current_size = abs(float(current_pos.get("netQuantity", 0))) if current_pos else 0
                
                if current_size > 0:
                    logging.info(f"{short_acc_name}: Current position size: {current_size} (partial fill)")
                    short_position_opened = True
                    
                    # If target size not yet set, this is our first partial fill
                    if target_short_size == 0:
                        # Calculate original target size based on available margin
                        margin = short_tr.get_available_margin()
                        if margin <= 0:
                            logging.warning(f"{short_acc_name}: No margin available after partial fill")
                            break  # Use existing partial position
                            
                        # Get price information
                        ob = short_tr.get_order_book(symbol)
                        if not ob.get("asks") or len(ob.get("asks", [])) == 0:
                            logging.warning(f"{short_acc_name}: No order book data for size calculation")
                            break  # Use existing partial position
                            
                        best_ask = float(ob.get("asks", [[0]])[0][0])
                        price = round(best_ask * (1 + maker_off), 2)
                        
                        # Get step size
                        resp_m = short_tr.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
                        mkts = resp_m.get("data", resp_m) if isinstance(resp_m, dict) else resp_m
                        step = next((float(m.get("baseIncrement",0.01)) for m in mkts if m.get("symbol")==symbol),0.01)
                        
                        # Calculate original intended size
                        notional = (margin + current_pos.get("initialMargin", 0)) * leverage
                        target_short_size = math.floor((notional/price)/step)*step
                        logging.info(f"{short_acc_name}: Target position size: {target_short_size}")
                    
                    # If already reached target or close enough, consider it done
                    if current_size >= target_short_size * 0.9:  # 90% filled is good enough
                        logging.info(f"{short_acc_name}: Position already {current_size}/{target_short_size} " +
                                     f"({current_size/target_short_size*100:.1f}%) filled - continuing")
                        break
                    
                    # Otherwise, need to place another order for the remaining size
                    remaining_size = target_short_size - current_size
                    logging.info(f"{short_acc_name}: Need to fill remaining {remaining_size} units")
                
                # Get order book for current price
                ob = short_tr.get_order_book(symbol)
                if not ob.get("asks") or len(ob.get("asks", [])) == 0:
                    logging.warning(f"{short_acc_name}: No asks in order book for {symbol}, retrying...")
                    time.sleep(2)
                    continue
                    
                best_ask = float(ob.get("asks", [[0]])[0][0])
                if best_ask <= 0:
                    logging.warning(f"{short_acc_name}: Invalid best ask price: {best_ask}, retrying...")
                    time.sleep(2)
                    continue
                    
                price = round(best_ask * (1 + maker_off), 2)
                
                # If this is a follow-up order for remaining size
                if current_size > 0 and target_short_size > 0:
                    qty = remaining_size
                else:
                    # Calculate fresh order size
                    margin = short_tr.get_available_margin()
                    if margin <= 0:
                        logging.warning(f"{short_acc_name}: No margin available, retrying...")
                        time.sleep(2)
                        continue
                        
                    notional = margin * leverage
                    
                    # Get step size
                    resp_m = short_tr.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
                    mkts = resp_m.get("data", resp_m) if isinstance(resp_m, dict) else resp_m
                    step = next((float(m.get("baseIncrement",0.01)) for m in mkts if m.get("symbol")==symbol),0.01)
                    qty = math.floor((notional/price)/step)*step
                    
                    # Store this as target size for potential partial fills
                    target_short_size = qty
                
                if qty <= 0:
                    logging.warning(f"{short_acc_name}: Calculated quantity is invalid: {qty}, retrying...")
                    time.sleep(2)
                    continue
                
                order = short_tr.place_limit_order(symbol, "Ask", price, qty)
                
                if order.get("error"):
                    logging.error(f"{short_acc_name}: Failed to place short order: {order.get('error')}")
                    time.sleep(2)
                    continue
                    
                oid = order.get("orderId") or order.get("data", {}).get("orderId") or order.get("id")
                
                if not oid:
                    logging.error(f"{short_acc_name}: No order ID returned for short order: {order}")
                    time.sleep(2)
                    continue
                    
                logging.info(f"{short_acc_name}: Attempt {attempt}/{retries}: Short maker {oid}@{price}, qty={qty}")
                
                # Wait for order to fill
                st = time.time()
                order_filled = False
                last_check_time = 0
                
                while time.time()-st < limit_to:
                    # Don't check position too frequently
                    current_time = time.time()
                    if current_time - last_check_time >= 2:  # Check every 2 seconds
                        last_check_time = current_time
                        
                        pos = short_tr.get_position(symbol)
                        if pos:
                            new_size = abs(float(pos.get("netQuantity", 0)))
                            
                            # If position exists or size increased
                            if new_size > 0 and (current_size == 0 or new_size > current_size):
                                s = float(pos.get("netQuantity", 0))
                                e = float(pos.get("entryPrice", 0))
                                m = float(pos.get("markPrice", 0))
                                l = pos.get("estLiquidationPrice", "Unknown")
                                pnl = pos.get("unrealizedPnl", "---")
                                logging.info(f"{short_acc_name}: {symbol}, размер={s:.2f} (~{abs(s*e):.2f} USDC), вход={e}, тек.цена={m}, ликв.={l}, PnL={pnl}")
                                
                                # Check if fully filled
                                if abs(new_size - qty) < step or new_size >= target_short_size * 0.9:
                                    logging.info(f"{short_acc_name}: Order fully filled or reached 90% of target")
                                    order_filled = True
                                    short_position_opened = True
                                    break
                                    
                                # Partial fill
                                if new_size > current_size:
                                    logging.info(f"{short_acc_name}: Order partially filled: {new_size}/{qty} units")
                                    # Exit loop and try to place another order
                                    short_position_opened = True
                                    break
                        
                    # Also check order status directly
                    status = short_tr.check_order_status(symbol, oid)
                    if status == "FILLED":
                        order_filled = True
                        short_position_opened = True
                        break
                        
                    time.sleep(1)
                    
                # If timed out or partial fill, cancel remaining order
                if not order_filled:
                    try:
                        short_tr.cancel_order(symbol, oid)
                        logging.info(f"{short_acc_name}: Cancelled unfilled order {oid}")
                    except Exception as e:
                        logging.error(f"{short_acc_name}: Error cancelling order: {e}")
                    
                    # Check one more time if any fills happened during cancellation
                    pos = short_tr.get_position(symbol)
                    if pos:
                        new_size = abs(float(pos.get("netQuantity", 0)))
                        if new_size > 0:
                            short_position_opened = True
                            if new_size >= target_short_size * 0.9:
                                logging.info(f"{short_acc_name}: Position size after cancel: {new_size}/{target_short_size} - sufficient for continuing")
                                break
                            elif attempt == retries:
                                logging.info(f"{short_acc_name}: Final attempt reached with partial fill: {new_size}/{target_short_size} - continuing with partial position")
                                break
                    
                    # If position exists but not fully filled, continue to next attempt
                    if short_position_opened and attempt < retries:
                        logging.info(f"{short_acc_name}: Will try to increase position size in next attempt")
                        continue
                
                # If order fully filled or close enough, break the loop
                if order_filled or (short_position_opened and attempt == retries):
                    break
                    
            except Exception as e:
                logging.error(f"{short_acc_name}: Error in short order placement attempt {attempt}: {e}")
                traceback.print_exc()
                time.sleep(2)
                
        if not short_position_opened:
            logging.error(f"{short_acc_name}: Failed to open short position after all retries")
            return False

        # 3) Immediate market order on LONG - with retry for failures
        long_position_opened = False
        max_long_attempts = 3
        
        for long_attempt in range(1, max_long_attempts + 1):
            try:
                margin_l = long_tr.get_available_margin()
                if margin_l <= 0:
                    logging.error(f"{long_acc_name}: No margin available. Trying again in 5 seconds (attempt {long_attempt}/{max_long_attempts})")
                    time.sleep(5)
                    continue
                    
                notional_l = margin_l * leverage
                long_order = long_tr.place_market_order(symbol, "Bid", notional_l)
                
                if long_order.get("error"):
                    logging.error(f"{long_acc_name}: Failed to place long order: {long_order.get('error')}. Retrying...")
                    time.sleep(5)
                    continue
                    
                # Wait a moment for the order to process
                time.sleep(3)
                
                # Verify position was opened
                for check in range(5):  # Multiple checks with increasing delays
                    pos_l = long_tr.get_position(symbol)
                    if pos_l and float(pos_l.get("netQuantity", 0)) != 0:
                        s_l   = float(pos_l.get("netQuantity",0))
                        e_l   = float(pos_l.get("entryPrice",0))
                        m_l   = float(pos_l.get("markPrice",0))
                        l_l   = pos_l.get("estLiquidationPrice","Unknown")
                        pnl_l = pos_l.get("unrealizedPnl","---")
                        logging.info(f"{long_acc_name}: {symbol}, размер={s_l:.2f} (~{abs(s_l*e_l):.2f} USDC), вход={e_l}, тек.цена={m_l}, ликв.={l_l}, PnL={pnl_l}")
                        long_position_opened = True
                        break
                    else:
                        logging.warning(f"{long_acc_name}: Position not found after market order, checking again in {check+1} seconds...")
                        time.sleep(check + 1)
                
                if long_position_opened:
                    break
                else:
                    logging.error(f"{long_acc_name}: Position not found after multiple checks. Retrying market order.")
            except Exception as e:
                logging.error(f"{long_acc_name}: Error placing long market order (attempt {long_attempt}): {e}")
                traceback.print_exc()
                time.sleep(5)
        
        if not long_position_opened:
            logging.error(f"{long_acc_name}: Failed to open long position after all attempts")
            # At this point we have a short position but no long position
            # Try to close the short position to avoid unbalanced risk
            try:
                short_pos = short_tr.get_position(symbol)
                if short_pos and float(short_pos.get("netQuantity", 0)) != 0:
                    logging.warning(f"{short_acc_name}: Closing short position because long position could not be opened")
                    short_qty = abs(float(short_pos.get("netQuantity", 0)))
                    # Market order to close the short
                    close_order = short_tr.place_market_order(symbol, "Bid", short_qty * float(short_pos.get("markPrice", 0)))
                    logging.info(f"{short_acc_name}: Closed short position: {close_order}")
            except Exception as e:
                logging.error(f"Error closing short position: {e}")
                
            return False

        # 4) Place take-profit orders using limit orders with opposite liquidation price
        try:
            log_sleep("before take profit")
            
            # Get updated position info for both positions
            short_pos = short_tr.get_position(symbol)
            long_pos = long_tr.get_position(symbol)
            
            short_acc_name = sa_cfg.get("name", "ShortAccount")
            long_acc_name = la_cfg.get("name", "LongAccount")
            
            if not short_pos or float(short_pos.get("netQuantity", 0)) == 0:
                logging.error(f"{short_acc_name}: Short position not found when trying to set take profit")
            
            if not long_pos or float(long_pos.get("netQuantity", 0)) == 0:
                logging.error(f"{long_acc_name}: Long position not found when trying to set take profit")
            
            # Only proceed if both positions exist
            if (short_pos and float(short_pos.get("netQuantity", 0)) != 0 and 
                long_pos and float(long_pos.get("netQuantity", 0)) != 0):
                
                # Extract liquidation prices
                liq_short = float(short_pos.get("estLiquidationPrice", 0))
                liq_long = float(long_pos.get("estLiquidationPrice", 0))
                
                if liq_short > 0 and liq_long > 0:
                    logging.info(f"{short_acc_name}: Short liquidation price = {liq_short}")
                    logging.info(f"{long_acc_name}: Long liquidation price = {liq_long}")
                    
                    # Calculate take profit prices based on opposite liquidation + offset
                    # For long position, use short liquidation + offset
                    tp_long_price = round(liq_short + tp_off_long, 2)
                    
                    # For short position, use long liquidation + offset
                    tp_short_price = round(liq_long + tp_off_short, 2)
                    
                    logging.info(f"{long_acc_name}: Setting long take profit at {tp_long_price} " +
                                 f"(short liq {liq_short} + offset {tp_off_long})")
                    
                    logging.info(f"{short_acc_name}: Setting short take profit at {tp_short_price} " +
                                 f"(long liq {liq_long} + offset {tp_off_short})")
                    
                    # Get quantities
                    short_qty = abs(float(short_pos.get("netQuantity", 0)))
                    long_qty = abs(float(long_pos.get("netQuantity", 0)))
                    
                    # Place limit orders with reduceOnly=True
                    # For short position - "Bid" side to close (buying back)
                    short_tp = short_tr.place_limit_order(
                        symbol=symbol,
                        side="Bid",  # Buy to close short
                        price=tp_short_price,
                        quantity=short_qty,
                        reduce_only=True
                    )
                    
                    short_order_id = short_tp.get("orderId") or short_tp.get("data", {}).get("orderId") or short_tp.get("id")
                    logging.info(f"{short_acc_name}: Short take profit limit order placed at {tp_short_price}, ID: {short_order_id}")
                    
                    # For long position - "Ask" side to close (selling)
                    long_tp = long_tr.place_limit_order(
                        symbol=symbol,
                        side="Ask",  # Sell to close long
                        price=tp_long_price,
                        quantity=long_qty,
                        reduce_only=True
                    )
                    
                    long_order_id = long_tp.get("orderId") or long_tp.get("data", {}).get("orderId") or long_tp.get("id")
                    logging.info(f"{long_acc_name}: Long take profit limit order placed at {tp_long_price}, ID: {long_order_id}")
                else:
                    logging.error("Invalid liquidation prices, cannot set take profits based on opposite positions")
                    # Fallback to standard take profits
                    
                    # For short position
                    short_qty = abs(float(short_pos.get("netQuantity", 0)))
                    entry_s = float(short_pos.get("entryPrice", 0))
                    tp_s = round(entry_s * 0.98, 2)  # 2% below entry
                    
                    short_tp = short_tr.place_limit_order(
                        symbol=symbol,
                        side="Bid",
                        price=tp_s,
                        quantity=short_qty,
                        reduce_only=True
                    )
                    
                    short_order_id = short_tp.get("orderId") or short_tp.get("data", {}).get("orderId") or short_tp.get("id")
                    logging.info(f"{short_acc_name}: Fallback short take profit limit order placed at {tp_s}, ID: {short_order_id}")
                    
                    # For long position
                    long_qty = abs(float(long_pos.get("netQuantity", 0)))
                    entry_l = float(long_pos.get("entryPrice", 0))
                    tp_l = round(entry_l * 1.02, 2)  # 2% above entry
                    
                    long_tp = long_tr.place_limit_order(
                        symbol=symbol,
                        side="Ask",
                        price=tp_l,
                        quantity=long_qty,
                        reduce_only=True
                    )
                    
                    long_order_id = long_tp.get("orderId") or long_tp.get("data", {}).get("orderId") or long_tp.get("id")
                    logging.info(f"{long_acc_name}: Fallback long take profit limit order placed at {tp_l}, ID: {long_order_id}")
            else:
                logging.error("Cannot set take profits based on opposite positions - one or both positions missing")
                
        except Exception as e:
            logging.error(f"Error setting take profit orders: {e}")
            traceback.print_exc()

        # 5) Monitor until both positions closed
        max_monitor_time = 3600 * 24  # 24 hours maximum monitoring time
        start_monitoring = time.time()
        
        short_acc_name = sa_cfg.get("name", "ShortAccount")
        long_acc_name = la_cfg.get("name", "LongAccount")
        
        try:
            while time.time() - start_monitoring < max_monitor_time:
                short_pos = short_tr.get_position(symbol)
                long_pos = long_tr.get_position(symbol)
                
                # Check if both positions exist
                short_active = short_pos and float(short_pos.get("netQuantity", 0)) != 0
                long_active = long_pos and float(long_pos.get("netQuantity", 0)) != 0
                
                # Log position details if active
                if short_active:
                    s = float(short_pos.get("netQuantity", 0))
                    e = float(short_pos.get("entryPrice", 0))
                    m = float(short_pos.get("markPrice", 0))
                    l = short_pos.get("estLiquidationPrice", "Unknown")
                    pnl = short_pos.get("unrealizedPnl", "---")
                    logging.info(f"{short_acc_name}: Short position: size={s:.2f}, entry={e}, mark={m}, liquidation={l}, PnL={pnl}")
                else:
                    logging.info(f"{short_acc_name}: No active short position found")
                
                if long_active:
                    s = float(long_pos.get("netQuantity", 0))
                    e = float(long_pos.get("entryPrice", 0))
                    m = float(long_pos.get("markPrice", 0))
                    l = long_pos.get("estLiquidationPrice", "Unknown")
                    pnl = long_pos.get("unrealizedPnl", "---")
                    logging.info(f"{long_acc_name}: Long position: size={s:.2f}, entry={e}, mark={m}, liquidation={l}, PnL={pnl}")
                else:
                    logging.info(f"{long_acc_name}: No active long position found")
                
                # Check if both positions are closed - use the explicit check with short_active and long_active flags
                if not short_active and not long_active:
                    # Double-check with a small delay to make sure it's not just API flakiness
                    time.sleep(3)
                    
                    # Re-check positions
                    short_pos = short_tr.get_position(symbol)
                    long_pos = long_tr.get_position(symbol)
                    
                    short_active = short_pos and float(short_pos.get("netQuantity", 0)) != 0
                    long_active = long_pos and float(long_pos.get("netQuantity", 0)) != 0
                    
                    if not short_active and not long_active:
                        logging.info("Both positions confirmed closed. Moving to sweep phase.")
                        break
                
                # If we've been monitoring for more than 1 hour, implement safety checks
                if time.time() - start_monitoring > 3600:
                    # If we haven't seen any positions in a long time, something might be wrong
                    if not short_active and not long_active:
                        logging.warning("No positions found after 1 hour of monitoring. Moving to sweep phase.")
                        break
                
                time.sleep(check_int)
        except Exception as e:
            logging.error(f"Error during position monitoring: {e}")
            traceback.print_exc()

        # 6) Sweep ALL remaining funds FROM sub-accounts TO parent account
        # …после мониторинга и подтверждения закрытия позиций…
        logging.info("Both positions confirmed closed. Moving to sweep phase.")
        # пауза перед свипом
        log_sleep("before sweep funds")

        all_funds_swept = True
        
        # Объявляем две функции для получения информации о балансе и вывода средств
        def get_subaccount_balance(trader, acc_name):
            """Берём доступную маржу как сумму для свипа"""
            try:
                bal = trader.get_available_margin()
                logging.info(f"{acc_name}: Available USDC margin = {bal}")
                return bal
            except Exception as e:
                logging.error(f"{acc_name}: Failed to get balance: {e}")
                return 0.0

        def withdraw_from_subaccount(trader, acc_name, amount, main_address):
            """Вывести USDC из суб-аккаунта на основной счет, вернуть результат запроса"""
            try:
                qty_str = f"{amount:.6f}"
                logging.info(f"{acc_name}: Withdrawing {qty_str} USDC -> {main_address}")
                result = trader.auth.request_withdrawal(
                    address=main_address,
                    blockchain="Solana",
                    quantity=qty_str,
                    symbol="USDC"
                )
                return result  # raw dict
            except Exception as e:
                logging.error(f"{acc_name}: Withdrawal failed: {e}")
                return None

                
        # Сам свип
        for side in ("short_account", "long_account"):
            tr = short_tr if side == "short_account" else long_tr
            acc_name = sa_cfg["name"] if side=="short_account" else la_cfg["name"]

            bal = get_subaccount_balance(tr, acc_name)
            if bal > 0.1:
                # Без задержки, сразу выводим
                res = withdraw_from_subaccount(tr, acc_name, bal, cfg["main_account"]["address"])
                if res:
                    logging.info(
                        f"{acc_name}: Withdrawal submitted — "
                        f"ID={res.get('id')} | "
                        f"Amount={res.get('quantity')} USDC | "
                        f"To={res.get('toAddress')} | "
                        f"Status={res.get('status')}"
                    )
                else:
                    logging.warning(f"{acc_name}: Withdrawal attempt failed")
            else:
                logging.info(f"{acc_name}: No significant funds to withdraw (balance: {bal} USDC)")
        
        # Return the status of fund sweeping
        return all_funds_swept
    except Exception as e:
        logging.error(f"Error in process_pair: {str(e)}")
        traceback.print_exc()
        return False  # Indicate error

# -------------------- Main --------------------
# -------------------- Main --------------------
def main() -> None:
    try:
        cfg_path = Path("config.yaml")
        if not cfg_path.exists():
            logging.error(f"Config not found: {cfg_path}")
            sys.exit(1)

        cfg = yaml.safe_load(cfg_path.read_text())
        # Валидация конфига
        if not cfg or "pairs" not in cfg or not cfg["pairs"]:
            logging.error("Config is empty or no pairs configured")
            sys.exit(1)
        if not cfg.get("main_account", {}).get("address"):
            logging.error("Main account address missing in config")
            sys.exit(1)

        threads = []

        # Для каждой пары создаём и запускаем воркер-поток
        for pair in cfg["pairs"]:
            short_name = pair["short_account"]["name"]
            long_name  = pair["long_account"]["name"]
            thread_name = f"{short_name}/{long_name}"
            t = threading.Thread(
                target=pair_worker,
                args=(pair, cfg),
                name=thread_name,
                daemon=True
            )
            t.start()
            threads.append(t)
            logging.info(f"Started worker thread for pair {thread_name}")

        # Главный поток просто ждёт, пока живут все воркеры
        for t in threads:
            t.join()

    except Exception as e:
        logging.error(f"Fatal error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
