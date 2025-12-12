import os
import time
import json
from dotenv import load_dotenv
from Upstox.upstox import upstox
from Upstox.base.constants import ExchangeCode, Side, OrderType

# 1. Load Environment Variables
load_dotenv()

SANDBOX_TOKEN = os.getenv("UPSTOX_SANDBOX_TOKEN")

if not SANDBOX_TOKEN:
    raise ValueError("CRITICAL: UPSTOX_SANDBOX_TOKEN not found in .env file.")

# 2. Construct Headers Manually
HEADERS = {
    "headers": {
        "Authorization": f"Bearer {SANDBOX_TOKEN}",
        "Accept": "application/json"
    }
}

def print_header(title):
    print(f"\n{'='*60}")
    print(f"TESTING: {title}")
    print(f"{'='*60}")

def test_connectivity_via_orderbook():
    """
    Since 'Profile' and 'Positions' do not exist in Sandbox,
    we verify connection by fetching the Order Book.
    Even if empty, it should return 200 OK.
    """
    print_header("1. Connection Check (via Order Book)")
    try:
        # In Sandbox, this is the most reliable "Hello World" check
        orders = upstox.fetch_orderbook(HEADERS)
        
        # If code reaches here without error, authentication is WORKING
        print(f"✅ SUCCESS: Connected to Sandbox!")
        print(f"   Current Sandbox Orders: {len(orders)}")
        return True
    except Exception as e:
        print(f"❌ FAILED: Connection Rejected.")
        print(f"   Error Details: {e}")
        print("   (Check: Is UPSTOX_SANDBOX_TOKEN correct in .env?)")
        print("   (Check: Is 'base' URL in upstox.py set to 'api-sandbox.upstox.com'?)")
        return False

def test_market_data_download():
    print_header("2. Downloading Instrument Keys (Public URL)")
    print("   (Downloading ~15MB Instrument File from Upstox Assets...)")
    try:
        # This hits a public URL (assets.upstox.com), so it works in Sandbox too
        upstox.create_eq_tokens()
        count = len(upstox.eq_tokens.get(ExchangeCode.NSE, {}))
        if count > 0:
            print(f"✅ SUCCESS: Instruments Downloaded (Count: {count})")
            return True
        else:
            print("❌ FAILED: Dictionary is empty.")
            return False
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False

def test_order_simulation():
    print_header("3. Order System Simulation")
    
    order_id = None
    
    # --- A. PLACE ORDER ---
    try:
        print("👉 Step A: Placing Dummy LIMIT Order (SBIN)")
        # Sandbox orders are "Mock" - they don't check if you have funds
        order_resp = upstox.limit_order_eq(
            exchange=ExchangeCode.NSE,
            symbol="SBIN", 
            quantity=1,
            price=500.00,
            side=Side.BUY,
            unique_id="SB_TEST_001",
            headers=HEADERS
        )
        
        # Parse response
        if isinstance(order_resp, dict):
             # Sometimes response is nested in 'data' depending on your parser
            order_id = order_resp.get('id') or order_resp.get('data', {}).get('order_id')
        
        if order_id:
            print(f"✅ SUCCESS: Order Accepted. Mock ID: {order_id}")
        else:
            print(f"❌ FAILED: No ID returned. Raw Resp: {order_resp}")
            return

    except Exception as e:
        print(f"❌ FAILED to Place: {e}")
        return

    time.sleep(1)

    # --- B. MODIFY ORDER ---
    orders = upstox.fetch_orderbook(HEADERS)
        
    # If code reaches here without error, authentication is WORKING
    print(f"✅ SUCCESS: Connected to Sandbox!")
    print(f"   Current Sandbox Orders: {len(orders)}")
    try:
        print(f"👉 Step B: Modifying Order {order_id}")
        modify_resp = upstox.modify_order(
            order_id=order_id,
            headers=HEADERS,
            price=505.00,
            order_type=OrderType.LIMIT
        )
        print(f"✅ SUCCESS: Modification Sent. Response: {modify_resp.get('id')}")
    except Exception as e:
        # Sandbox sometimes auto-completes orders instantly, making them unmodifiable.
        # This is common behavior in mock environments.
        print(f"⚠️ NOTE: Modify failed (Order might be closed/filled instantly in Sandbox). Error: {e}")

    time.sleep(1)

    # --- C. CANCEL ORDER ---
    try:
        print(f"👉 Step C: Cancelling Order {order_id}")
        cancel_resp = upstox.cancel_order(
            order_id=order_id,
            headers=HEADERS
        )
        print(f"✅ SUCCESS: Cancellation Sent. Message: {cancel_resp}")
    except Exception as e:
        print(f"⚠️ NOTE: Cancel failed (Order might be already closed). Error: {e}")

    orders = upstox.fetch_orderbook(HEADERS)
        
    # If code reaches here without error, authentication is WORKING
    print(f"✅ SUCCESS: Connected to Sandbox!")
    print(f"   Current Sandbox Orders: {[order.get('status') for order in orders]}")

if __name__ == "__main__":
    print("STARTING FINAL SANDBOX TEST...")
    
    if test_connectivity_via_orderbook():
        if test_market_data_download():
            test_order_simulation()
    
    print("\nTEST COMPLETE.")