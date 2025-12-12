# main.py
import os
from dotenv import load_dotenv
from Upstox.upstox import upstox
from Upstox.base.constants import ExchangeCode

# 1. Load variables
load_dotenv()

params = {
    "api_key": os.getenv("UPSTOX_API_KEY"),
    "api_secret": os.getenv("UPSTOX_API_SECRET"),
    "redirect_uri": os.getenv("UPSTOX_REDIRECT_URI"),
    "totpstr": os.getenv("UPSTOX_TOTP_SECRET"),
    "mobile_no": os.getenv("UPSTOX_MOBILE_NO"),
    "pin": os.getenv("UPSTOX_PIN"),
}

def main():
    print("--- Starting Login Process ---")
    try:
        # 2. Authenticate (This will open Chrome briefly)
        # Note: Your code is designed to use Selenium to auto-login
        headers = upstox.create_headers(params)
        print("Login Successful!")
        print(f"Auth Headers: {headers}")

        # 3. Fetch Profile to verify
        print("\n--- Fetching Profile ---")
        profile = upstox.profile(headers)
        print(profile)

        # 4. Fetch Funds/Positions
        print("\n--- Fetching Funds/Positions ---")
        positions = upstox.fetch_positions(headers)
        print(positions)


        print("\n--- Placing Order ---")
        order_resp = upstox.market_order_eq(
            exchange=ExchangeCode.NSE,
            symbol="RELIANCE", 
            quantity=1,
            side="BUY",
            unique_id="test_trade",
            headers=headers
        )
        print(order_resp)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()