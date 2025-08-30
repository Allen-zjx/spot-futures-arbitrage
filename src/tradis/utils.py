from datetime import datetime
import pandas as pd
from tardis_dev import datasets, get_exchange_details


def extract_available_symbols(exchange: str):
    """
    提取交易所的所有可用交易对
    """
    details = get_exchange_details(exchange)
    # 提取所有USDT永续合约交易对
    usdt_symbols = []
    for symbol_info in details["availableSymbols"]:
        if symbol_info["id"].endswith("USDT"):
            if "availableSince" in symbol_info:
                start_time = symbol_info["availableSince"]
                start_time = pd.to_datetime(start_time)
                start_date = start_time.strftime("%Y-%m-%d")
            else:
                raise ValueError(
                    f"symbol {symbol_info['symbol']} has no availableSince"
                )
            if "availableTo" in symbol_info:
                end_time = symbol_info["availableTo"]
                end_time = pd.to_datetime(end_time)
                end_date = end_time.strftime("%Y-%m-%d")
            else:
                end_date = datetime.now().strftime("%Y-%m-%d")
            usdt_symbols.append(
                {
                    "symbol": symbol_info["id"],
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )
    return usdt_symbols
