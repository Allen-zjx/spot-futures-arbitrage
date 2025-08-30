from datetime import datetime
from tardis_dev import datasets, get_exchange_details
import pandas as pd
from dotenv import load_dotenv
import os
from tqdm import tqdm
import json

load_dotenv()

TARDIS_KEY = os.getenv("TARDIS_KEY")

black_symbols = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "BNBUSDT",
]


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
            
            if pd.to_datetime(end_date) > (pd.to_datetime() - pd.Timedelta(days=1)) and symbol_info["id"] not in black_symbols:
                usdt_symbols.append(
                    {
                        "symbol": symbol_info["id"],
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )
    return usdt_symbols

def load_success_symbols():
    if not os.path.exists("success.json"):
        return set()
    symbols = set()
    with open("success.json", "r") as f:
        for line in f:
            if line.strip():
                try:
                    data = json.loads(line)
                    symbols.add(data["symbol"])
                except:
                    continue
    return symbols
    

def load_error_symbols():
    if not os.path.exists("error.json"):
        return set()
    symbols = set()
    with open("error.json", "r") as f:
        for line in f:
            if line.strip():
                try:
                    data = json.loads(line)
                    symbols.add(data["symbol"])
                except:
                    continue
    return symbols
    
if __name__ == "__main__":
    # 提取所有的USDT合约
    exchange = "binance"
    download_date_type = ["trades"]
    usdt_symbols = extract_available_symbols(exchange)
    print(len(usdt_symbols))

    # # 加载已完成的交易对
    # success_symbols = load_success_symbols()
    
    # start_date = "2025-08-24"

    # end_date = "2025-08-25"
    
    
    # symbols = [symbol_info["symbol"] for symbol_info in usdt_symbols]
    
    # datasets.download(
    #     exchange=exchange,
    #     data_types=download_date_type,
    #     from_date=start_date,
    #     to_date=end_date,
    #     symbols=symbols,
    #     api_key=TARDIS_KEY,
    #     concurrency=10,  # 这是控制并发下载的数量参数
    #     download_dir=f"tardis_data",
    # )

