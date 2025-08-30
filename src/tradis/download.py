from datetime import datetime
from tardis_dev import datasets, get_exchange_details
import pandas as pd
from dotenv import load_dotenv
import os
from tqdm import tqdm
import json

load_dotenv()

TARDIS_KEY = os.getenv("TARDIS_KEY")


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
    bybit_usdt_symbols = extract_available_symbols("bybit")

    # 加载已完成的交易对
    success_symbols = load_success_symbols()

    # 可以bybit_usdt_symbols[:2] 先测试一下
    for symbol_info in tqdm(bybit_usdt_symbols[:], desc="Downloading bybit usdt trades"):
        if symbol_info["symbol"] in success_symbols:
            print(f"Symbol {symbol_info['symbol']} already downloaded")
            continue
        try:
            # 一次性下载start_date : end_date 的数据, 他的库内部是异步并发的, 可以并发下载;
            print(f"Downloading {symbol_info['symbol']} from {symbol_info['start_date']} to {symbol_info['end_date']}")
            datasets.download(
                exchange="bybit",
                data_types=["trades"],
                from_date=symbol_info["start_date"],
                to_date=symbol_info["end_date"],
                symbols=[symbol_info["symbol"]],
                api_key=TARDIS_KEY,
                concurrency=10,  # 这是控制并发下载的数量参数
                download_dir=f"bybit_usdt_trades/{symbol_info['symbol']}",
            )
            with open("success.json", "a") as f:
                f.write(
                    json.dumps(
                        {
                            "symbol": symbol_info["symbol"],
                            "start_date": symbol_info["start_date"],
                            "end_date": symbol_info["end_date"],
                        }
                    )
                    + "\n"
                )
        except Exception as e:
            print(f"Error downloading {symbol_info['symbol']}: {e}")
            with open("error.json", "a") as f:
                f.write(
                    json.dumps(
                        {
                            "symbol": symbol_info["symbol"],
                            "start_date": symbol_info["start_date"],
                            "end_date": symbol_info["end_date"],
                            "error": str(e),
                        }
                    )
                    + "\n"
                )
            continue
