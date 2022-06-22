import os
import pathlib
import shutil
from datetime import date
from pathlib import Path

import requests
from dateutil.relativedelta import relativedelta

# def get_price_data_bybit() -> None:
#     url = "https://public.bybit.com/spot_index/BTCUSD/"
#     ds = date(2022, 1, 1)
#     de = date(2022, 6, 14)
#     for i in range((de - ds).days + 1):
#         date = ds + timedelta(i)
#         day_str = str(date.day).zfill(2)
#         month_str = str(date.month).zfill(2)
#         file_name = f"BTCUSD2022-{month_str}-{day_str}_index_price.csv.gz"
#         urlData = requests.get(url + file_name).content
#         save_path = f"histrical-data/price/bybit/2022/{date.month}/2022_{date.month}_{date.day}_BTC.gz"
#         with open(save_path, mode="wb") as f:  # wb でバイト型を書き込める
#             f.write(urlData)

#     for file_path in pathlib.Path("./histrical-data/price/bybit/2022").rglob("*"):
#         if Path.is_file(file_path):
#             if ".gz" in str(file_path):
#                 with gzip.open(file_path, mode="rb") as gzip_file:
#                     content = gzip_file.read()
#                     target_path = str(file_path).replace(".gz", ".csv")
#                     with open(target_path, mode="wb") as decompressed_file:
#                         decompressed_file.write(content)
#                         os.remove(str(file_path))


def get_price_data_binance(currency: str, product: str, category: str, minute: int, s_date: date, e_date: date) -> None:
    """
    Binanceから価格データを取得する関数
    1ヶ月あたりでデータを取得する
    下記を参照
    https://data.binance.vision/?prefix=data/

    Parameters
    ----------
    currency: str
        取得する通貨名

    s_date : datetime
    e_date : datetime
        取得する開始日と終了日

    type : str
        取得データの種類

    minute : int
        ロウソク足の長さ

    Returns
    -------
    None
    ./dataフォルダにCSVが保存される

    """
    try:
        # example
        # "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/15m/BTCUSDT-15m-2022-05.zip"
        url = f"https://data.binance.vision/data/{product}/monthly/{category}/{currency}/{minute}m/"

        while s_date < e_date:
            year_str = s_date.year
            month_str = str(s_date.month).rjust(2, "0")
            zip_name = f"{currency}-{minute}m-{year_str}-{month_str}.zip"

            save_folder = "data/"
            res = requests.get(url + zip_name)
            if res.status_code == 404:
                raise Exception("File doesn't exist")
            urlData = requests.get(url + zip_name).content

            os.makedirs(save_folder, exist_ok=True)
            with open(save_folder + zip_name, mode="wb") as f:  # wb でバイト型を書き込める
                f.write(urlData)
            for file_path in pathlib.Path("./" + save_folder).rglob("*"):
                if Path.is_file(file_path):
                    if ".zip" in str(file_path):
                        shutil.unpack_archive(str(file_path), save_folder)
                        os.remove(str(file_path))
            s_date += relativedelta(months=1)
    except Exception as e:
        print(e)
