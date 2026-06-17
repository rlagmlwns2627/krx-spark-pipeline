import logging
import os
from datetime import datetime

import pandas as pd
from pykrx import stock

logger = logging.getLogger(__name__)

# 현재 파일 기준으로 CSV 경로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TICKER_FILE = os.path.join(BASE_DIR, "kospi_tickers.csv")


def get_tickers() -> list[str]:
    """
    CSV 파일에서 KOSPI 종목 코드 반환
    KRX API 대신 로컬 파일 사용 (Docker 환경에서 KRX API 차단 우회)
    """
    df = pd.read_csv(TICKER_FILE, dtype=str)
    return df["ticker"].tolist()


def fetch_ohlcv(date: str, market: str = "KOSPI") -> pd.DataFrame:
    """
    특정 날짜의 전 종목 OHLCV 수집
    date: "YYYYMMDD" 형식
    휴장일이면 빈 DataFrame 반환
    """
    tickers = get_tickers()
    logger.info(f"{date} 수집 시작 — {len(tickers)}개 종목")

    records = []
    for ticker in tickers:
        try:
            df = stock.get_market_ohlcv(date, date, ticker)
            if df.empty:
                continue
            row = df.iloc[0]
            records.append({
                "ticker": ticker,
                "date": date,
                "open": int(row["시가"]),
                "high": int(row["고가"]),
                "low": int(row["저가"]),
                "close": int(row["종가"]),
                "volume": int(row["거래량"]),
                # 거래대금은 별도 API로 제공되므로 종가 * 거래량으로 계산
                "trade_value": int(row["종가"]) * int(row["거래량"]),
            })
        except Exception as e:
            # 특정 종목 실패 시 전체 수집을 멈추지 않고 경고만 기록
            logger.warning(f"[{ticker}] 수집 실패: {e}")
            continue

    result = pd.DataFrame(records)
    logger.info(f"수집 완료 — {len(result)}개 종목")
    return result