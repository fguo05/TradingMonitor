import pprint
import random
import time
import pymysql
import requests
from datetime import datetime, timedelta, timezone
import openai
import re

import yfinance as yf
import pandas as pd


# ============================================
# ===============API key手动赋值===============
# ============================================
OPENAI_API_KEY = ""


def creat_db_connection(db_name):
    if db_name not in ["real", "test"]:
        print("数据库名错误！")

    return pymysql.connect(
        host='rm-uf6q5h4a7tkthf82cno.mysql.rds.aliyuncs.com',  # 公网地址
        port=3306,  # 端口
        user='db_USER1',  # 数据库账号
        password='Cangjie!2025',  # 数据库密码
        db='db_test1' if db_name == "real" else "db_agent_test",
        charset='utf8mb4',  # 字符编码
    )


# ========== 用户输入代码 ==========
def format_ticker(asset_type: str, symbol: str) -> (str, str):
    if asset_type == 'crypto':
        ticker = f"CRYPTO:{symbol}"
    elif asset_type == 'forex':
        ticker = f"FOREX:{symbol}"
    else:
        ticker = symbol

    safe_name = re.sub(r'[\\/:*?"<>|]', '_', ticker)
    return ticker, safe_name


# ========== AlphaVantage新闻API获取新闻 ==========
def get_stock_news(symbol: str, cur_date:str=None, topics: str = None) -> list:
    """
    cur_date为空则默认以今天为 to_time 获取24h新闻
    """
    # Alpha Vantage News Sentiment API Ticker 映射表
    ALPHAVANTAGE_TICKER_MAP = {
        # ===== 股票 (US Stocks) =====
        "Apple": "AAPL",
        "Tesla": "TSLA",
        "Microsoft": "MSFT",
        "Amazon": "AMZN",
        "Google": "GOOGL",
        "Meta": "META",
        "Nvidia": "NVDA",

        # ===== 加密货币 (Crypto) =====
        "BTC": "CRYPTO:BTC",
        "Bitcoin": "CRYPTO:BTC",
        "Ethereum": "CRYPTO:ETH",
        "Solana": "CRYPTO:SOL",
        "Ripple": "CRYPTO:XRP",
        "Cardano": "CRYPTO:ADA",

        # ===== 外汇 (Forex) =====
        "USD": "FOREX:USD",
        "EUR": "FOREX:EUR",
        "JPY": "FOREX:JPY",
        "GBP": "FOREX:GBP",
        "CNY": "FOREX:CNY",
    }

    if symbol in ALPHAVANTAGE_TICKER_MAP:
        symbol = ALPHAVANTAGE_TICKER_MAP[symbol]

    if cur_date:
        cur_date_obj = datetime.strptime(cur_date, '%Y-%m-%d')
        start_time = cur_date_obj
        from_time = start_time.strftime("%Y%m%dT%H%M")
        to_time = (cur_date_obj + timedelta(hours=24)).strftime("%Y%m%dT%H%M")
    else:
        cur_date_obj = datetime.now(timezone.utc)
        start_time = cur_date_obj - timedelta(hours=24)
        from_time = start_time.strftime("%Y%m%dT%H%M")
        to_time = cur_date_obj.strftime("%Y%m%dT%H%M")

    # 构造API请求
    api_key = 'NJ7FJN6R0LIM0W77'
    base_url = 'https://www.alphavantage.co/query?function=NEWS_SENTIMENT'
    url = f"{base_url}&tickers={symbol}&time_from={from_time}&time_to={to_time}&apikey={api_key}"

    response = requests.get(url)
    data = response.json()

    if "feed" not in data:
        print(f"{symbol} 在过去24小时内没有找到相关新闻")
        return []

    news_list = [
        {
            "title": news.get('title', '').strip(),
            "content": news.get('summary', '').strip(),
            "publish_time": news.get('time_published', ''),
            "source": news.get('source', '').strip(),
            "topics": [t.get('topic', '') for t in news.get('topics', [])],
            "url": news.get('url', '').strip(),
            "alphavantage_sentiment_score": news["overall_sentiment_score"]
        }
        for news in data["feed"]
    ]

    return news_list


def get_news_by_url(conn, symbol: str, url: str):
    """
    根据 symbol + url 查询新闻：
    - News.url 唯一
    - NewsPiece (news_id, ticker_id) 联合唯一
    返回：dict 或 None
    """

    with conn.cursor(pymysql.cursors.DictCursor) as cursor:

        # 1. 获取 ticker_id
        sql_ticker = """
            SELECT id
            FROM Ticker
            WHERE symbol = %s
            LIMIT 1
        """
        cursor.execute(sql_ticker, (symbol,))
        row = cursor.fetchone()

        if not row:
            print(f"[WARN] symbol={symbol} 在 Ticker 表中不存在")
            return None

        ticker_id = row["id"]

        # 2️. 根据 url 获取 news_id
        sql_news = """
            SELECT id, title, content, publish_time, source, url
            FROM News
            WHERE url = %s
            LIMIT 1
        """
        cursor.execute(sql_news, (url,))
        news_row = cursor.fetchone()

        if not news_row:
            return None

        news_id = news_row["id"]

        # 3. 根据 (news_id, ticker_id) 获取 NewsPiece
        sql_piece = """
            SELECT
                sentiment,
                sentiment_score,
                alphavantage_sentiment_score,
                sentiment_title_content,
                sentiment_score_title_content
            FROM NewsPiece
            WHERE news_id = %s
              AND ticker_id = %s
            LIMIT 1
        """
        cursor.execute(sql_piece, (news_id, ticker_id))
        piece_row = cursor.fetchone()

        if not piece_row:
            return None

    # 4. 统一返回结构（单条 dict）
    return {
        "sentiment": piece_row.get("sentiment"),
        "sentiment_score": piece_row.get("sentiment_score"),
        "sentiment_title_content": piece_row.get("sentiment_title_content"),
        "sentiment_score_title_content": piece_row.get("sentiment_score_title_content"),
        "alphavantage_sentiment_score": piece_row.get("alphavantage_sentiment_score"),
    }


def get_news_from_db(conn, symbol, cur_date:str=None):
    """
    Obsolete: 由于API获取的 publish_time 信息不准确，不再使用时间获取新闻
    如果传入 cur_date 则返回当前日期的新闻，否则默认返回截止到目前 24h 内的新闻
    """

    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        # 1. 获取 ticker_id
        sql_ticker = """SELECT id FROM Ticker WHERE symbol = %s LIMIT 1"""
        cursor.execute(sql_ticker, (symbol,))
        row = cursor.fetchone()

        if not row:
            print(f"[WARN] symbol={symbol} 在 Ticker 表中不存在")
            return []

        ticker_id = row["id"]

        # 2. 构造时间
        if cur_date is None:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=24)
        else:
            cur_date = datetime.strptime(cur_date, "%Y-%m-%d").date()

            start_time = datetime.combine(cur_date, datetime.min.time())
            end_time = datetime.combine(cur_date, datetime.max.time())

        # 3. 查新闻
        sql_news = """
            SELECT
                n.id,
                n.title,
                n.content,
                n.url,
                np.sentiment,
                np.sentiment_score,
                np.alphavantage_sentiment_score,
                n.publish_time,
                n.source
            FROM NewsPiece np
            JOIN News n ON np.news_id = n.id
            WHERE np.ticker_id = %s
              AND n.publish_time BETWEEN %s AND %s
            ORDER BY n.publish_time DESC
        """

        cursor.execute(sql_news, (ticker_id, start_time, end_time))
        results = cursor.fetchall()

    # 4. 统一返回格式
    news_list = []
    for n in results:
        news_list.append({
            'id': n["id"],
            "title": n["title"],
            "content": n["content"],
            "url": n["url"],
            "sentiment": n.get("sentiment", None),
            "sentiment_score": n.get("sentiment_score", None),
            "alphavantage_sentiment_score": n.get("alphavantage_sentiment_score", None),
            "publish_time": n["publish_time"],
            "source": n["source"]
        })

    return news_list


# ========== chatgpt 情感分析函数 ==========
def classify_sentiment(company_name, headline, strategy):
    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    if strategy not in ["title-only", "title+content"]:
        print("策略输入错误！")
        return None

    if strategy == "title-only":
        prompt = f"""
        Forget all your previous instructions. Pretend you are a financial expert.
        You are a financial expert with stock recommendation experience.
        Answer "YES" if good news, "NO" if bad news, or "UNKNOWN" if uncertain in the first line.
        Then elaborate with one short and concise sentence on the next line.
        Is this headline good or bad for the stock price of {company_name} in the short term?

        Headline: {headline}
        """
    else:
        prompt = f"""
        Forget all your previous instructions. Pretend you are a financial expert.
        You are a financial expert with stock recommendation experience.
        Answer "YES" if good news, "NO" if bad news, or "UNKNOWN" if uncertain in the first line.
        Then elaborate with one short and concise sentence on the next line.
        Is this newspiece good or bad for the stock price of {company_name} in the short term?

        {headline}
        """

    messages = [{"role": "user", "content": prompt}]
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0
    )

    content = response.choices[0].message.content.strip()
    return content


def save_news_to_db(news_list, symbol, asset_type, conn, max_retries=5):
    """
    使用 INSERT ... ON DUPLICATE KEY 优化并发与重复问题，并对 1205 锁等待超时进行重试。
    news_list: list of dict (each dict must contain title, content, source, url, publish_time, sentiment, sentiment_score, alphavantage_sentiment_score)
    """
    # 我们使用 autocommit 来减少长事务
    conn.autocommit(True)
    cursor = conn.cursor()

    try:
        # 直接用 INSERT IGNORE 或 INSERT ... ON DUPLICATE 来创建/获取 ticker，避免竞态
        cursor.execute("INSERT IGNORE INTO ticker (symbol, type) VALUES (%s, %s)", (symbol, asset_type))
        cursor.execute("SELECT id FROM ticker WHERE symbol = %s", (symbol,))
        ticker_row = cursor.fetchone()
        if not ticker_row:
            raise RuntimeError(f"无法获取 ticker id for symbol={symbol}")
        ticker_id = ticker_row[0]

        for news in news_list:
            # parse publish_time
            try:
                publish_time = datetime.strptime(news['publish_time'], "%Y%m%dT%H%M%S")
            except Exception as e:
                print(f"跳过格式错误的时间: {news.get('publish_time')}，错误: {e}")
                continue

            title = news['title']
            content = news.get('content', '')
            source = news.get('source', '')
            url = news.get('url', '')
            topics = ', '.join(news.get('topics', []))
            sentiment_score = float(news.get('sentiment_score', 0.0))
            sentiment = news.get('sentiment', '')
            alphavantage_sentiment_score = news.get('alphavantage_sentiment_score', 0.0)

            # 插入 news：如果已存在（url 唯一），则不报错并返回现有 id
            # 注意：news 表必须在 title 上有 UNIQUE 索引
            inserted = False
            for attempt in range(max_retries):
                try:
                    cursor.execute("""
                        INSERT INTO news (title, content, source, url, publish_time, topics)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)
                    """, (title, content, source, url, publish_time, topics))
                    news_id = cursor.lastrowid  # 对于 ON DUPLICATE + LAST_INSERT_ID 能拿到存在/新插入的 id
                    if not news_id:
                        cursor.execute("SELECT id FROM news WHERE url = %s", (url,))
                        news_id = cursor.fetchone()[0]
                        print(f"{title} (fallback) -> news_id = {news_id}")
                    inserted = True
                    break
                except pymysql.OperationalError as e:
                    # 1205 是 Lock wait timeout exceeded
                    if e.args and e.args[0] == 1205:
                        sleep_time = (2 ** attempt) + random.random()
                        print(f"插入 news 遇到 1205 锁等待超时，{sleep_time:.1f}s 后重试 (attempt {attempt+1}/{max_retries})")
                        time.sleep(sleep_time)
                        continue
                    else:
                        raise

            if not inserted:
                raise RuntimeError(f"尝试插入 news 多次失败（锁超时）: {title}")

            # 插入/更新 newspiece：确保 newspiece 在 (news_id, ticker_id) 有 UNIQUE 索引
            # 如果你只想避免重复插入而不更新，可以把后面的 UPDATE 写成 `id = id`
            for attempt in range(max_retries):
                try:
                    # 如果改成title+content的话把两个字段改成sentiment_title_content和sentiment_score_title_content
                    cursor.execute("""
                        INSERT INTO newspiece (news_id, ticker_id, sentiment_score, sentiment, alphavantage_sentiment_score)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        sentiment_score = VALUES(sentiment_score),
                        sentiment = VALUES(sentiment),
                        alphavantage_sentiment_score = VALUES(alphavantage_sentiment_score)
                    """, (news_id, ticker_id, sentiment_score, sentiment, alphavantage_sentiment_score))
                    # 若需要，可检查 cursor.rowcount 等
                    break
                except pymysql.OperationalError as e:
                    if e.args and e.args[0] == 1205:
                        sleep_time = (2 ** attempt) + random.random()
                        print(f"插入 newspiece 遇到 1205，{sleep_time:.1f}s 后重试 (attempt {attempt+1}/{max_retries})")
                        time.sleep(sleep_time)
                        continue
                    else:
                        raise
    finally:
        try:
            cursor.close()
        except Exception:
            pass


def calculate_average_sentiment(news_list):
    # 统一转成float，因为可能Decimal、float都有无法相加（API搜到的是float，数据库中是Decimal）
    title_only_scores = [float(item['sentiment_score']) for item in news_list if 'sentiment_score' in item and item['sentiment_score'] is not None]
    title_content_scores = [float(item['sentiment_score_title_content']) for item in news_list if 'sentiment_score_title_content' in item and item['sentiment_score_title_content'] is not None]

    title_only_score = title_content_score = None

    if title_only_scores:
        title_only_score = sum(title_only_scores) / len(title_only_scores)

    if title_content_scores:
        title_content_score = sum(title_content_scores) / len(title_content_scores)

    return title_only_score, title_content_score

def trading_decision(score):
    if score > 0.3:
        print("*** 开多仓 ***")
        return 2 #多开仓
    elif 0.2 < score <= 0.3:
        print("*** 清空空仓 ***")
        return 1 #清空空仓
    elif -0.2 <= score <= 0.2:
        print("*** 不动 ***")
        return 0 #不动
    elif -0.3 <= score <-0.2:
        print("*** 清空多仓 ***")
        return -1 #清空多仓
    elif score<-0.3:
        print("*** 开空仓 ***")
        return -2 #开空仓


def get_yfinance_data(ticker, start, end):
    YFINANCE_TICKER_MAP = {
        # ===== 股票 (US Stocks) =====
        "Apple": "AAPL",
        "Tesla": "TSLA",
        "Microsoft": "MSFT",
        "Amazon": "AMZN",
        "Google": "GOOGL",
        "Meta": "META",
        "Nvidia": "NVDA",

        # ===== 加密货币 (Crypto) =====
        "bitcoin": "BTC-USD",
        "BTC": "BTC-USD",
        "Bitcoin": "BTC-USD",
        "Ethereum": "ETH-USD",
        "Solana": "SOL-USD",
        "Ripple": "RIPPLE-USD",
        "Cardano": "ADA-USD",

        # ===== 外汇 (Forex) =====
        "USD": "FOREX:USD",
        "EUR": "FOREX:EUR",
        "JPY": "FOREX:JPY",
        "GBP": "FOREX:GBP",
        "CNY": "FOREX:CNY",
    }

    # 将ticker转成yf可识别的ticker，如Bitcoin -> BTC-USD
    if ticker in YFINANCE_TICKER_MAP:
        ticker = YFINANCE_TICKER_MAP[ticker]

    end = (datetime.strptime(end, "%Y-%m-%d")+timedelta(days=1)).strftime("%Y-%m-%d") # 注意yfinance是取到end前一天，所以end+1

    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=False)
    df.index = pd.to_datetime(df.index)
    df.columns = df.columns.get_level_values(0)

    return df



if __name__ == "__main__":
    news_list = get_stock_news("CRYPTO:BTC")