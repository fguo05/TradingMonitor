import os
from pandas.errors import EmptyDataError
from utils import *

import matplotlib.pyplot as plt

pd.set_option('display.max_rows', None)


def backtest_sentiment_strategy(symbol, asset_type, start_date, end_date, db_name):
    yf_data = get_yfinance_data("Bitcoin", start_date, end_date)

    position = 0 # 当前仓位
    records = []

    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    for current_date in dates[:-1]:
        date_str = current_date.strftime("%Y-%m-%d")
        next_date = current_date + timedelta(days=1)

        print(f"===== {date_str} =====")

        # 获取新闻
        news_list = get_stock_news(symbol, cur_date=date_str)
        if news_list:
            print(f"成功获取 {len(news_list)} 条新闻")
        else:
            continue

        conn = creat_db_connection(db_name)
        news_from_db = get_news_from_db(conn, symbol, date_str)

        # 情感打分
        # 如果新闻在数据库中存在且打过分则直接用，否则用 openai 打分
        for news in news_list:
            found = False
            for news_db in news_from_db:
                if news['url'] == news_db['url'] and news_db['sentiment_score'] is not None:
                    news['sentiment'] = news_db['sentiment']
                    news['sentiment_score'] = news_db['sentiment_score']
                    found = True
                    break

            if found: # 在数据库中找到了
                continue

            # 如果数据库中未找到，则 openai 处理新闻

            # title ONLY
            headline = news['title']
            raw_sentiment = classify_sentiment(symbol, headline)

            # title+content
            # title_content = f"News title: {news['title']}\nNews content: {news['content']}"
            # raw_sentiment = classify_sentiment(symbol, title_content)

            # 处理 openai 分析结果
            parts = raw_sentiment.split('\n', 1)
            label = parts[0].strip().upper()
            explanation = parts[1].strip() if len(parts) > 1 else ''

            # 映射 label 到分数字段
            if label == "YES":
                score = 1.0
            elif label == "NO":
                score = -1.0
            else:
                score = 0.0

            news['sentiment_score'] = score
            news['sentiment'] = explanation

        # 新闻写入数据库
        print("正在将新闻写入数据库...")
        save_news_to_db(
            news_list=news_list,
            symbol=symbol,  # 如 BTC
            asset_type=asset_type,  # 如 crypto
            conn=conn
        )
        print("新闻成功写入数据库。")

        avg_sentiment = calculate_average_sentiment(news_list)
        if avg_sentiment is None:
            continue

        # 交易决策
        decision = trading_decision(avg_sentiment)

        # 仓位更新
        prev_position = position

        if decision == 2:
            position = 1
        elif decision == -2:
            position = -1
        elif decision == 1 and position == -1:
            position = 0
        elif decision == -1 and position == 1:
            position = 0
        # decision == 0 → 不动

        # 价格
        try:
            today_close = yf_data.loc[date_str]["Close"]
            next_close = yf_data.loc[next_date.strftime("%Y-%m-%d")]["Close"]
        except KeyError:
            continue

        # 计算收益
        ret = prev_position * (next_close - today_close) / today_close

        win = ret > 0 if prev_position != 0 else None

        record = {
            "date": current_date,
            "avg_sentiment": avg_sentiment,
            "decision": decision,
            "prev_position": prev_position,
            "new_position": position,
            "today_close": today_close,
            "next_close": next_close,
            "return": ret,
            "win": win
        }

        records.append(record)

    records_df = pd.DataFrame(records)
    records_df["date"] = pd.to_datetime(records_df["date"])
    records_df = records_df.sort_values("date").reset_index(drop=True)

    return records_df


def save_backtest_records_to_csv(records_df: pd.DataFrame, csv_path: str):
    records_df = records_df.copy()
    records_df["date"] = pd.to_datetime(records_df["date"])

    # 从csv获取旧的数据，将新旧数据合并
    if os.path.exists(csv_path):
        try:
            old_df = pd.read_csv(csv_path)
            old_df["date"] = pd.to_datetime(old_df["date"])

            merged_df = pd.concat([old_df, records_df], ignore_index=True)

        except EmptyDataError:
            print(f"[WARN] {csv_path} is empty. Recreating file.")
            merged_df = records_df.copy()

    else:
        merged_df = records_df.copy()

    # 排序 + 去重
    merged_df = merged_df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    merged_df.to_csv(csv_path, index=False)
    print(f"回测结果已存入{csv_path}")


def compare_2_strategies():
    """ title vs title+content """

    df_title = pd.read_csv("btc_sentiment_backtest.csv")
    df_tc = pd.read_csv("btc_sentiment_backtest_title+content.csv")

    for df in [df_title, df_tc]:
        df["date"] = pd.to_datetime(df["date"])
        df.sort_values("date", inplace=True)
        df.reset_index(drop=True, inplace=True)

    # ========= 去除NaN（未交易）的天 =========
    df_title_trade = df_title.dropna(subset=["win"]).copy()
    df_tc_trade = df_tc.dropna(subset=["win"]).copy()

    # 转成 Bool
    df_title_trade["win"] = df_title_trade["win"].astype(bool)
    df_tc_trade["win"] = df_tc_trade["win"].astype(bool)

    # ========= rolling：最近30“笔”交易胜率，而不是30天，因为去除NaN了 =========
    df_title_trade["rolling_winrate_30"] = df_title_trade["win"].rolling(30).mean()
    df_tc_trade["rolling_winrate_30"] = df_tc_trade["win"].rolling(30).mean()

    print("Title ONLY trades:")
    print(df_title_trade[["date", "rolling_winrate_30"]])
    print("Title + Content trades:")
    print(df_tc_trade[["date", "rolling_winrate_30"]])

    # ========= 画图 =========
    plt.figure()
    plt.plot(df_title_trade["date"], df_title_trade["rolling_winrate_30"], label="Title Only")
    plt.plot(df_tc_trade["date"], df_tc_trade["rolling_winrate_30"], label="Title + Content")
    plt.xlabel("Date")
    plt.ylabel("Rolling 30-Trade Win Rate")
    plt.title("Sentiment Strategy Win Rate Comparison (Rolling 30 Trades)")
    plt.legend()
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    # symbol_raw = "BTC"
    # asset_type = "crypto"
    #
    # df = backtest_sentiment_strategy(
    #     symbol=symbol_raw,
    #     asset_type=asset_type,
    #     start_date="2026-01-07",
    #     end_date="2026-01-09"
    # )
    #
    # csv_path = "btc_sentiment_backtest.csv"
    #
    # save_backtest_records_to_csv(
    #     records_df=df,
    #     csv_path=csv_path
    # )
    #
    # # 只统计有持仓的天
    # trade_df = df[df["win"].notna()]
    #
    # win_rate = trade_df["win"].astype(bool).mean()
    # total_return = (df["return"] + 1).prod() - 1
    #
    # print("======trade_df['win']=====")
    # print(f"胜率: {win_rate:.2%}")
    # print(f"累计收益: {total_return:.2%}")
    #
    # trade_df["rolling_winrate_30"] = trade_df["win"].rolling(30).mean()
    # print(trade_df)
    #

    compare_2_strategies()