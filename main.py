import pprint

from utils import *


def trade(cur_date:str=None):
    # asset_type = input("请输入资产类型（stock/fund/crypto/forex）：").strip().lower()
    asset_type = "crypto"
    # symbol_raw = input("请输入代码（例如TSLA, BTC, USD）：").strip().upper()
    symbol_raw = "BTC"

    symbol, safe_symbol = format_ticker(asset_type, symbol_raw)

    # print(f"已识别并格式化后的代码（API用）：{symbol}")
    # print(f"对应安全的文件夹名：{safe_symbol}")

    # ========== 设定参数并查重 ==========
    all_topics = "aaaaa"

    if cur_date:
        print(f"正在获取{cur_date}当天的新闻...")
        news_list = get_stock_news(symbol, cur_date, topics=all_topics)
    else:
        print(f"正在获取 {symbol} 在过去 24 小时内的新闻...")
        news_list = get_stock_news(symbol, topics=all_topics)

    if news_list:
        print(f"成功获取 {len(news_list)} 条新闻")
    else:
        return

    # 从数据库获取新闻，看是否已经存在并打过分，避免重复打分
    if cur_date:
        news_from_db = get_news_from_db(conn, symbol_raw, cur_date)
    else:
        news_from_db = get_news_from_db(conn, symbol_raw)

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
        # print(f"新闻未找到，openai进行打分...{news['title']}")
        headline = news['title']
        raw_sentiment = classify_sentiment(symbol, headline)

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

        news['sentiment'] = explanation
        news['sentiment_score'] = score

    # 新闻写入数据库
    print("正在将新闻写入数据库...")
    save_news_to_db(
        news_list=news_list,
        symbol=symbol_raw,  # 如 BTC
        asset_type=asset_type,  # 如 crypto
        conn=conn
    )
    print("新闻成功写入数据库。")

    # 示例调用
    print("calculating...")
    avg_sentiment = calculate_average_sentiment(news_list)

    if avg_sentiment is not None:
        print(f"Average Sentiment Score: {avg_sentiment:.4f}")
    else:
        print("No valid sentiment scores found.")

    # 判断是否需要交易
    if avg_sentiment is not None:
        decision = trading_decision(avg_sentiment)
        print(f"Trade Decision: {decision}")



if __name__ == "__main__":
    # trade("2026-01-02")

    trade()