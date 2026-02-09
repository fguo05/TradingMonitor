from utils import *


def trade(db_name:str, cur_date:str=None):
    """
    获取新闻，检索数据库，打分，写入数据库，做出决策
    :param db_name: "real":实盘数据库
                    "test":测试数据库
    :param cur_date: 日期，不填则获取24h内新闻
    """

    if db_name not in ["real", "test"]:
        print("数据库名错误！")
        return None

    # asset_type = input("请输入资产类型（stock/fund/crypto/forex）：").strip().lower()
    asset_type = "crypto"
    symbol_raw = input("请输入代码（例如TSLA, BTC, USD）：").strip().upper()
    # symbol_raw = "BTC"

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

    conn = create_db_connection(db_name)

    # 如果新闻在数据库中存在且打过分则直接用
    for news in news_list:
        news_db = get_news_by_url(conn, symbol_raw, news["url"])

        # 数据库找到了url
        if news_db:
            news["sentiment"] = news_db["sentiment"]
            news["sentiment_score"] = news_db["sentiment_score"]
            news['sentiment_title_content'] = news_db["sentiment_title_content"]
            news['sentiment_score_title_content'] = news_db["sentiment_score_title_content"]

    # 如果数据库中未找到，则 openai 处理新闻
    for news in news_list:
        # 1. title-only
        if "sentiment_score" not in news or news["sentiment_score"] is None:
            # print("    title-only:不在数据库或无分数，openai打分...")
            headline = news['title']
            raw_sentiment = classify_sentiment(symbol, headline, "title-only")

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

        # 2. title+content
        if "sentiment_score_title_content" not in news or news["sentiment_score_title_content"] is None:
            # print("    title+content:不在数据库或无分数，openai打分...")
            newspiece = f"News title: {news['title']}\nNews content: {news['content']}"
            raw_sentiment = classify_sentiment(symbol, newspiece, "title+content")

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

            news['sentiment_title_content'] = explanation
            news['sentiment_score_title_content'] = score

        # 打印新闻
        print(news["title"], news["content"])
        # 打印 openai 分数
        print("Title分数：", news["sentiment_score"], "Title+Content分数：", news["sentiment_score_title_content"])

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
    print("\ncalculating...\n")
    title_only_score, title_content_score = calculate_average_sentiment(news_list)

    print("====================== Title-ONLY ======================")
    if title_only_score is not None:
        print(f"Average Sentiment Score: {title_only_score:.4f}")
        decision = trading_decision(title_only_score)
        print(f"Trade Decision: {decision}")
    else:
        print("No valid Title-ONLY sentiment scores.")
    print("========================================================")

    print("\n=================== Title+Content ======================")
    if title_content_score is not None:
        print(f"Average Sentiment Score: {title_content_score:.4f}")
        decision = trading_decision(title_content_score)
        print(f"Trade Decision: {decision}")
    else:
        print("No valid Title+Content sentiment scores.")
    print("========================================================")


if __name__ == "__main__":
    """
    trade 函数参数
    第一个参数 db_name：指定使用哪个数据库
                      1. "test"：使用测试数据库
                      2. "real"：使用实盘数据库
    第二个参数 cur_date（可不写，不写则默认今日交易）
    """

    # trade(db_name="test", cur_date="2026-01-10")


    trade(db_name="test")