import os
import json
import pprint
import mojito
import asyncio
import datetime


def get_stock_exchange():
    current_path = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(current_path, "config.json"), "r") as config_file:
        config = json.load(config_file)
        stock_api_key = config["stock_access_key"]
        stock_secret_key = config["stock_secret_key"]
        acc_no = config["account_num"]

    return mojito.KoreaInvestment(
        api_key=stock_api_key, api_secret=stock_secret_key, acc_no=acc_no
    )


def deliver_report(err, msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


async def fetch_stock_data(exchange, symbol):
    ticker = exchange.fetch_price(symbol)
    return ticker


async def send_stock_data_to_kafka(exchange, symbol: tuple, producer, topic):
    try:
        ticker = await fetch_stock_data(exchange, symbol[1])
        json_ticker = json.dumps(
            {
                "name": symbol[0],
                "symbol": symbol[1],
                "data": ticker,
            }
        )
        producer.produce(
            topic, value=json_ticker.encode("utf-8"), callback=deliver_report
        )
        producer.poll(0)
        producer.flush()
    except Exception as error:
        print("Exception occurred: {}".format(error))


async def send_multiple_stock_to_kafka(exchange, symbols: list, producer, topic):
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    if now.hour >= 9 and now.hour < 15:
        for symbol in symbols:
            await send_stock_data_to_kafka(exchange, symbol, producer, topic)
