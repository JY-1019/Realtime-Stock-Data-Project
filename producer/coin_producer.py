import os
import json
import pprint
import asyncio
import ccxt.pro as ccxtpro


def get_upbit_exchange():
    current_path = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(current_path, "config.json"), "r") as config_file:
        config = json.load(config_file)
        upbit_api_key = config["upbit_access_key"]
        upbit_secret_key = config["upbit_secret_key"]

    return ccxtpro.upbit(
        {
            "apiKey": upbit_api_key,
            "secret": upbit_secret_key,
            "enableRateLimit": True,
        }
    )


def get_bybit_exchange():
    current_path = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(current_path, "config.json"), "r") as config_file:
        config = json.load(config_file)
        bybit_api_key = config["bybit_access_key"]
        bybit_secret_key = config["bybit_secret_key"]

    return ccxtpro.bybit(
        {
            "apiKey": bybit_api_key,
            "secret": bybit_secret_key,
            "enableRateLimit": True,
        }
    )


def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


async def send_coin_data_to_kafka(exchange, symbol, producer, topic):
    try:
        ticker = await exchange.watch_ticker(symbol)
        json_ticker = json.dumps({"symbol": symbol, "data": ticker})
        producer.produce(
            topic, value=json_ticker.encode("utf-8"), callback=delivery_report
        )
        producer.flush()
    except Exception as error:
        print("Exception occurred: {}".format(error))

    await asyncio.sleep(3)


async def send_multiple_coins_to_kafka(exchange, symbols: list, producer, topic):
    coins = [
        send_coin_data_to_kafka(exchange, symbol, producer, topic) for symbol in symbols
    ]
    await asyncio.gather(*coins)
    await exchange.close()
