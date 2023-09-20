import os
import asyncio

from coin_producer import *
from stock_producer import *
from symbols import upbit_symbols, bybit_symbols, stock_symbols
from confluent_kafka import Producer


async def main():
    upbit_exchange = get_upbit_exchange()
    bybit_exchange = get_bybit_exchange()
    stock_exchange = get_stock_exchange()

    upbit_coins = [coin.value for coin in upbit_symbols]
    bybit_coins = [coin.value for coin in bybit_symbols]

    stocks = stock_symbols(stock_exchange)

    kafka_brokers = "broker1:9092,broker2:9092,broker3:9092"
    producer_config = {"bootstrap.servers": kafka_brokers}

    producer = Producer(producer_config)

    while True:
        await asyncio.gather(
            send_multiple_coins_to_kafka(
                upbit_exchange, upbit_coins, producer, "upbit"
            ),
            send_multiple_coins_to_kafka(
                bybit_exchange, bybit_coins, producer, "bybit"
            ),
            send_multiple_stock_to_kafka(stock_exchange, stocks, producer, "kStock"),
        )


if __name__ == "__main__":
    asyncio.run(main())
