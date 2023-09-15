import os
import asyncio

from coin_producer import *
from coin_symbol import upbit_symbols, bybit_symbols
from confluent_kafka import Producer


async def main():
    upbit_exchange = get_upbit_exchange()
    bybit_exchange = get_bybit_exchange()

    upbit_coins = [coin.value for coin in upbit_symbols]
    bybit_coins = [coin.value for coin in bybit_symbols]

    kafka_brokers = "kafka1:9092,kafka2:9092,kafka3:9092"
    producer_config = {"bootstrap.servers": kafka_brokers}

    producer = Producer(producer_config)

    await asyncio.gather(
        send_multiple_coins_to_kafka(upbit_exchange, upbit_coins, producer, "upbit"),
        send_multiple_coins_to_kafka(bybit_exchange, bybit_coins, producer, "bybit"),
    )


if __name__ == "__main__":
    asyncio.run(main())
