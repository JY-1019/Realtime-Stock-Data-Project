import ccxt
import asyncio
from confluent_kafka import Producer

async def fetch_and_send_to_kafka(upbit, bybit, producer):
    upbit_symbol = 'BTC/KRW'
    bybit_symbol = 'BTC/USD'

    while True:
        try:
            upbit_data = await upbit.fetch_ticker(upbit_symbol)
            bybit_data = await bybit.fetch_ticker(bybit_symbol)

            producer.produce('your_kafka_topic', value=str({'upbit': upbit_data, 'bybit': bybit_data}))

            def delivery_report(err, msg):
                if err is not None:
                    print('메시지 전송 오류: {}'.format(err))
                else:
                    print('메시지가 성공적으로 전송되었습니다: {}'.format(msg.value()))

            # 비동기로 전송
            producer.poll(0)
            producer.flush()

        except Exception as e:
            print('오류 발생: {}'.format(str(e)))

        await asyncio.sleep(60)  # 60초마다 데이터 업데이트

def periodic_task(fetch_and_send, upbit, bybit, producer):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_send(upbit, bybit, producer))

if __name__ == '__main__':
    import ccxtpro
    from confluent_kafka import Producer

    # Upbit 설정
    upbit = ccxtpro.upbit()

    # Bybit 설정
    bybit = ccxtpro.bybit()

    # Kafka Producer 설정
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',  # Kafka 브로커 주소
    }
    producer = Producer(kafka_config)

    # 일급 함수를 사용하여 주기적으로 실행
    periodic_task(fetch_and_send_to_kafka, upbit, bybit, producer)
