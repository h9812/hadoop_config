import requests
import json
from datetime import datetime
import time
from confluent_kafka import Producer

SYMBOL = 'VCB'
TS = 1718373000
SLEEP_TIME = 10
TEST_MODE = True
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092'
}
KAFKA_TOPIC = 'rt-stock'
# KAFKA_PRODUCER = Producer(**KAFKA_CONF)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

TEST_DT = datetime.strptime("14/06/2024 10", "%d/%m/%Y %H")

if __name__ == "__main__":

    latest_ts = 0
    while True:
        try:
            res = requests.post('https://quotes.vcbs.com.vn/f/df.asmx/do',
                                headers={'Content-Type': 'application/json'},
                                data=json.dumps({'a':'3','p':[SYMBOL,TS,0]}))

            if res.status_code == 200:
                res_data = res.json()['d'][0].split("#")
                data = []
                price = 0
                ts = -25200
                amount = 0
                for idx, row in enumerate(res_data):
                    change_values = [float(x) if x != 'B' and x != 'S' and x != '' else x for x in row.split('|')]
                    price = price + change_values[0]
                    ts = ts + change_values[1]
                    amount = change_values[2]
                    bOrS = change_values[3]
                    data.append([ts, price, amount])

                for idx, elem in enumerate(data):
                    ts = elem[0]
                    if ts <= latest_ts: continue
                    
                    if TEST_MODE:
                        now = datetime.now()
                        if ts > datetime(year=TEST_DT.year, month=TEST_DT.month, day=TEST_DT.day, hour=TEST_DT.hour, minute=now.minute, second=now.second).timestamp():
                            break
                    
                    price = elem[1]
                    amount = elem[2]
                    # KAFKA_PRODUCER.produce(KAFKA_TOPIC, key=ts, value=json.dumps({'ts': ts, 'data': {'price': price, 'amount': amount}}), callback=delivery_report)
                    # KAFKA_PRODUCER.flush()
                    print(idx + 1)
                    print({'ts': ts, 'data': {'price': price/1000, 'amount': amount/1000}})
                    latest_ts = ts
            else:
                print('Request failed')
                print('Status code:', res.status_code)
                print('Response:', res.text)
        except Exception as ex: print(ex)
        time.sleep(SLEEP_TIME)
    
