import os
import boto3
import json
import random
import time
import uuid
import logging
from locust import User, task, constant, events
from faker import Faker
import datetime
from dotenv import load_dotenv


load_dotenv()

REGION = os.environ.get("REGION") if os.environ.get("REGION") else "us-east-1"
BATCH_SIZE = int(os.environ.get("LOCUST_BATCH_SIZE")) if os.environ.get("LOCUST_BATCH_SIZE") else 1

faker = Faker()


class KinesisBotoClient:
    def __init__(self, region_name, stream_name, batch_size):
        stream_name = os.getenv("KINESIS_DEFAULT_STREAM_NAME")
        self.kinesis_client = boto3.client('kinesis', region_name=region_name)
        self.stream_name = stream_name
        self.batch_size = batch_size

        logging.info("Created KinesisBotoClient in '%s' for Kinesis stream '%s'", region_name, stream_name)

    def send(self, records):
        request_meta = {
            "request_type": "Send data",
            "name": "Kinesis",
            "start_time": time.time(),
            "response_length": 0,
            "response": None,
            "context": {},
            "exception": None,
        }
        start_perf_counter = time.perf_counter()

        try:
            cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            self.kinesis_client.put_records(
                StreamName=self.stream_name, Records=records)
            with open("kinesis.csv", "a") as f:
                _hash = records[0]["PartitionKey"]
                f.write(f"{_hash},{cur_time}\n")
        except Exception as e:
            request_meta['exception'] = e

        request_meta["response_time"] = (
            time.perf_counter() - start_perf_counter) * 1000

        for _ in range(self.batch_size):
            events.request.fire(**request_meta)


class KinesisBotoUser(User):
    abstract = True

    def __init__(self, env):
        super().__init__(env)
        self.client = KinesisBotoClient(region_name=REGION, stream_name=self.host, batch_size=BATCH_SIZE)


class SensorAPIUser(KinesisBotoUser):
    wait_time = constant(1)

    def on_start(self):
        self.user_id = str(uuid.uuid4())
        return super().on_start()
    
    def generate_random_txn(self):
        txn = {}
        txn["Hash"] = "0x" + ''.join(random.choices('0123456789abcdef', k=32))
        txn["Status"] = 'PENDING'
        txn["Amount"] = random.randint(1, 99)
        txn["Type"] = random.choice([1, 2, 3])
        txn["Nonce"] = random.randint(0, 999999)
        txn["Fee"] = random.randint(1, 9)
        txn["Sender"] = [
            ["0x" + ''.join(random.choices('0123456789abcdef', k=16)), txn["Amount"]]]
        txn["Receiver"] = [
            ["0x" + ''.join(random.choices('0123456789abcdef', k=16)), txn["Amount"]]]
        message = {}
        message["type"] = "TRANSACTION"
        message["data"] = txn
        return message, txn["Hash"]

    def generate_random_block(self):
        block = {}
        block["Hash"] = "0x" + ''.join(random.choices('0123456789abcdef', k=32))
        block["PreviousBlockHash"] = "0x" + ''.join(random.choices('0123456789abcdef', k=32))
        block["Height"] = random.randint(1, 100)
        block["Nonce"] = random.randint(0, 999999)
        block["Difficulty"] = random.randint(1, 100)
        block["Miner"] = "0x" + ''.join(random.choices('0123456789abcdef', k=32))
        block["Timestamp"] = random.randint(1000000000, 2000000000)
        block["Transactions"] = []
        for _ in range(random.randint(0, 200)):
            msg, _ = self.generate_random_txn()
            txn = msg["data"]
            txn["Status"] = 'APPROVED'
            block["Transactions"].append(txn)
        block["Uncles"] = []
        block["Sidecar"] = []
        if (len(block["Transactions"]) > 0):
            for _ in range(random.randint(0, 3)):
                off_chain_data = {}
                off_chain_data["ID"] = "0x" + \
                    ''.join(random.choices('0123456789abcdef', k=32))
                off_chain_data["Size"] = random.randint(1024, 2048)
                off_chain_data["TransactionID"] = random.choice(
                    block["Transactions"])["Hash"]
                block["Sidecar"].append(off_chain_data)
        block["OffChainDataSizes"] = [random.randint(1024, 2048) for _ in range(2)]
        message = {}
        message["type"] = "BLOCK"
        message["data"] = block
        return message, block["Hash"]

    @task
    def send_sensor_value(self):
        events = []
        BATCH_SIZE = 1
        for i in range(BATCH_SIZE):
            test_event, partition_key = None, None
            # if random.randint(0, 20) == 0:
            #     test_event, partition_key = self.generate_random_block()
            # else:
            test_event, partition_key = self.generate_random_txn()
            if test_event == None:
                continue
            event = {'Data': json.dumps(test_event), 'PartitionKey': partition_key}
            events.append(event)

        logging.debug("Generated events for Kinesis: %s", events)
        self.client.send(events)
