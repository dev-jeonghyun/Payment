import asyncio
from contextlib import asynccontextmanager
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
import joblib

# 1. 모델 로드
model = joblib.load("isolation_forest_model.pkl")


# 2. Kafka 컨슈머 및 프로듀서 연동 루프 (전체가 while True로 감싸져 안정성 보장)
async def consume_and_predict():
    while True:
        consumer = None
        producer = None
        try:
            consumer = AIOKafkaConsumer(
                "payment-topic",
                bootstrap_servers="kafka:9092",
                group_id="anomaly-detection-group",
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")

            await consumer.start()
            await producer.start()
            print("Kafka 컨슈머 및 프로듀서가 성공적으로 연결되었습니다.")

            # 메시지 수신 및 처리 루프
            async for msg in consumer:
                try:
                    payment_data = json.loads(msg.value.decode("utf-8"))
                    order_id = payment_data.get("orderId")

                    # 데이터 필드 키값 매칭 (스프링 부트 DTO와 일치)
                    features = [[
                        payment_data.get("timeDiff", 0),
                        payment_data.get("locDiff", 0),
                        payment_data.get("isNight", 0),
                        payment_data.get("isHighAmount", 0),
                    ]]

                    prediction = model.predict(features)
                    is_anomalous = True if prediction[0] == -1 else False
                    result_status = "FRAUD" if is_anomalous else "NORMAL"

                    print(
                        f"분석 완료: OrderId {order_id} -> "
                        f"{'이상(FRAUD)' if is_anomalous else '정상(NORMAL)'}"
                    )

                    # 3. 결과를 'detection-result-topic'으로 발행
                    result_message = json.dumps(
                        {"orderId": order_id, "status": result_status}
                    )
                    await producer.send_and_wait(
                        "detection-result-topic", result_message.encode("utf-8")
                    )
                    print(f"결과 전송 완료: {order_id} -> detection-result-topic")

                except Exception as inner_e:
                    print(f"메시지 개별 처리 중 에러 발생: {inner_e}")

        except Exception as e:
            print(f"카프카 연결 또는 통신 에러 발생 (3초 후 재시도): {e}")
            await asyncio.sleep(3)
        finally:
            if consumer:
                try:
                    await consumer.stop()
                except Exception:
                    pass
            if producer:
                try:
                    await producer.stop()
                except Exception:
                    pass


# 4. Lifespan 설정
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_and_predict())
    yield
    # 필요시 task 취소 로직 추가 가능


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"status": "Kafka 탐지 서버 정상 가동 중"}