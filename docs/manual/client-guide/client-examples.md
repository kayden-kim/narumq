# 언어별 MQTT 클라이언트 연동 예제

| 항목 | 내용 |
| :--- | :--- |
| **대상 독자** | NaruMQ 브로커와 통신하는 애플리케이션을 개발하는 소프트웨어 엔지니어 |
| **문서 목적** | Go, Python, Node.js의 표준 MQTT 클라이언트 라이브러리를 사용한 연동 코드를 제공한다. |
| **검증 환경** | NaruMQ 로컬 개발 모드 (`127.0.0.1:1883`) 및 TLS 보안 모드 (`127.0.0.1:8883`) |

---

## 1. Go 언어 예제 (`eclipse/paho.mqtt.golang`)

Go 생태계에서 널리 쓰이는 Eclipse Paho 라이브러리를 사용한 예제입니다.

### 의존성 설치
```sh
go get github.com/eclipse/paho.mqtt.golang
```

### 소스 코드 (`main.go`)
```go
package main

import (
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://127.0.0.1:1883") // 로컬 개발 모드 주소
	opts.SetClientID("go-sample-client")
	opts.SetCleanSession(true)

	// 메시지 수신 기본 핸들러
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("[수신] 토픽: %s, 내용: %s\n", msg.Topic(), string(msg.Payload()))
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("연결 실패: %v", token.Error())
	}
	fmt.Println("NaruMQ 브로커 연결 성공")

	// 토픽 구독
	topic := "sensors/device01/temperature"
	if token := client.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
		log.Fatalf("구독 실패: %v", token.Error())
	}
	fmt.Printf("토픽 구독 완료: %s\n", topic)

	// 메시지 발행 (QoS 0)
	payload := `{"temperature": 23.8, "timestamp": 1720000000}`
	token := client.Publish(topic, 0, false, payload)
	token.Wait()
	fmt.Printf("[발행] 메시지 전송 완료: %s\n", payload)

	time.Sleep(2 * time.Second)
	client.Disconnect(250)
}
```

> 💡 **참고:** QoS 1 또는 QoS 2 메시지도 동일한 방식으로 발행 및 구독할 수 있습니다 (예: `opts.SetCleanSession(false)`, `client.Publish(topic, 1, false, payload)`). standalone 프로세스는 메모리 내에서 ACK 핸드셰이크를 완료합니다.

---

## 2. Python 예제 (`paho-mqtt`)

Python의 표준 라이브러리인 `paho-mqtt`를 사용한 예제입니다.

### 의존성 설치
```sh
pip install paho-mqtt
```

### 소스 코드 (`client.py`)
```python
import time
import paho.mqtt.client as mqtt

# 메시지 수신 콜백
def on_message(client, userdata, msg):
    print(f"[수신] 토픽: {msg.topic}, 내용: {msg.payload.decode('utf-8')}")

# 연결 성공 콜백
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("NaruMQ 브로커 연결 성공")
        client.subscribe("sensors/device01/temperature", qos=0)
    else:
        print(f"연결 실패, 코드: {rc}")

# MQTT v5 / v3.1.1 클라이언트 생성
client = mqtt.Client(client_id="python-sample-client", protocol=mqtt.MQTTv311)
client.on_connect = on_connect
client.on_message = on_message

# 로컬 브로커 연결 (익명 개발 모드)
client.connect("127.0.0.1", 1883, keepalive=60)
client.loop_start()

# 메시지 발행 (QoS 0)
time.sleep(1)
payload = '{"humidity": 55.2, "status": "normal"}'
client.publish("sensors/device01/temperature", payload=payload, qos=0)
print(f"[발행] 메시지 전송 완료: {payload}")

time.sleep(2)
client.loop_stop()
client.disconnect()
```

---

## 3. Node.js 예제 (`mqtt`)

Node.js 환경에서 가장 널리 쓰이는 `mqtt` 패키지를 사용한 비동기 예제입니다.

### 의존성 설치
```sh
npm install mqtt
```

### 소스 코드 (`app.js`)
```javascript
const mqtt = require('mqtt');

// 브로커 연결 옵션
const client = mqtt.connect('mqtt://127.0.0.1:1883', {
  clientId: 'nodejs-sample-client',
  clean: true,
  connectTimeout: 4000
});

client.on('connect', () => {
  console.log('NaruMQ 브로커 연결 성공');

  const topic = 'sensors/device01/temperature';
  client.subscribe(topic, { qos: 0 }, (err) => {
    if (!err) {
      console.log(`구독 등록 완료: ${topic}`);

      // 메시지 발행
      const payload = JSON.stringify({ battery: 98, voltage: 3.7 });
      client.publish(topic, payload, { qos: 0 }, () => {
        console.log(`[발행] 메시지 전송: ${payload}`);
      });
    }
  });
});

client.on('message', (topic, message) => {
  console.log(`[수신] 토픽: ${topic}, 내용: ${message.toString()}`);
  client.end();
});

client.on('error', (err) => {
  console.error('MQTT 연결 오류:', err);
});
```

---

## 4. TLS 보안 모드 적용 팁

운영 환경에서 브로커의 TLS 포트(`8883`)로 연결할 때는 CA 인증서를 지정해야 합니다.

::: code-group

```go [Go TLS 설정]
tlsConfig := &tls.Config{
    RootCAs: caCertPool,
    ServerName: "broker.example.com",
}
opts.SetTLSConfig(tlsConfig)
opts.AddBroker("ssl://broker.example.com:8883")
opts.SetUsername("my-username")
opts.SetPassword("my-password")
```

```python [Python TLS 설정]
client.tls_set(ca_certs="ca.crt")
client.username_pw_set("my-username", "my-password")
client.connect("broker.example.com", 8883)
```

```javascript [Node.js TLS 설정]
const fs = require('fs');
const client = mqtt.connect('mqtts://broker.example.com:8883', {
  ca: [fs.readFileSync('ca.crt')],
  username: 'my-username',
  password: 'my-password'
});
```

:::

---

## 5. 다음 단계

- 브로커 프로세스를 직접 실행하고 운영 환경에 맞게 튜닝하는 방법은 [브로커 운영자 가이드](../operator-guide/installation.md)를 참조하십시오.
