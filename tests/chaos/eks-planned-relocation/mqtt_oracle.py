"""MQTT half of the planned cross-worker same-EBS relocation acceptance (DEP-17, DEP-21).

Run `prepare`, then `relocate_worker.py`, recreate the target Pod port-forward to the replacement Pod,
then run `verify` without publishing again. `--output` must name the same run directory for all three
steps. Its name is used as the cluster ID and synthetic topic prefix. The private directory contains
`mqtt.password`. Local target/survivor MQTT ports default to 28883/23883.

Both the physical relocation scenario and the post-relocation MQTT scenario must pass for DRN-SCN-034.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

sys.dont_write_bytecode = True
sys.path.append(str(Path(__file__).resolve().parents[2]))  # tests/ holds scenario_results.py
from scenario_results import scenario_result  # noqa: E402


def run_oracle(mode: str, private: Path, out: Path, target_port: int, publisher_port: int) -> dict:
    run = out.name
    password = (private / 'mqtt.password').read_text(encoding='utf-8').strip()
    base = 'acceptance/' + run
    queue_topic = base + '/queue'
    retained_topic = base + '/retained'
    queued = [(run + ':offline-qos1:' + str(i)).encode() for i in range(3)]
    retained = (run + ':retained-marker').encode()

    def digest(value):
        return hashlib.sha256(value).hexdigest()

    def save(name, obj):
        (out / name).write_text(json.dumps(obj, indent=2), encoding='utf-8')

    class Client:
        def __init__(self, identity, port, clean=True, expiry=0):
            self.connected = threading.Event()
            self.disconnected = threading.Event()
            self.subscribed = threading.Event()
            self.received = threading.Event()
            self.messages = []
            self.errors = []
            self.session_present = False
            self.acks = []
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=identity,
                                      protocol=mqtt.MQTTv5, reconnect_on_failure=False)
            self.client.username_pw_set('acceptance-device', password)

            def connect(client, userdata, flags, reason_code, properties):
                self.session_present = flags.session_present
                if reason_code.is_failure:
                    self.errors.append('CONNACK ' + str(reason_code))
                self.connected.set()

            def subscribe(client, userdata, mid, reasons, properties):
                if any(r.is_failure for r in reasons):
                    self.errors.append('SUBACK failure')
                self.subscribed.set()

            def message(client, userdata, msg):
                self.messages.append({'topic': msg.topic, 'sha256': digest(msg.payload),
                                      'qos': msg.qos, 'retain': msg.retain, 'dup': msg.dup})
                self.received.set()

            def publish(client, userdata, mid, reason_code, properties):
                self.acks.append({'mid': mid, 'reason': str(reason_code), 'reasonValue': reason_code.value})
                if reason_code.is_failure:
                    self.errors.append('PUBACK failure ' + str(reason_code))
            self.client.on_connect = connect
            self.client.on_subscribe = subscribe
            self.client.on_message = message
            self.client.on_publish = publish
            self.client.on_disconnect = lambda *args: self.disconnected.set()
            props = Properties(PacketTypes.CONNECT)
            props.SessionExpiryInterval = expiry
            self.client.connect('127.0.0.1', port, 30, clean_start=clean, properties=props)
            self.client.loop_start()
            assert self.connected.wait(15), 'CONNACK timeout'
            assert not self.errors, self.errors

        def subscribe(self, topic):
            self.client.subscribe(topic, qos=1)
            assert self.subscribed.wait(15), 'SUBACK timeout'
            assert not self.errors, self.errors

        def publish(self, topic, payload, retain=False):
            p = self.client.publish(topic, payload, qos=1, retain=retain)
            p.wait_for_publish(timeout=15)
            assert p.is_published() and p.rc == mqtt.MQTT_ERR_SUCCESS, 'PUBACK not received'
            assert not self.errors, self.errors

        def close(self):
            self.client.disconnect()
            self.disconnected.wait(5)
            self.client.loop_stop()

    def check_retained():
        reader = Client('ebs-worker-reader', target_port)
        try:
            reader.subscribe(retained_topic)
            assert reader.received.wait(15), 'retained payload timeout'
            found = [m for m in reader.messages if m['topic'] == retained_topic]
            assert found and all(m['sha256'] == digest(retained) and m['retain'] and m['qos'] == 1 for m in found), found
            return found
        finally:
            reader.close()

    try:
        if mode == 'prepare':
            subscriber = Client('ebs-worker-subscriber', target_port, clean=True, expiry=3600)
            subscriber.subscribe(queue_topic)
            subscriber.close()
            publisher = Client('ebs-worker-publisher', publisher_port)
            try:
                publisher.publish(retained_topic, retained, retain=True)
                for value in queued:
                    publisher.publish(queue_topic, value)
                assert len(publisher.acks) == 4, publisher.acks
                pubacks = publisher.acks
            finally:
                publisher.close()
            outcome = {'result': 'PASS', 'runId': run, 'pubacks': pubacks,
                       'retained': check_retained(), 'offlineSessionClientId': 'ebs-worker-subscriber',
                       'offlinePayloadHashes': [digest(p) for p in queued], 'retainedHash': digest(retained),
                       'sessionExpirySeconds': 3600}
            save('mqtt-before-relocation.json', outcome)
            print('PASS: retained plus three offline QoS 1 messages received successful PUBACKs before relocation')
        else:
            subscriber = Client('ebs-worker-subscriber', target_port, clean=False, expiry=3600)
            try:
                assert subscriber.session_present, 'persistent session not resumed'
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    if set(m['sha256'] for m in subscriber.messages) == set(digest(p) for p in queued):
                        break
                    time.sleep(0.1)
                assert {m['sha256'] for m in subscriber.messages} == {digest(p) for p in queued}, subscriber.messages
                assert all(m['topic'] == queue_topic and m['qos'] == 1 for m in subscriber.messages), subscriber.messages
                messages = list(subscriber.messages)
            finally:
                subscriber.close()
            outcome = {'result': 'PASS', 'runId': run, 'sessionPresent': True,
                       'offlineMessages': messages, 'allExpectedOfflineHashesReceived': True,
                       'qos1DuplicateCount': len(messages) - len(queued), 'retained': check_retained(),
                       'noRepublishDuringRecovery': True}
            save('mqtt-after-relocation.json', outcome)
            print('PASS: persistent session and all three QoS 1 payloads resumed; retained payload unchanged; no republish')
        return outcome
    except Exception as exc:
        save('mqtt-' + mode + '-failure.json', {'result': 'FAIL', 'error': str(exc)})
        raise


# Scenarios: drain-and-rolling-update.md:DRN-SCN-034
@scenario_result
def verify_relocation_mqtt(private: Path, out: Path, target_port: int = 28883, publisher_port: int = 23883) -> dict:
    physical = json.loads((out / 'physical-relocation-result.json').read_text(encoding='utf-8'))
    assert physical['result'] == 'PASS' and physical['runId'] == out.name
    assert physical['safeBeforeDelete'] is True and physical['allThreeBrokersReady'] is True
    return run_oracle('verify', private, out, target_port, publisher_port)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('prepare', 'verify'))
    parser.add_argument('--private', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True, help='same run directory as relocate_worker.py')
    parser.add_argument('--target-port', type=int, default=28883)
    parser.add_argument('--publisher-port', type=int, default=23883)
    args = parser.parse_args(argv)
    if args.mode == 'prepare':
        run_oracle(args.mode, args.private, args.output, args.target_port, args.publisher_port)
    else:
        verify_relocation_mqtt(args.private, args.output, args.target_port, args.publisher_port)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
