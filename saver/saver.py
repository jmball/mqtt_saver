"""Save data obtained from MQTT broker."""

import collections
import contextlib
import json
import time

import numpy as np
import paho.mqtt.client as mqtt


# bind context manager methods to mqtt client class
def __enter__(self):
    pass


def __exit__(self, *args):
    self.loop_stop()
    self.disconnect()


mqtt.Client.__enter__ = __enter__
mqtt.Client.__exit__ = __exit__


# create thread-safe containers for storing latest data and save info
exp1_latest = collections.deque(maxlen=1)
exp2_latest = collections.deque(maxlen=1)
exp3_latest = collections.deque(maxlen=1)
exp4_latest = collections.deque(maxlen=1)
exp5_latest = collections.deque(maxlen=1)
folder = collections.deque(maxlen=1)

# initialise save info/data queues
exp1_latest.append({"msg": {"clear": True, "id": "-"}, "data": np.empty((0, 2))})
exp2_latest.append({"msg": {"clear": True, "id": "-"}, "data": np.empty((0, 4))})
exp3_latest.append({"msg": {"clear": True, "id": "-"}, "data": np.empty((0, 4))})
exp4_latest.append({"msg": {"clear": True, "id": "-"}, "data": np.empty((0, 2))})
exp5_latest.append({"msg": {"clear": True, "id": "-"}, "data": np.empty((0, 3))})


# MQTT on_message callback functions for experiment type
def on_message_1(mqttc, obj, msg):
    """Act on an MQTT msg.

    Append or clear data stored in a queue.
    """
    m = json.loads(msg.payload)
    data = exp1_latest[0]["data"]
    if m["clear"] is True:
        np.savetxt(
            m["id"].txt,
            data,
            delimiter="\t",
            newline="\n",
            header="\tvoltage (V)\tcurrent (A)\ttime (s)\tstatus",
            comments="",
        )
        data = np.empty((0, 2))
    else:
        data = np.append(data, np.array([[m["x1"], m["y1"]]]), axis=0)
    exp1_latest.append({"msg": m, "data": data})


def on_message_2(mqttc, obj, msg):
    """Act on an MQTT msg.

    Append or clear data stored in a queue.
    """
    m = json.loads(msg.payload)
    data = exp2_latest[0]["data"]
    if m["clear"] is True:
        np.savetxt(
            m["id"].txt,
            data,
            delimiter="\t",
            newline="\n",
            header="\tvoltage (V)\tcurrent (A)\ttime (s)\tstatus",
            comments="",
        )
        data = np.empty((0, 4))
    else:
        if len(data) == 0:
            data0 = np.array(m["data"])
            data1 = np.zeros(data0.shape)
            data = np.append(data0, data1, axis=1)
        else:
            data[:, 2:] = np.array(m["data"])
    exp2_latest.append({"msg": m, "data": data})


def on_message_3(mqttc, obj, msg):
    """Act on an MQTT msg.

    Append or clear data stored in a queue.
    """
    m = json.loads(msg.payload)
    data = exp3_latest[0]["data"]
    if m["clear"] is True:
        np.savetxt(
            m["id"].txt,
            data,
            delimiter="\t",
            newline="\n",
            header="\tvoltage (V)\tcurrent (A)\ttime (s)\tstatus",
            comments="",
        )
        data = np.empty((0, 4))
    else:
        data = np.append(data, np.array([[m["x1"], m["y1"], m["y2"], m["y3"]]]), axis=0)
    exp3_latest.append({"msg": m, "data": data})


def on_message_4(mqttc, obj, msg):
    """Act on an MQTT msg.

    Append or clear data stored in a queue.
    """
    m = json.loads(msg.payload)
    data = exp4_latest[0]["data"]
    if m["clear"] is True:
        np.savetxt(
            m["id"].txt,
            data,
            delimiter="\t",
            newline="\n",
            header="\tvoltage (V)\tcurrent (A)\ttime (s)\tstatus",
            comments="",
        )
        data = np.empty((0, 2))
    else:
        data = np.append(data, np.array([[m["x1"], m["y1"]]]), axis=0)
    exp4_latest.append({"msg": m, "data": data})


def on_message_5(mqttc, obj, msg):
    """Act on an MQTT msg.

    Append or clear data stored in a queue.
    """
    m = json.loads(msg.payload)
    data = exp5_latest[0]["data"]
    if m["clear"] is True:
        np.savetxt(
            m["id"].txt,
            data,
            delimiter="\t",
            newline="\n",
            header="\ttime (s)\tvoltage (V)\teqe (%)\tjsc (mA/cm^2)",
            comments="",
        )
        data = np.empty((0, 3))
    else:
        data = np.append(data, np.array([[m["x1"], m["y1"], m["y2"]]]), axis=0)
    exp5_latest.append({"msg": m, "data": data})


def on_message_6(mqttc, obj, msg):
    """Act on an MQTT msg.

    Get save information.
    """
    m = json.loads(msg.payload)
    if m["folder"] is not None:
        folder.append(m["folder"])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-mqtt-host",
        type=str,
        default="",
        help="IP address or hostname for MQTT broker.",
    )

    args = parser.parse_args()

    MQTTHOST = args.mqtt_host

    subtopics = []
    subtopics.append(f"data/voltage")
    subtopics.append(f"data/iv")
    subtopics.append(f"data/mppt")
    subtopics.append(f"data/current")
    subtopics.append(f"data/eqe")
    subtopics.append(f"data/saver")

    on_messages = [
        on_message_1,
        on_message_2,
        on_message_3,
        on_message_4,
        on_message_5,
        on_message_6,
    ]

    # run mqtt subscriber threads in context manager so they disconnect cleanly upon
    # exit.
    with contextlib.ExitStack() as stack:
        for subtopic, on_msg in zip(subtopics, on_messages):
            mqttc = mqtt.Client()
            stack.enter_context(mqttc)
            mqttc.on_message = on_msg
            mqttc.connect(MQTTHOST)
            mqttc.subscribe(subtopic, qos=2)
            mqttc.loop_start()

        # block main thread from terminating
        while True:
            time.sleep(60)
