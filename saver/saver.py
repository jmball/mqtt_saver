"""Save data obtained from MQTT broker."""

import collections
import contextlib
import csv
import json
import pathlib
import time
import warnings

import numpy as np
import paho.mqtt.client as mqtt


# create thread-safe containers for storing save settings
folder = collections.deque(maxlen=1)
archive = collections.deque(maxlen=1)


def save_data(exp, m):
    """Save data to text file.

    Parameters
    ----------
    topic : str
        MQTT topic. Lowest level sub-topic sets file type.
    m : dict
        Message dictionary.
    """
    save_folder = folder[0]
    timestamp = pathlib.PurePath(save_folder).parts[-1][-10:]
    save_path = save_folder.joinpath(f"{m['id']}_{timestamp}.{exp}")

    # create file with header if pixel
    if not save_path.exists():
        with open(save_path, "w", newline="\n") as f:
            if exp == "eqe":
                f.writelines(
                    "timestamp (s)\twavelength (nm)\tX (V)\tY (V)\tAux In 1 (V)\tAux In 2 (V)\tAux In 3 (V)\tAux In 4 (V)\tR (V)\tPhase (deg)\tFreq (Hz)\tCh1 display\tCh2 display\tR/Aux In 1\tEQE\tJsc (ma/cm2)\n"
                )
            elif exp == "spectrum":
                f.writelines("wls (nm)\tirr (W/m^2/nm)\n")
            elif exp == "psu":
                f.writelines(
                    "voltage (v)\tcurrent (A)\ttime (s)\tstatus\tpsu_current (A)\n"
                )
            else:
                f.writelines("voltage (v)\tcurrent (A)\ttime (s)\tstatus\n")

    with open(save_path, "a", newline="\n") as f:
        writer = csv.writer(f, delimiter="\t")
        if (exp == "iv") or (exp == "spectrum"):
            writer.writerows(m["data"])
        else:
            writer.writerow(m["data"])

    # TODO: add option for network archive


def save_cache(m):
    """Save data from cache.

    Parameters
    ----------
    m : dict
        Message dictionary.
    """
    save_folder = folder[0]
    save_path = save_folder.joinpath(f"{m['filename']}")

    if not save_path.exists():
        with open(save_path, "w") as f:
            f.write(m["contents"])


def update_settings(m):
    """Update save settings.

    Parameters
    ----------
    m : dict
        Message dictionary.
    """
    f = pathlib.Path(m["folder"])
    a = pathlib.Path(m["archive"])
    folder.append(f)
    archive.append(a)


def on_message(mqttc, obj, msg):
    """Act on an MQTT msg."""
    m = json.loads(msg.payload)
    if (exp := msg.topic.split("/")[-1]) == "settings":
        update_settings(m)
    elif exp == "cache":
        save_cache(m)
    elif exp in ["vt", "iv", "mppt", "it", "eqe", "spectrum", "psu"]:
        save_data(exp, m)
    else:
        warnings.warn(f"Topic not handled: {msg.topic}")


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

    mqttc = mqtt.Client()
    mqttc.on_message = on_message
    mqttc.connect(args.mqtt_host)
    # subscribe to all sub-topics in data channel
    mqttc.subscribe("data/#", qos=2)
    mqttc.loop_forever()
