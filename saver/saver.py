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


def save_data(kind, data):
    """Save data to text file.

    Parameters
    ----------
    topic : str
        MQTT topic. Lowest level sub-topic sets file type.
    m : dict
        Message dictionary.
    """
    exp = kind.replace("_measurement", "")
    save_folder = folder[0]
    timestamp = pathlib.PurePath(save_folder).parts[-1][-10:]
    save_path = save_folder.joinpath(f"{data['id']}_{timestamp}.{exp}")

    # create file with header if pixel
    if not save_path.exists():
        with open(save_path, "w", newline="\n") as f:
            if exp == "eqe":
                f.writelines(
                    "timestamp (s)\twavelength (nm)\tX (V)\tY (V)\tAux In 1 (V)\tAux In 2 (V)\tAux In 3 (V)\tAux In 4 (V)\tR (V)\tPhase (deg)\tFreq (Hz)\tCh1 display\tCh2 display\tR/Aux In 1\tEQE\tJsc (ma/cm2)\n"
                )
            else:
                f.writelines("voltage (v)\tcurrent (A)\ttime (s)\tstatus\n")

    with open(save_path, "a", newline="\n") as f:
        writer = csv.writer(f, delimiter="\t")
        if exp == "iv":
            writer.writerows(data)
        else:
            writer.writerow(data)


def save_calibration(kind, data):
    """Save calibration data.

    Parameters
    ----------
    kind : str
        Kind of data.
    data : list
        Data to save.
    """
    save_folder = folder[0]
    save_path = save_folder.joinpath(f"{kind}_measurement.cal")

    # write headers
    with open(save_path, "w", newline="\n") as f:
        if (cal := kind.replace("_calibration", "")) == "eqe":
            f.writelines(
                "timestamp (s)\twavelength (nm)\tX (V)\tY (V)\tAux In 1 (V)\tAux In 2 (V)\tAux In 3 (V)\tAux In 4 (V)\tR (V)\tPhase (deg)\tFreq (Hz)\tCh1 display\tCh2 display\tR/Aux In 1\n"
            )
        elif cal == "solarsim":
            f.writelines("wls (nm)\tirr (W/m^2/nm)\n")
        elif cal == "psu":
            f.writelines(
                "voltage (v)\tcurrent (A)\ttime (s)\tstatus\tpsu_current (A)\n"
            )

    # append data
    with open(save_path, "a", newline="\n") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerows(data)


def update_settings(data):
    """Update save settings.

    Parameters
    ----------
    data : str
        Folder name.
    """
    f = pathlib.Path(data)
    if f.exists() is False:
        # create directory in cwd
        f.mkdir()
    folder.append(f)


def on_message(mqttc, obj, msg):
    """Act on an MQTT msg."""
    m = json.loads(msg.payload)
    kind = m["kind"]
    data = m["data"]

    if kind == "save_settings":
        update_settings(data)
    elif kind in ["solarsim_calibration", "eqe_calibration", "psu_calibration"]:
        save_calibration(kind, data)
    elif kind in [
        "vt_measurement",
        "iv_measurement",
        "mppt_measurement",
        "it_measurement",
        "eqe_measurement",
    ]:
        save_data(kind, data)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-mqtthost",
        type=str,
        default="",
        help="IP address or hostname for MQTT broker.",
    )

    args = parser.parse_args()

    mqttc = mqtt.Client()
    mqttc.on_message = on_message
    mqttc.connect(args.mqtthost)
    # subscribe to all sub-topics in cli data channel
    mqttc.subscribe("server/response", qos=2)
    mqttc.loop_forever()
