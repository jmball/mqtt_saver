"""Save data obtained from MQTT broker."""

import csv
import json
import pathlib
import time

import paho.mqtt.client as mqtt
import yaml


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
                    "timestamp (s)\twavelength (nm)\tX (V)\tY (V)\tAux In 1 (V)\tAux"
                    + " In 2 (V)\tAux In 3 (V)\tAux In 4 (V)\tR (V)\tPhase (deg)\tFreq"
                    + " (Hz)\tCh1 display\tCh2 display\tR/Aux In 1\tEQE\tJsc (ma/cm2)\n"
                )
            else:
                f.writelines("voltage (v)\tcurrent (A)\ttime (s)\tstatus\n")

    with open(save_path, "a", newline="\n") as f:
        writer = csv.writer(f, delimiter="\t")
        if exp == "iv":
            writer.writerows(data)
        else:
            writer.writerow(data)


def save_settings(mqttc):
    """Save calibration data.

    Parameters
    ----------
    mqttc : mqtt.Client
        MQTT save client.
    """
    if folder is not None:
        save_folder = folder
    else:
        save_folder = pathlib.Path()

    # save config
    if config != {}:
        save_path = save_folder.joinpath("measurement_config.yaml")
        with open(save_path, "w") as f:
            yaml.dump(calibration, f)
    else:
        mqttc.publish(
            "log",
            json.dumps(
                {"kind": "warning", "data": "No configuration settings to save."}
            ),
        ).wait_for_publish()

    # save calibration
    if calibration != {}:
        # save eqe calibration
        eqe_diode = config["experiments"]["eqe"]["calibration_diode"]
        save_path = save_folder.joinpath(f"{eqe_diode}_eqe.cal")

        with open(save_path, "w", newline="\n") as f:
            f.writelines(
                "timestamp (s)\twavelength (nm)\tX (V)\tY (V)\tAux In 1 (V)\tAux In 2 "
                + "(V)\tAux In 3 (V)\tAux In 4 (V)\tR (V)\tPhase (deg)\tFreq (Hz)\tCh1 "
                + "display\tCh2 display\tR/Aux In 1\n"
            )
        with open(save_path, "a", newline="\n") as f:
            writer = csv.writer(f, delimiter="\t")
            try:
                writer.writerows(calibration["eqe"][eqe_diode])
            except KeyError:
                mqttc.publish(
                    "log",
                    json.dumps(
                        {"kind": "warning", "data": "No EQE calibration data to save."}
                    ),
                ).wait_for_publish()

        # save spectral calibration
        save_path = save_folder.joinpath("spectrum.cal")

        with open(save_path, "w", newline="\n") as f:
            f.writelines("wls (nm)\tirr (W/m^2/nm)\n")
        with open(save_path, "a", newline="\n") as f:
            writer = csv.writer(f, delimiter="\t")
            try:
                writer.writerows(calibration["spectrum"])
            except KeyError:
                mqttc.publish(
                    "log",
                    json.dumps(
                        {
                            "kind": "warning",
                            "data": "No spectrum calibration data to save.",
                        }
                    ),
                ).wait_for_publish()

        # save psu calibration
        save_path = save_folder.joinpath("psu.cal")

        with open(save_path, "w", newline="\n") as f:
            f.writelines(
                "voltage (v)\tcurrent (A)\ttime (s)\tstatus\tpsu_current (A)\n"
            )
        with open(save_path, "a", newline="\n") as f:
            writer = csv.writer(f, delimiter="\t")
            try:
                writer.writerows(calibration["psu"])
            except KeyError:
                mqttc.publish(
                    "log",
                    json.dumps(
                        {"kind": "warning", "data": "No PSU calibration data to save."}
                    ),
                ).wait_for_publish()
    else:
        mqttc.publish(
            "log",
            json.dumps({"kind": "warning", "data": "No calibration settings to save."}),
        ).wait_for_publish()


def update_folder(data):
    """Update save settings.

    Parameters
    ----------
    data : str
        Folder name.
    """
    global folder

    folder = pathlib.Path(data)
    if folder.exists() is False:
        # create directory in cwd
        folder.mkdir()


def update_config(new_config):
    """Update configuration settings.

    Parameters
    ----------
    new_config : dict
        Configuration settings.
    """
    global config

    config = new_config


def update_calibration(new_calibration):
    """Update calibration data.

    Parameters
    ----------
    new_calibration : dict
        Configuration settings.
    """
    global calibration

    calibration = new_calibration


def on_message(mqttc, obj, msg):
    """Act on an MQTT msg."""
    m = json.loads(msg.payload)
    kind = m["kind"]
    data = m["data"]

    if kind == "save_folder":
        update_folder(data)
    elif kind == "config":
        update_config(data)
    elif kind == "calibration":
        update_calibration(data)
    elif kind == "save_settings":
        save_settings(mqttc)
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
        default="127.0.0.1",
        help="IP address or hostname for MQTT broker.",
    )

    args = parser.parse_args()

    # init global variables
    folder = None
    config = {}
    calibration = {}

    mqttc = mqtt.Client()
    mqttc.on_message = on_message
    mqttc.connect(args.mqtthost)
    # subscribe to all sub-topics in cli data channel
    mqttc.subscribe("server/response", qos=2)

    # get latest config data from server
    mqttc.publish(
        "server/request", json.dumps({"action": "get_config", "data": ""}), qos=2
    ).wait_for_publish()

    # wait for config to be updated
    time.sleep(1)

    # get latest calibration data from server
    mqttc.publish(
        "server/request", json.dumps({"action": "get_calibration", "data": ""}), qos=2
    ).wait_for_publish()

    mqttc.loop_forever()
