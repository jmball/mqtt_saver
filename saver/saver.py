"""Save data obtained from MQTT broker."""

import csv
import json
import pathlib
import threading
import time
import uuid

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
    if folder is not None:
        save_folder = folder
        timestamp = pathlib.PurePath(save_folder).parts[-1][-10:]
    else:
        save_folder = pathlib.Path()
        timestamp = ""

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


def save_config(mqttc):
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


def save_calibration(mqttc, payload):
    """Save calibration data.

    Parameters
    ----------
    mqttc : mqtt.Client
        MQTT save client.
    """
    if folder is not None:
        save_folder = folder
    else:
        save_folder = pathlib.Path("temp")

    # save calibration
    if calibration != {}:
        # save eqe calibration
        try:
            for key, value in calibration["eqe"]:
                diode = key
                timestamp = value["timestamp"]
                data = value["data"]

                save_path = save_folder.joinpath(f"{timestamp}_{diode}_eqe.cal")

                if save_path.exists() is False:
                    with open(save_path, "w", newline="\n") as f:
                        f.writelines(
                            "timestamp (s)\twavelength (nm)\tX (V)\tY (V)\tAux In 1 (V)\t"
                            + "Aux In 2 (V)\tAux In 3 (V)\tAux In 4 (V)\tR (V)\tPhase "
                            + "(deg)\tFreq (Hz)\tCh1 display\tCh2 display\tR/Aux In 1\n"
                        )
                    with open(save_path, "a", newline="\n") as f:
                        writer = csv.writer(f, delimiter="\t")
                        writer.writerows(data)
        except KeyError:
            mqttc.publish(
                "log",
                json.dumps(
                    {"kind": "warning", "data": "No EQE calibration data to save."}
                ),
            ).wait_for_publish()

        # save spectral calibration
        try:
            timestamp = calibration["solarsim"]["spectrum"]["timestamp"]
            data = calibration["solarsim"]["spectrum"]["data"]
            save_path = save_folder.joinpath(f"{timestamp}_spectrum.cal")

            if save_path.exists() is False:
                with open(save_path, "w", newline="\n") as f:
                    f.writelines("wls (nm)\traw (counts)\tirr (W/m^2/nm)\n")
                with open(save_path, "a", newline="\n") as f:
                    writer = csv.writer(f, delimiter="\t")
                    writer.writerows(data)
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

        # save solarsim diode measurements
        try:
            for key, value in calibration["solarsim"]["diodes"]:
                diode = key
                timestamp = value["timestamp"]
                data = value["data"]

                save_path = save_folder.joinpath(f"{timestamp}_{diode}_solarsim.cal")

                if save_path.exists() is False:
                    with open(save_path, "w", newline="\n") as f:
                        f.writelines("voltage (v)\tcurrent (A)\ttime (s)\tstatus\n")
                    with open(save_path, "a", newline="\n") as f:
                        writer = csv.writer(f, delimiter="\t")
                        writer.writerows(data)
        except KeyError:
            mqttc.publish(
                "log",
                json.dumps(
                    {"kind": "warning", "data": "No EQE calibration data to save."}
                ),
            ).wait_for_publish()

        # save psu calibration
        try:
            for key, value in calibration["psu"]:
                diode = key
                timestamp = value["timestamp"]
                data = value["data"]

                save_path = save_folder.joinpath(f"{timestamp}_{diode}_psu.cal")

                if save_path.exists() is False:
                    with open(save_path, "w", newline="\n") as f:
                        f.writelines(
                            "voltage (v)\tcurrent (A)\ttime (s)\tstatus\tpsu_current (A)\n"
                        )
                    with open(save_path, "a", newline="\n") as f:
                        writer = csv.writer(f, delimiter="\t")
                        writer.writerows(data)
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


def save_args(payload):
    """Save arguments parsed to server run command.

    Parameters
    ----------
    args : dict
        Arguments parsed to server run command.
    """
    global folder

    args = payload["args"]

    folder = args["destination"]

    if folder is not None:
        save_folder = folder
    else:
        save_folder = pathlib.Path()

    save_path = save_folder.joinpath("run_args.yaml")

    with open(save_path, "w") as f:
        yaml.dump(args, f)


def update_folder(payload):
    """Update the save folder.

    Parameters
    ----------
    payload : dict
        Message payload.
    """
    global folder

    folder = pathlib.Path(payload["args"]["destination"])
    if folder.exists() is False:
        folder.mkdir()


def on_message(mqttc, obj, msg):
    """Act on an MQTT msg."""
    payload = json.loads(msg.payload)

    if (topic := msg.topic) == "data/raw":
        save_raw(payload)
    elif topic == "data/processed":
        save_processed(payload)
    elif topic == "data/calibration":
        if (measurement := payload["measurement"]) == "eqe_calibration":
            read_eqe_cal(payload)
        elif measurement == "psu_calibration":
            read_psu_cal(payload)
        elif measurement == "solarsim_diode":
            read_solarsim_diode_cal(payload)
        elif measurement == "spectrum_calibration":
            read_spactrum_cal(payload)
    elif topic == "measurement/request":
        if payload["action"] == "run":
            update_folder(payload)
            save_args(payload)
            save_config(payload)
            save_calibration()


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
    calibration = {}

    # create mqtt client id
    client_id = f"saver-{uuid.uuid4().hex}"

    mqttc = mqtt.Client(client_id)
    mqttc.on_message = on_message
    mqttc.connect(args.mqtthost)
    mqttc.subscribe("data/raw", qos=2)
    mqttc.subscribe("data/processed", qos=2)
    mqttc.subscribe("data/calibration", qos=2)
    mqttc.subscribe("measurement/request", qos=2)
    mqttc.loop_start()

    # get data folder name
    request(mqttc, "get_save_folder")

    # get latest config data from server
    request(mqttc, "get_config")

    # get latest calibration data from server
    request(mqttc, "get_calibration")

    mqttc.loop_forever()
