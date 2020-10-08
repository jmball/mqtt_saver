"""Save data obtained from MQTT broker."""

import collections
import csv
import pathlib
import pickle
import queue
import threading
import time
import uuid

from datetime import datetime

from central_control.put_ftp import put_ftp

import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import yaml


# make header strings
eqe_header = (
    "timestamp (s)\twavelength (nm)\tX (V)\tY (V)\tAux In 1 (V)\tAux"
    + " In 2 (V)\tAux In 3 (V)\tAux In 4 (V)\tR (V)\tPhase (deg)\tFreq"
    + " (Hz)\tCh1 display\tCh2 display\n"
)
eqe_processed_header = eqe_header[:-1] + "\tEQE\n"
iv_header = "voltage (v)\tcurrent (A)\ttime (s)\tstatus\n"
iv_processed_header = (
    iv_header[:-1] + "\tcurrent_density (mA/cm^2)\tpower_density (mW/cm^2)\n"
)
spectrum_cal_header = "wls (nm)\traw (counts)\n"
psu_cal_header = iv_header[:-1] + "\tset_psu_current (A)\n"

# queue latest file names for optional FTP backup in worker thread
backup_q = queue.Queue()

# flag whether a run is complete (use deque for thread-safety)
run_complete = collections.deque(maxlen=1)
run_complete.append(False)

# add incoming mqtt messages to a queue for worker thread
save_queue = queue.Queue()


def save_data(payload, kind, processed=False):
    """Save data to text file.

    Parameters
    ----------
    payload : str
        MQTT message payload.
    processed : bool
        Flag for highlighting when data has been processed.
    """
    if kind.startswith("iv_measurement"):
        exp_prefix = payload["sweep"][0]
        exp_suffix = kind[-1]
        kind = kind.split("/")[0]
    else:
        exp_prefix = ""
        exp_suffix = ""

    exp = f"{exp_prefix}{kind.replace('_measurement', '')}{exp_suffix}"

    if folder is not None:
        save_folder = folder
    else:
        save_folder = pathlib.Path()

    if processed is True:
        save_folder = save_folder.joinpath("processed")
        file_prefix = "processed_"
        if save_folder.exists() is False:
            save_folder.mkdir()
    else:
        file_prefix = ""

    label = payload["pixel"]["label"]
    pixel = payload["pixel"]["pixel"]
    idn = f"{label}_device{pixel}"

    save_path = save_folder.joinpath(f"{file_prefix}{idn}_{exp_timestamp}.{exp}.tsv")

    # create file with header if pixel
    if save_path.exists() is False:
        print(f"New save path: {save_path}")
        # append file name for backup
        backup_q.put(save_path)
        with open(save_path, "w", newline="\n") as f:
            if exp == "eqe":
                if processed is True:
                    f.writelines(eqe_processed_header)
                else:
                    f.writelines(eqe_header)
            else:
                if processed is True:
                    f.writelines(iv_processed_header)
                else:
                    f.writelines(iv_header)

    if payload["data"] == []:
        print("EMPTY PAYLOAD")

    # append data to file
    with open(save_path, "a", newline="\n") as f:
        writer = csv.writer(f, delimiter="\t")
        if exp.startswith("liv") or exp.startswith("div"):
            writer.writerows(payload["data"])
        else:
            writer.writerow(payload["data"])


def save_calibration(payload, kind, extra=None):
    """Save calibration data.

    Parameters
    ----------
    mqttc : mqtt.Client
        MQTT save client.
    kind : str
        Kind of calibration data.
    extra : str
        Extra information about the calibration type added to the filename.
    """
    save_folder = pathlib.Path("calibration")
    if save_folder.exists() is False:
        save_folder.mkdir()

    # format timestamp into something human readable including
    timestamp = payload["timestamp"]
    # local timezone
    timezone = datetime.now().astimezone().tzinfo
    fmt = "%Y-%m-%d_%H-%M-%S_%z"
    human_timestamp = datetime.fromtimestamp(timestamp, tz=timezone).strftime(f"{fmt}")

    data = payload["data"]

    if kind == "eqe":
        idn = payload["diode"]
        save_path = save_folder.joinpath(f"{human_timestamp}_{idn}.{kind}.cal.tsv")
        header = eqe_header
    elif kind == "spectrum":
        save_path = save_folder.joinpath(f"{human_timestamp}.{kind}.cal.tsv")
        header = spectrum_cal_header
    elif kind == "solarsim_diode":
        idn = payload["diode"]
        save_path = save_folder.joinpath(f"{human_timestamp}_{idn}.ss.cal.tsv")
        header = iv_header
    elif kind == "rtd":
        idn = payload["diode"]
        save_path = save_folder.joinpath(f"{human_timestamp}_{idn}.{kind}.cal.tsv")
        header = iv_header
    elif kind == "psu":
        idn = payload["diode"]
        save_path = save_folder.joinpath(
            f"{human_timestamp}_{idn}_{extra}.{kind}.cal.tsv"
        )
        header = psu_cal_header

    if save_path.exists() is False:
        with open(save_path, "w", newline="\n") as f:
            f.writelines(header)
        with open(save_path, "a", newline="\n") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerows(data)

        # trigger FTP backup of cal file
        backup_q.put(save_path)
        run_complete.append(True)


def save_run_settings(payload):
    """Save arguments parsed to server run command.

    Parameters
    ----------
    args : dict
        Arguments parsed to server run command.
    """
    global folder
    global exp_timestamp

    folder = pathlib.Path(payload["args"]["run_name"])
    if folder.exists() is False:
        folder.mkdir()

    exp_timestamp = pathlib.PurePath(folder).parts[-1][-10:]

    run_args_path = folder.joinpath(f"run_args_{exp_timestamp}.yaml")
    config_path = folder.joinpath(f"measurement_config_{exp_timestamp}.yaml")

    # save args
    with open(run_args_path, "w") as f:
        yaml.dump(payload["args"], f)

    # save config
    with open(config_path, "w") as f:
        yaml.dump(payload["config"], f)

    # add files to FTP backup queue
    backup_q.put(run_args_path)
    backup_q.put(config_path)


def ftp_backup(ftphost):
    """Backup files using FTP.

    Parameters
    ----------
    ftphost : str
        Full FTP server address and remote path for backup, e.g.
        'ftp://[hostname]/[path]/'.
    """
    dest_folder = pathlib.PurePurePosixPath("/dump")  # folder in server to upload to
    while True:
        if run_complete[0] is True:
            # run has finished so backup all files left in the queue
            while backup_q.empty() is False:
                file = backup_q.get()

                with put_ftp(ftphost) as ftp:
                    with open(file, 'rb') as fh:
                        ftp.uploadFile(fh, remote_path=str(dest_folder / PurePurePosixPath(file.parent) +'/')

                backup_q.task_done()

            # reset the run complete flag
            run_complete.append(False)
        elif backup_q.qsize() > 1:
            # there is at least one finished file to backup
            file = backup_q.get()

            with put_ftp(ftphost) as ftp:
                with open(file, 'rb') as fh:
                    ftp.uploadFile(fh, remote_path=str(dest_folder / PurePurePosixPath(file.parent) +'/')

            backup_q.task_done()
        else:
            time.sleep(1)


def on_message(mqttc, obj, msg):
    """Act on an MQTT msg."""
    save_queue.put_nowait(msg)


def save_handler():
    """Handle cmds to saver."""
    while True:
        msg = save_queue.get()

        try:
            payload = pickle.loads(msg.payload)
            topic_list = msg.topic.split("/")

            if (topic := topic_list[0]) == "data":
                if (subtopic0 := topic_list[1]) == "raw":
                    save_data(payload, msg.topic.replace("data/raw/", ""))
                elif subtopic0 == "processed":
                    save_data(payload, msg.topic.replace("data/processed/", ""), True)
            elif topic == "calibration":
                if topic_list[1] == "psu":
                    subtopic1 = topic_list[2]
                else:
                    subtopic1 = None
                save_calibration(payload, topic_list[1], subtopic1)
            elif msg.topic == "measurement/run":
                save_run_settings(payload)
                run_complete.append(False)
            elif msg.topic == "measurement/log":
                if payload["msg"] == "Run complete!":
                    run_complete.append(True)
        except:
            pass

        save_queue.task_done()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-mqtthost",
        type=str,
        default="127.0.0.1",
        help="IP address or hostname for MQTT broker.",
    )
    parser.add_argument(
        "--ftphost",
        type=str,
        help="Full FTP server address and remote path for backup, e.g. ftp://[hostname]/[path]/",
    )

    args = parser.parse_args()

    # init global variables
    folder = None
    exp_timestamp = ""

    # start save handler thread
    threading.Thread(target=save_handler, daemon=True).start()

    # start FTP backup thread if required
    if args.ftphost is not None:
        threading.Thread(target=ftp_backup, args=(args.ftphost,), daemon=True).start()

    # create mqtt client id
    client_id = f"saver-{uuid.uuid4().hex}"

    mqttc = mqtt.Client(client_id)
    mqttc.will_set("saver/status", pickle.dumps(f"{client_id} offline"), 2, retain=True)
    mqttc.on_message = on_message
    mqttc.connect(args.mqtthost)
    mqttc.subscribe("data/#", qos=2)
    mqttc.subscribe("calibration/#", qos=2)
    mqttc.subscribe("measurement/#", qos=2)
    publish.single(
        "saver/status",
        pickle.dumps(f"{client_id} ready"),
        qos=2,
        hostname=args.mqtthost,
    )
    print(f"{client_id} connected!")
    mqttc.loop_forever()
