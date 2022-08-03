"""Save data obtained from MQTT broker."""

import csv
import pathlib
import json
import queue
import threading
import time
import uuid
import pandas as pd
import hmac

import logging

# for logging directly to systemd journal if we can
try:
    import systemd.journal
except ImportError:
    pass

from datetime import datetime

try:
    from centralcontrol.put_ftp import put_ftp
except ImportError:
    pass

import paho.mqtt.client as mqtt
import yaml
import os
import sys

import argparse


class Saver(object):
    ftp_env_var = "SAVER_FTP"
    hk = "gosox".encode()

    def __init__(self, mqtt_host="127.0.0.1", ftp_uri=None):

        self.outq = queue.Queue()

        # setup logging
        logname = __name__
        if (__package__ is not None) and (__package__ in __name__):
            # log at the package level if the imports are all correct
            logname = __package__
        self.lg = logging.getLogger(logname)
        self.lg.setLevel(logging.DEBUG)

        if not self.lg.hasHandlers():
            # set up a logging handler for passing messages to the UI log window

            uih = logging.Handler()
            uih.setLevel(logging.INFO)
            uih.emit = self.send_log_msg
            self.lg.addHandler(uih)

            # set up logging to systemd's journal if it's there
            if "systemd" in sys.modules:
                sysdl = systemd.journal.JournalHandler(SYSLOG_IDENTIFIER=self.lg.name)
                sysLogFormat = logging.Formatter(("%(levelname)s|%(message)s"))
                sysdl.setFormatter(sysLogFormat)
                self.lg.addHandler(sysdl)
            else:
                # for logging to stdout & stderr
                ch = logging.StreamHandler()
                logFormat = logging.Formatter(("%(asctime)s|%(name)s|%(levelname)s|%(message)s"))
                ch.setFormatter(logFormat)
                self.lg.addHandler(ch)

        self.mqtt_host = mqtt_host
        self.ftp_uri = ftp_uri

        # make header strings
        eqe_header_items = [
            "timestamp (s)",
            "wavelength (nm)",
            "X (V)",
            "Y (V)",
            "Aux In 1 (V)",
            "Aux In 2 (V)",
            "Aux In 3 (V)",
            "Aux In 4 (V)",
            "R (V)",
            "Phase (deg)",
            "Freq (Hz)",
            "Ch1 display",
            "Ch2 display",
        ]
        self.eqe_header = "\t".join(eqe_header_items) + "\n"

        self.iv_header = "voltage (v)\tcurrent (A)\ttime (s)\tstatus\n"

        self.spectrum_cal_header = "wls (nm)\traw (counts)\n"

        self.psu_cal_header = self.iv_header[:-1] + "\tset_psu_current (A)\n"

        self.daq_header = "timestamp (s)\tT (degC)\tIntensity (V)\n"

        # event for when we should start processing the backup queue
        self.trigger_backup = threading.Event()

        # add incoming mqtt messages to a queue for worker thread
        self.save_queue = queue.SimpleQueue()

        self.folder = None
        self.exp_timestamp = None

        if "centralcontrol.put_ftp" in sys.modules:
            # queue latest file names for optional FTP backup in worker thread
            self.backup_q = queue.SimpleQueue()
        else:
            self.backup_q = None
            self.lg.debug("FTP backup support missing.")

        # create mqtt client id
        self.client_id = f"saver-{uuid.uuid4().hex}"

        # setup mqttclient and callbacks
        self.mqttc = mqtt.Client(self.client_id)
        self.mqttc.will_set("saver/status", json.dumps(f"{self.client_id} offline"), 2, retain=True)
        self.mqttc.on_message = self.on_message
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_disconnect = self.on_disconnect

    # send up a log message to the status channel
    def send_log_msg(self, record):
        payload = {"level": record.levelno, "msg": record.msg}
        self.outq.put({"topic": "measurement/log", "payload": json.dumps(payload), "qos": 2})

    def save_data(self, payload, kind):
        """Save data to text file.

        Parameters
        ----------
        payload : str
            MQTT message payload.
        kind : str
            Measurement kind, e.g. eqe_measurement etc.
        """
        # create file extension, adding prefix for type of iv measurement if applicable
        if kind.startswith("iv_measurement"):
            exp_prefix = payload["sweep"][0]
            kind = kind.split("/")[0]
        else:
            exp_prefix = ""
        exp = f"{exp_prefix}{kind.replace('_measurement', '')}"

        # handle missing timestamp
        if self.exp_timestamp is None:
            self.exp_timestamp = time.time()  # since we didn't get one, we'll make our own epoch
            self.lg.warning("New data with no epoch has appeared")
            self.lg.warning("Possibly because of a missed run start message")
            self.lg.warning(f"Using new epoch: {self.exp_timestamp}")

        # deal with possible save folder issues
        if self.folder is None:
            self.folder = pathlib.Path().joinpath(str(self.exp_timestamp))
            self.lg.warning("Target data folder unconfigured")
            self.lg.warning("Possibly because of a missed run start message")
            self.lg.warning(f"New target folder configured: {self.folder}")

        if self.folder.exists() == False:
            self.lg.warning("Target data folder does not exist: {self.folder}")
            self.lg.warning("That could mean the data folder was disappeared mid-run or it wasn't created properly on run start")
            self.lg.warning("Regenerating that folder now")
            self.folder.mkdir(parents=True, exist_ok=False)

        save_folder = self.folder

        file_prefix = ""

        # get device information for path if applicable
        try:
            if exp == "daq":  # daq type doesn't have pixel data
                idn = "daq"
            else:
                idn = f'{payload["pixel"]["label"]}_device{payload["pixel"]["pixel"]}'
        except Exception as e:
            idn = "unknown_deviceX"
            self.lg.debug(f"Payload parse error: {e}")
            self.lg.debug(f"Using {idn=}")

        # define a format to use for the file name
        save_path_format = "{file_prefix}{idn}_{timestamp}.{exp}.tsv"

        # automatically increment iv scan extension
        if exp.startswith("liv") or exp.startswith("div"):
            i = 1
            while True:
                test_exp = f"{exp}{i}"

                test_name = save_path_format.format(file_prefix=file_prefix, idn=idn, timestamp=self.exp_timestamp, exp=test_exp)

                if save_folder.joinpath(test_name).exists():
                    i = i + 1
                else:
                    exp = test_exp
                    break

        # build save path
        save_path = save_folder.joinpath(save_path_format.format(file_prefix=file_prefix, idn=idn, timestamp=self.exp_timestamp, exp=exp))

        new_file = not save_path.exists()

        # create the data file with just a header row
        if new_file:
            with open(save_path, "x", newline="\n") as f:
                if exp == "eqe":
                    f.writelines(self.eqe_header)
                elif exp == "daq":
                    f.writelines(self.daq_header)
                else:
                    f.writelines(self.iv_header)

        if payload["data"] == []:
            self.lg.debug("EMPTY PAYLOAD")

        # append the data to file
        with open(save_path, "a", newline="\n") as f:
            writer = csv.writer(f, delimiter="\t")
            if exp.startswith("liv") or exp.startswith("div"):
                writer.writerows(payload["data"])
                single_row = False
            else:
                writer.writerow(payload["data"][0])
                single_row = True

        if (new_file) and (self.ftp_uri is not None):
            self.backup_q.put(save_path)  # append file name for backup
            if (single_row == True) and self.trigger_backup.is_set():
                self.lg.warning(f"It's possible an unfinished file was added to the backup queue during active backup task: {save_path}")

    def save_calibration(self, payload, kind, extra=None):
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
        save_folder.mkdir(exist_ok=True, parents=True)

        # format timestamp into something human readable including
        timestamp = payload["timestamp"]
        # local timezone
        timezone = datetime.now().astimezone().tzinfo
        fmt = "%Y-%m-%d_%H-%M-%S_%z"
        human_timestamp = datetime.fromtimestamp(timestamp, tz=timezone).strftime(f"{fmt}")

        data = payload["data"]

        good_kind = False
        if kind == "eqe":
            good_kind = True
            idn = payload["diode"]
            save_path = save_folder.joinpath(f"{human_timestamp}_{idn}.{kind}.cal.tsv")
            header = self.eqe_header
        elif kind == "spectrum":
            good_kind = True
            save_path = save_folder.joinpath(f"{human_timestamp}.{kind}.cal.tsv")
            header = self.spectrum_cal_header
        elif kind == "solarsim_diode":
            good_kind = True
            idn = payload["diode"]
            save_path = save_folder.joinpath(f"{human_timestamp}_{idn}.ss.cal.tsv")
            header = self.iv_header
        elif kind == "rtd":
            good_kind = True
            idn = payload["diode"]
            save_path = save_folder.joinpath(f"{human_timestamp}_{idn}.{kind}.cal.tsv")
            header = self.iv_header
        elif kind == "psu":
            good_kind = True
            idn = payload["diode"]
            save_path = save_folder.joinpath(f"{human_timestamp}_{idn}_{extra}.{kind}.cal.tsv")
            header = self.psu_cal_header

        if good_kind == True:
            if save_path.exists() == False:
                with open(save_path, "x", newline="\n") as f:
                    f.writelines(header)
                with open(save_path, "a", newline="\n") as f:
                    writer = csv.writer(f, delimiter="\t")
                    writer.writerows(data)
                if self.ftp_uri is not None:
                    # trigger FTP backup of cal file
                    self.backup_q.put(save_path)
            else:
                self.lg.debug(f"Not saving cal data because a file for it already exists: {save_path=}")
        else:
            self.lg.debug(f"Not saving cal data because we don't understand its kind: {kind=}")

    def save_run_settings(self, payload):
        """Save arguments parsed to server run command.

        Parameters
        ----------
        args : dict
            Arguments parsed to server run command.
        """

        run_folder = payload["args"]["run_name"]
        self.folder = pathlib.Path(run_folder)

        if self.folder.exists():
            self.lg.warning(f"Attempt to remake pre-existing run folder: {self.folder.resolve()}")
            self.lg.warning(f"Falling back to timestamp-named folder to prevent overwriting data.")
            new_run_folder = f"{time.time()}_{run_folder}"
            self.folder = pathlib.Path(new_run_folder)

        self.folder.mkdir(parents=True, exist_ok=False)

        try:
            save_path_str = str(self.folder.resolve())
        except Exception as e:
            save_path_str = str(self.folder)

        self.lg.info(f"Saver will save this run data into {save_path_str}")

        self.exp_timestamp = payload["args"]["run_name_suffix"]

        run_args_path = self.folder.joinpath(f"run_args_{self.exp_timestamp}.yaml")
        config_path = self.folder.joinpath(f"measurement_config_{self.exp_timestamp}.yaml")

        # save the device selection dataframe(s) if provided
        if "pixel_data_object_names" in payload["args"]:
            for key in payload["args"]["pixel_data_object_names"]:
                df = pd.DataFrame.from_dict(payload["args"][key])
                if "IV" in key:
                    name = "IV_"
                elif "EQE" in key:
                    name = "EQE_"
                else:
                    name = ""

                # keep only the whitelisted cols
                dfk = df.loc[:, payload["args"]["pix_cols_to_save"]]

                save_path = self.folder.joinpath(f"{name}pixel_setup_{self.exp_timestamp}.csv")

                # handle custom area overrides for the csv (if any)
                dfk.replace({"area": -1, "dark_area": -1}, payload["args"]["a_ovr_spin"], inplace=True)

                dfk.to_csv(save_path, mode="x")
                # we've handled this data now, don't want to save it twice
                del payload["args"][key]
                if self.ftp_uri is not None:
                    self.backup_q.put(save_path)

        # save args
        with open(run_args_path, "x") as f:
            yaml.dump(payload["args"], f)
        if self.ftp_uri is not None:
            self.backup_q.put(run_args_path)

        # save config
        with open(config_path, "x") as f:
            yaml.dump(payload["config"], f)
        if self.ftp_uri is not None:
            self.backup_q.put(config_path)

    def ftp_backup(self, ftp_uri):
        """Backup files using FTP.

        Parameters
        ----------
        ftphost : str
            Full FTP server address and remote path for backup, e.g.
            'ftp://[hostname]/[path]/'.
        """
        while True:
            self.trigger_backup.wait()  # wait for backup trigger
            # run has finished so backup all files left in the queue
            while not self.backup_q.empty():
                file_to_send = self.backup_q.get()
                if file_to_send.exists():  # handle case when file to backup might have disappeared
                    try:
                        self.send_backup_file(file_to_send, ftp_uri)
                    except Exception as e:
                        self.lg.debug(e)
                        self.lg.warning(f"Data backup failure. Retrying...")
                        self.backup_q.put(file_to_send)  # requeue it for later
                        time.sleep(1)  # don't spam backup tries
                else:
                    self.lg.warning(f"{file_to_send} does not exist!")

            self.lg.debug("FTP backup complete.")
            self.trigger_backup.clear()  # reset the backup trigger flag

    def send_backup_file(self, source, dest):
        protocol, address = dest.split("://")
        host, dest_path = address.split("/", 1)
        ftphost = f"{protocol}://{host}"
        dest_folder = pathlib.PurePosixPath("/" + dest_path)
        dest_folder = dest_folder / source.parent
        with put_ftp(ftphost) as ftp:
            with open(source, "rb") as fh:
                ftp.uploadFile(fh, remote_path=str(dest_folder) + "/")

    def on_message(self, mqttc, obj, msg):
        """Act on an MQTT msg."""
        self.save_queue.put_nowait(msg)

    def save_handler(self):
        """Handle cmds to saver."""
        self.lg.debug(f"Saving to {os.getcwd()}")
        while True:
            msg = self.save_queue.get()

            try:
                payload = json.loads(msg.payload.decode())
                topic_list = msg.topic.split("/")
                topic = topic_list[0]

                if topic == "data":
                    subtopic0 = topic_list[1]
                    if subtopic0 == "raw":
                        self.save_data(payload, msg.topic.replace("data/raw/", ""))
                    else:
                        self.lg.debug(f"Saver not acting on data subtopic: {subtopic0}")
                elif topic == "calibration":
                    subtopic0 = topic_list[1]
                    if subtopic0 == "psu":
                        subtopic1 = topic_list[2]
                    else:
                        subtopic1 = None
                    self.save_calibration(payload, subtopic0, subtopic1)
                elif msg.topic == "measurement/run":
                    if "rundata" in payload:
                        rundata = payload["rundata"]
                        remotedigest = bytes.fromhex(rundata.pop("digest").removeprefix("0x"))
                        jrundatab = json.dumps(rundata).encode()
                        localdigest = hmac.digest(self.hk, jrundatab, "sha1")
                        if remotedigest != localdigest:
                            self.lg.warning(f"Malformed run data.")
                        else:
                            self.save_run_settings(rundata)
                    else:
                        self.save_run_settings(payload)
                elif msg.topic == "measurement/log":
                    if (payload["msg"] == "Run complete!") and (self.ftp_uri is not None):
                        self.lg.info(f"Saver noticed a run completion. Triggering a backup task.")
                        self.trigger_backup.set()
                else:
                    self.lg.debug(f"Saver not acting on topic: {msg.topic}")
            except Exception as e:
                self.lg.warning(f"Data save issue: {e}")

    def mqtt_connector(self, mqttc):
        while True:
            mqttc.connect(self.mqtt_host)
            mqttc.loop_forever(retry_first_connection=True)

    # relays outgoing messages
    def out_relay(self):
        while True:
            to_send = self.outq.get()
            self.mqttc.publish(**to_send)

    def on_connect(self, client, userdata, flags, rc):
        self.lg.debug(f"{self.client_id} connected to broker with result code {rc}")
        self.mqttc.subscribe("data/#", qos=2)
        self.mqttc.subscribe("calibration/#", qos=2)
        self.mqttc.subscribe("measurement/#", qos=2)
        self.mqttc.publish("saver/status", json.dumps(f"{self.client_id} ready"), qos=2)

    def on_disconnect(self, client, userdata, rc):
        self.lg.debug(f"{self.client_id} disconnected from broker with result code {rc}")

    def run(self):
        # start the mqtt connector thread
        threading.Thread(target=self.mqtt_connector, args=(self.mqttc,), daemon=True).start()

        # start FTP backup thread if required
        if (self.ftp_uri is not None) and (self.backup_q is not None):
            threading.Thread(target=self.ftp_backup, args=(self.ftp_uri,), daemon=True).start()
            self.lg.debug(f"FTP backup active to {self.ftp_uri}")
        else:
            self.lg.debug("FTP backup not in use")

        # start output relay
        threading.Thread(target=self.out_relay, daemon=True).start()

        # start save handler
        self.save_handler()


def main():
    parser = argparse.ArgumentParser(description="MQTT Saver")
    parser.add_argument("--mqtt-host", type=str, nargs="?", default="127.0.0.1", const="127.0.0.1", help="IP address or hostname for MQTT broker.")
    parser.add_argument("--ftp-uri", type=str, help="Full FTP server address and remote path for backup, e.g. ftp://[hostname]/[path]/")

    args = parser.parse_args()
    ftp_uri_env_var_name = Saver.ftp_env_var
    if args.ftp_uri is not None:
        ftp_uri = args.ftp_uri
    elif ftp_uri_env_var_name in os.environ:
        ftp_uri = os.environ.get(ftp_uri_env_var_name)
    else:
        ftp_uri = None

    s = Saver(mqtt_host=args.mqtt_host, ftp_uri=ftp_uri)
    s.run()


if __name__ == "__main__":
    main()
