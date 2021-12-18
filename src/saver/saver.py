"""Save data obtained from MQTT broker."""

import collections
import csv
import pathlib
import pickle
import queue
import threading
import queue
import time
import uuid

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
        self.eqe_processed_header = self.eqe_header[:-1] + "\tEQE\n"

        self.iv_header = "voltage (v)\tcurrent (A)\ttime (s)\tstatus\n"
        self.iv_processed_header = self.iv_header[:-1] + "\tcurrent_density (mA/cm^2)\tpower_density (mW/cm^2)\n"

        self.spectrum_cal_header = "wls (nm)\traw (counts)\n"

        self.psu_cal_header = self.iv_header[:-1] + "\tset_psu_current (A)\n"

        self.daq_header = "timestamp (s)\tT (degC)\tIntensity (V)\n"

        # flag whether a run is complete (use deque for thread-safety)
        self.run_complete = collections.deque(maxlen=1)
        self.run_complete.append(False)

        # add incoming mqtt messages to a queue for worker thread
        self.save_queue = queue.Queue()

        self.folder = None
        self.exp_timestamp = None

        if "centralcontrol.put_ftp" in sys.modules:
            # queue latest file names for optional FTP backup in worker thread
            self.backup_q = queue.Queue()
        else:
            self.backup_q = None
            self.lg.debug("FTP backup support missing.")

        # create mqtt client id
        self.client_id = f"saver-{uuid.uuid4().hex}"

        self.mqttc = mqtt.Client(self.client_id)
        self.mqttc.will_set("saver/status", pickle.dumps(f"{self.client_id} offline"), 2, retain=True)
        self.mqttc.on_message = self.on_message
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_disconnect = self.on_disconnect

    # send up a log message to the status channel
    def send_log_msg(self, record):
        payload = {"level": record.levelno, "msg": record.msg}
        self.outq.put({"topic": "measurement/log", "payload": pickle.dumps(payload), "qos": 2})

    def save_data(self, payload, kind, processed=False):
        """Save data to text file.

        Parameters
        ----------
        payload : str
            MQTT message payload.
        kind : str
            Measurement kind, e.g. eqe_measurement etc.
        processed : bool
            Flag for highlighting when data has been processed.
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
            raise ValueError("Unable to complete save since there's no epoch. Possibly missed the run start message.")

        # get master save folder
        if self.folder is not None:
            save_folder = self.folder
        else:
            save_folder = pathlib.Path()

        if save_folder.exists() == False:
            self.lg.warning(f"We got new run data, but there's no existing run folder for it!")
            self.lg.warning(f"That could mean the data folder was deleted mid-run or the saver missed the run start message.")
            self.lg.warning(f"Saving into: {save_folder}")
            save_folder.mkdir(parents=True, exist_ok=True)

        # append processed sub folder to path if applicable
        if processed == True:
            save_folder = save_folder.joinpath("processed")
            save_folder.mkdir(parents=True, exist_ok=True)
            file_prefix = "processed_"
        else:
            file_prefix = ""

        # get device information for path if applicable
        try:
            label = payload["pixel"]["label"]
            pixel = payload["pixel"]["pixel"]
            idn = f"{label}_device{pixel}"
        except Exception as e:
            idn = "unknown_deviceX"
            self.lg.debug(f"Payload parse error: {e}")
            self.lg.debug(f"Using {idn=}")

        # automatically increment iv scan extension
        if exp.startswith("liv") or exp.startswith("div"):
            i = 1
            while True:
                test_exp = exp + str(i)

                test_path = save_folder.joinpath(f"{file_prefix}{idn}_{self.exp_timestamp}.{test_exp}.tsv")

                if test_path.exists() == False:
                    exp = test_exp
                    break
                else:
                    i += 1

        # build save path
        save_path = save_folder.joinpath(f"{file_prefix}{idn}_{self.exp_timestamp}.{exp}.tsv")

        # create file with header if pixel
        if save_path.exists() == False:
            self.lg.debug(f"New save path: {save_path}")
            if self.ftp_uri is not None:
                # append file name for backup
                self.backup_q.put(save_path)
            with open(save_path, "x", newline="\n") as f:
                if exp == "eqe":
                    if processed == True:
                        f.writelines(self.eqe_processed_header)
                    else:
                        f.writelines(self.eqe_header)
                elif exp == "daq":
                    f.writelines(self.daq_header)
                else:
                    if processed == True:
                        f.writelines(self.iv_processed_header)
                    else:
                        f.writelines(self.iv_header)

        if payload["data"] == []:
            self.lg.debug("EMPTY PAYLOAD")

        # append data to file
        with open(save_path, "a", newline="\n") as f:
            writer = csv.writer(f, delimiter="\t")
            if exp.startswith("liv") or exp.startswith("div"):
                writer.writerows(payload["data"])
            else:
                writer.writerow(payload["data"])

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

        header = ""
        save_path = pathlib.Path("/does/not/exist")
        if kind == "eqe":
            idn = payload["diode"]
            save_path = save_folder.joinpath(f"{human_timestamp}_{idn}.{kind}.cal.tsv")
            header = self.eqe_header
        elif kind == "spectrum":
            save_path = save_folder.joinpath(f"{human_timestamp}.{kind}.cal.tsv")
            header = self.spectrum_cal_header
        elif kind == "solarsim_diode":
            idn = payload["diode"]
            save_path = save_folder.joinpath(f"{human_timestamp}_{idn}.ss.cal.tsv")
            header = self.iv_header
        elif kind == "rtd":
            idn = payload["diode"]
            save_path = save_folder.joinpath(f"{human_timestamp}_{idn}.{kind}.cal.tsv")
            header = self.iv_header
        elif kind == "psu":
            idn = payload["diode"]
            save_path = save_folder.joinpath(f"{human_timestamp}_{idn}_{extra}.{kind}.cal.tsv")
            header = self.psu_cal_header

        if save_path.exists() == False:
            with open(save_path, "x", newline="\n") as f:
                f.writelines(header)
            with open(save_path, "a", newline="\n") as f:
                writer = csv.writer(f, delimiter="\t")
                writer.writerows(data)

            # trigger FTP backup of cal file
            if self.ftp_uri is not None:
                self.backup_q.put(save_path)
            # self.run_complete.append(True)

    def save_run_settings(self, payload):
        """Save arguments parsed to server run command.

        Parameters
        ----------
        args : dict
            Arguments parsed to server run command.
        """
        self.folder = pathlib.Path(payload["args"]["run_name"])
        self.folder.mkdir(parents=True, exist_ok=False)

        self.exp_timestamp = payload["args"]["run_name_suffix"]

        run_args_path = self.folder.joinpath(f"run_args_{self.exp_timestamp}.yaml")
        config_path = self.folder.joinpath(f"measurement_config_{self.exp_timestamp}.yaml")

        # save the device selection dataframe(s) if provided
        if "pixel_data_object_names" in payload["args"]:
            for key in payload["args"]["pixel_data_object_names"]:
                df = payload["args"][key]
                name = df.index.name

                # keep only the whitelisted cols
                dfk = df.loc[:, payload["args"]["pix_cols_to_save"]]

                save_path = self.folder.joinpath(f"{name}_pixel_setup_{self.exp_timestamp}.csv")

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
            if self.run_complete[0] == True:
                # run has finished so backup all files left in the queue
                while not self.backup_q.empty():
                    file_to_send = self.backup_q.get()
                    try:
                        self.send_backup_file(file_to_send, ftp_uri)
                    except Exception as e:
                        self.lg.debug(e)
                        self.lg.warning(f"Temporary data backup failure. Retrying...")
                        self.backup_q.put(file_to_send)  # requeue it for later
                        time.sleep(1)  # don't spam backup tries

                self.lg.debug("FTP backup complete.")
                self.run_complete.append(False)  # reset the run complete flag
            # elif self.backup_q.qsize() > 1:
            #     # there is at least one finished file to backup
            #     self.send_backup_file(self.backup_q.get(), ftp_uri)
            #     self.backup_q.task_done()
            else:
                time.sleep(1)  # don't spam run complete check

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
                payload = pickle.loads(msg.payload)
                topic_list = msg.topic.split("/")
                topic = topic_list[0]

                if topic == "data":
                    subtopic0 = topic_list[1]
                    if subtopic0 == "raw":
                        self.save_data(payload, msg.topic.replace("data/raw/", ""))
                    elif subtopic0 == "processed":
                        self.save_data(payload, msg.topic.replace("data/processed/", ""), True)
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
                    self.save_run_settings(payload)
                    # self.run_complete.append(False)
                elif msg.topic == "measurement/log":
                    if payload["msg"] == "Run complete!":
                        self.exp_timestamp = None  # reset the run timestamp
                        self.folder = None  # reset the save folder
                        self.run_complete.append(True)
                else:
                    self.lg.debug(f"Saver not acting on topic: {msg.topic}")
            except Exception as e:
                self.lg.warning(f"Data save issue: {e}")

            self.save_queue.task_done()

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
        self.mqttc.publish("saver/status", pickle.dumps(f"{self.client_id} ready"), qos=2)

    def on_disconnect(self, client, userdata, rc):
        print(f"{self.client_id} disconnected from broker with result code {rc}")

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
