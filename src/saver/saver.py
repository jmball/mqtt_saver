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

try:
    from centralcontrol.put_ftp import put_ftp
except ImportError:
    pass

import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import yaml
import os
import sys

import argparse

class Saver:
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

    folder = None
    exp_timestamp = ""

    if 'centralcontrol' in sys.modules:
        ftp_support = True
    else:
        ftp_support = False

    def save_data(self, payload, kind, processed=False):
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

        if self.folder is not None:
            save_folder = self.folder
        else:
            save_folder = pathlib.Path()

        if processed is True:
            save_folder = save_folder.joinpath("processed")
            save_folder.mkdir(parents=True, exist_ok=True)
            file_prefix = "processed_"
        else:
            file_prefix = ""

        label = payload["pixel"]["label"]
        pixel = payload["pixel"]["pixel"]
        idn = f"{label}_device{pixel}"

        save_path = save_folder.joinpath(f"{file_prefix}{idn}_{self.exp_timestamp}.{exp}.tsv")

        # create file with header if pixel
        if save_path.exists() == False:
            print(f"New save path: {save_path}")
            # append file name for backup
            self.backup_q.put(save_path)
            with open(save_path, "w", newline="\n") as f:
                if exp == "eqe":
                    if processed is True:
                        f.writelines(self.eqe_processed_header)
                    else:
                        f.writelines(self.eqe_header)
                else:
                    if processed is True:
                        f.writelines(self.iv_processed_header)
                    else:
                        f.writelines(self.iv_header)

        if payload["data"] == []:
            print("EMPTY PAYLOAD")

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

        if save_path.exists() is False:
            with open(save_path, "w", newline="\n") as f:
                f.writelines(header)
            with open(save_path, "a", newline="\n") as f:
                writer = csv.writer(f, delimiter="\t")
                writer.writerows(data)

            # trigger FTP backup of cal file
            self.backup_q.put(save_path)
            self.run_complete.append(True)


    def save_run_settings(self, payload):
        """Save arguments parsed to server run command.

        Parameters
        ----------
        args : dict
            Arguments parsed to server run command.
        """

        self.folder = pathlib.Path(payload["args"]["run_name"])
        self.folder.mkdir(parents=True, exist_ok=True)

        self.exp_timestamp = payload["args"]["run_name_suffix"]

        run_args_path = self.folder.joinpath(f"run_args_{self.exp_timestamp}.yaml")
        config_path = self.folder.joinpath(f"measurement_config_{self.exp_timestamp}.yaml")

        # save the device selection dataframe(s)
        for key in payload["args"]["pixel_data_object_names"]:
            df = payload["args"][key]
            dfk = df[payload["args"]["pix_cols_to_save"]] #keep only the whitelisted cols
            save_path = self.folder.joinpath(f"{df.index.name}_pixel_setup_{self.exp_timestamp}.csv")

            # handle custom area overrides for the csv (if any)
            dfk['area'] = dfk['area'].replace(-1, payload["args"]["a_ovr_spin"])
            dfk['dark_area'] = dfk['dark_area'].replace(-1, payload["args"]["a_ovr_spin"])

            dfk.to_csv(save_path)
            # we've handled this data now, don't want to save it twice
            del payload["args"][key]
            self.backup_q.put(save_path)

        # save args
        with open(run_args_path, "w") as f:
            yaml.dump(payload["args"], f)
        self.backup_q.put(run_args_path)

        # save config
        with open(config_path, "w") as f:
            yaml.dump(payload["config"], f)
        self.backup_q.put(config_path)


    def ftp_backup(self, ftphost):
        """Backup files using FTP.

        Parameters
        ----------
        ftphost : str
            Full FTP server address and remote path for backup, e.g.
            'ftp://[hostname]/[path]/'.
        """
        while True:
            if self.run_complete[0] is True:
                # run has finished so backup all files left in the queue
                while self.backup_q.empty() is False:
                    self.send_backup_file(self.backup_q.get(), ftphost)
                    self.backup_q.task_done()

                # reset the run complete flag
                self.run_complete.append(False)
            elif self.backup_q.qsize() > 1:
                # there is at least one finished file to backup
                self.send_backup_file(self.backup_q.get(), ftphost)
                self.backup_q.task_done()
            else:
                time.sleep(1)


    def send_backup_file(self, source, dest):
        protocol, address = dest.split('://')
        host, dest_path = address.split('/', 1)
        ftphost = f"{protocol}://{host}"
        dest_folder = pathlib.PurePosixPath("/"+dest_path)
        dest_folder = (dest_folder / source.parent)
        with put_ftp(ftphost) as ftp:
            with open(source, 'rb') as fh:
                ftp.uploadFile(fh, remote_path=str(dest_folder) + '/')


    def on_message(self, mqttc, obj, msg):
        """Act on an MQTT msg."""
        self.save_queue.put_nowait(msg)


    def save_handler(self):
        """Handle cmds to saver."""
        while True:
            msg = self.save_queue.get()

            try:
                payload = pickle.loads(msg.payload)
                topic_list = msg.topic.split("/")

                if (topic := topic_list[0]) == "data":
                    if (subtopic0 := topic_list[1]) == "raw":
                        self.save_data(payload, msg.topic.replace("data/raw/", ""))
                    elif subtopic0 == "processed":
                        self.save_data(payload, msg.topic.replace("data/processed/", ""), True)
                elif topic == "calibration":
                    if topic_list[1] == "psu":
                        subtopic1 = topic_list[2]
                    else:
                        subtopic1 = None
                    self.save_calibration(payload, topic_list[1], subtopic1)
                elif msg.topic == "measurement/run":
                    self.save_run_settings(payload)
                    self.run_complete.append(False)
                elif msg.topic == "measurement/log":
                    if payload["msg"] == "Run complete!":
                        self.run_complete.append(True)
            except:
                pass

            self.save_queue.task_done()


    def main(self):

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

        # start save handler thread
        threading.Thread(target=self.save_handler, daemon=True).start()

        # start FTP backup thread if required
        if args.ftphost is not None:
            ftp_addr = args.ftphost
        else:
            ftp_addr = os.environ.get('SAVER_FTP')

        if (ftp_addr is not None) and (self.ftp_support == True):
            threading.Thread(target=self.ftp_backup, args=(ftp_addr,), daemon=True).start()
            print(f'FTP backup enabled: {ftp_addr}')

        # create mqtt client id
        client_id = f"saver-{uuid.uuid4().hex}"

        mqttc = mqtt.Client(client_id)
        mqttc.will_set("saver/status", pickle.dumps(f"{client_id} offline"), 2, retain=True)
        mqttc.on_message = self.on_message
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
        print(f"Saving to {os.getcwd()}")
        mqttc.loop_forever()

def run():
    s = Saver()
    s.main()

if __name__ == "__main__":
    run()
