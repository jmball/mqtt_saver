import paho.mqtt.client as mqtt
import pickle
import time

timestamp = time.time()

test_args = {
    "ad_switch": True,
    "chan1": 0.0,
    "chan1_ma": 0.0,
    "chan2": 0.0,
    "chan2_ma": 0.0,
    "chan3": 0.0,
    "chan3_ma": 0.0,
    "eqe_bias": 0.0,
    "eqe_devs": "0x00000003FFC0",
    "eqe_end": 1100.0,
    "eqe_int": 10,
    "eqe_selections": [
        "sb1",
        "sb2",
        "sb3",
        "sb4",
        "sb5",
        "sb6",
        "sc1",
        "sc2",
        "sc3",
        "sc4",
        "sc5",
        "sc6",
    ],
    "eqe_start": 300.0,
    "eqe_step": 100.0,
    "eqe_subs_dev_nums": [1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6],
    "eqe_subs_labels": [
        "bad devices",
        "bad devices",
        "bad devices",
        "bad devices",
        "bad devices",
        "bad devices",
        "C",
        "C",
        "C",
        "C",
        "C",
        "C",
    ],
    "eqe_subs_names": ["B", "B", "B", "B", "B", "B", "C", "C", "C", "C", "C", "C"],
    "goto_x": 62.5,
    "goto_y": 0.0,
    "goto_z": 0.0,
    "i_dwell": 3.0,
    "i_dwell_check": True,
    "i_dwell_value": 0.0,
    "i_dwell_value_ma": 0.0,
    "iv_devs": "0x0000000000FF",
    "iv_selections": [
        "sa1",
        "sa2",
        "sa3",
        "sa4",
        "sa5",
        "sa6",
        "sb1",
        "sb2",
        "sb3",
        "sb4",
        "sb5",
        "sb6",
    ],
    "iv_steps": 101.0,
    "iv_subs_dev_nums": [1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6],
    "iv_subs_labels": [
        "A",
        "A",
        "A",
        "A",
        "A",
        "A",
        "bad devices",
        "bad devices",
        "bad devices",
        "bad devices",
        "bad devices",
        "bad devices",
    ],
    "iv_subs_names": ["A", "A", "A", "A", "A", "A", "B", "B", "B", "B", "B", "B"],
    "label_tree": ["A", "bad devices", "C", "D", "E", "F", "G", "H"],
    "light_recipe": "AM1.5_1.0SUN",
    "lit_sweep": 0,
    "mppt_check": True,
    "mppt_dwell": 10.0,
    "mppt_params": "basic://",
    "nplc": 1.0,
    "return_switch": True,
    "run_name": "test_1595938312",
    "run_name_prefix": "test_",
    "run_name_suffix": "1595938312",
    "smart_mode": False,
    "source_delay": 3.0,
    "subs_names": ["A", "B", "C", "D", "E", "F", "G", "H"],
    "sweep_check": True,
    "sweep_end": -0.2,
    "sweep_start": 1.2,
    "v_dwell": 3.0,
    "v_dwell_check": True,
    "v_dwell_value": 0.0,
}

# config = load_config_from_file()
test_config = {
    "controller": {"address": "127.0.0.1"},
    "ivt": {"percent_beyond_voc": 25, "voltage_beyond_isc": 0.1},
    "lia": {
        "address": "127.0.0.1",
        "baud": 9600,
        "output_interface": 0,
        "terminator": "\\r",
    },
    "monochromator": {
        "address": "127.0.0.1",
        "baud": 9600,
        "filter_change_wls": [370, 640, 715, 765],
        "grating_change_wls": [1200],
        "terminator": "\\r",
    },
    "network": {"archive": "ftp://test:21/drop", "live_data_uri": "https://google.com"},
    "psu": {
        "address": "127.0.0.1",
        "baud": 9600,
        "calibration": {"current_step": 0.1, "max_current": 1},
        "ch1_voltage": 30,
        "ch2_voltage": 30,
        "ch3_voltage": 5,
        "terminator": "\\r",
    },
    "reference": {
        "calibration": {
            "eqe": {
                "eqe": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                "wls": [
                    350,
                    400,
                    450,
                    500,
                    550,
                    600,
                    650,
                    700,
                    750,
                    800,
                    850,
                    900,
                    950,
                    1000,
                    1050,
                    1100,
                ],
            }
        },
        "spectra": {"AM1.5G": {"irr": [1, 1, 1], "wls": [0, 1, 2]}},
    },
    "smu": {
        "address": "127.0.0.1",
        "baud": 57600,
        "front_terminals": False,
        "terminator": "\\n",
        "two_wire": False,
    },
    "solarsim": {"uri": "wavelabs://127.0.0.1:1111"},
    "stage": {
        "uri": "127.0.0.1",
        "custom_positions": {
            "Load Position": 23,
            "Midway": 62.5,
            "Offline Position": 120,
            "Test spot A": 88.54241,
        },
        "experiment_positions": {"eqe": [800, 200], "solarsim": [200, 200]},
        "length": [850, 350],
        "speed": 29,
    },
    "substrates": {
        "active_layout": "6px_1in",
        "adapters": {
            "6px_1in_pcb": {"pcb_contact_pads": 6, "pcb_resistor": 0},
            "6px_30mm_pcb": {"pcb_contact_pads": 6, "pcb_resistor": 0},
        },
        "layout_names": ["6px_1in"],
        "layouts": {
            "4px_30mm": {
                "areas": [0.15, 0, 1.0, 0.15, 0.15, 0],
                "pcb_name": "6px_30mm_pcb",
                "pixels": [1, 3, 4, 5],
                "positions": [[-6, 1], [0, 1], [0, 1], [0, 1], [6, 1], [0, 1]],
            },
            "6px_1in": {
                "areas": [0.1, 0.1, 0.1, 0.1, 0.1, 0.1],
                "pcb_name": "6px_1in_pcb",
                "pixels": [1, 2, 3, 4, 5, 6],
                "positions": [[-5, 2], [-5, 2], [0, 3], [0, 4], [5, 5], [5, 6]],
            },
        },
        "number": [2, 4],
        "spacing": [35, 30],
    },
    "visa": {"visa_lib": "@py"},
}


def test_saver():
    mqttc.publish("measurement/run", pickle.dumps({"args": test_args, "config": test_config}), 2).wait_for_publish()

    raw_iv_data = [[0, 1, 2, 3], [0, 1, 2, 3], [0, 1, 2, 3]]

    raw_ivt_data = [0, 1, 2, 3]

    raw_eqe_data = [time.time(), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

    processed_iv_data = [[0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5]]

    processed_ivt_data = [0, 1, 2, 3, 4, 5]

    processed_eqe_data = [time.time(), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    raw_iv_payload = {
        "data": raw_iv_data,
        "idn": "test_pixel0",
        "pixel": {},
        "clear": False,
        "end": False,
        "sweep": "light",
    }

    raw_ivt_payload = {
        "data": raw_ivt_data,
        "idn": "test_pixel0",
        "pixel": {},
        "clear": False,
        "end": False,
        "sweep": "light",
    }

    raw_eqe_payload = {
        "data": raw_eqe_data,
        "idn": "test_pixel0",
        "pixel": {},
        "clear": False,
        "end": False,
        "sweep": "light",
    }

    processed_iv_payload = {
        "data": processed_iv_data,
        "idn": "test_pixel0",
        "pixel": {},
        "clear": False,
        "end": False,
        "sweep": "light",
    }

    processed_ivt_payload = {
        "data": processed_ivt_data,
        "idn": "test_pixel0",
        "pixel": {},
        "clear": False,
        "end": False,
        "sweep": "light",
    }

    processed_eqe_payload = {
        "data": processed_eqe_data,
        "idn": "test_pixel0",
        "pixel": {},
        "clear": False,
        "end": False,
        "sweep": "light",
    }

    # simulate writing a few lines to a single file
    for i in range(5):
        mqttc.publish(
            "data/raw/vt_measurement",
            pickle.dumps(raw_ivt_payload),
            2,
        ).wait_for_publish()

        mqttc.publish(
            "data/raw/it_measurement",
            pickle.dumps(raw_ivt_payload),
            2,
        ).wait_for_publish()

        mqttc.publish(
            "data/raw/mppt_measurement",
            pickle.dumps(raw_ivt_payload),
            2,
        ).wait_for_publish()

        mqttc.publish(
            "data/raw/eqe_measurement",
            pickle.dumps(raw_eqe_payload),
            2,
        ).wait_for_publish()

    mqttc.publish(
        "data/raw/iv_measurement",
        pickle.dumps(raw_iv_payload),
        2,
    ).wait_for_publish()

    for i in range(5):
        mqttc.publish(
            "data/processed/vt_measurement",
            pickle.dumps(processed_ivt_payload),
            2,
        ).wait_for_publish()

        mqttc.publish(
            "data/processed/it_measurement",
            pickle.dumps(processed_ivt_payload),
            2,
        ).wait_for_publish()

        mqttc.publish(
            "data/processed/mppt_measurement",
            pickle.dumps(processed_ivt_payload),
            2,
        ).wait_for_publish()

        mqttc.publish(
            "data/processed/eqe_measurement",
            pickle.dumps(processed_eqe_payload),
            2,
        ).wait_for_publish()

    mqttc.publish(
        "data/processed/iv_measurement",
        pickle.dumps(processed_iv_payload),
        2,
    ).wait_for_publish()


if __name__ == "__main__":
    mqttc = mqtt.Client()
    mqttc.connect("127.0.0.1")
    mqttc.loop_start()

    test_saver()

    mqttc.loop_stop()
    mqttc.disconnect()
