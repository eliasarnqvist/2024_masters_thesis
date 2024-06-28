
"""
Spring 2024
Based on script from Cristiano Fontana, modified by Elias Arnqvist

This script is meant to replay a raw.adr file produced from ABCD.
It will go through a range of parameters specified below. 

To function, tmux needs to be running with ABCD. 
This is done with a startup script. 
Then this script can be executed. 
"""

import numpy as np
import scipy.optimize as opt

import datetime
import zmq
import json
import time
import threading

import subprocess
import csv

# Addresses for ABCD
ADDRESS_COMMANDS_WAAN = 'tcp://127.0.0.1:16208'
ADDRESS_COMMANDS_SPEC = 'tcp://127.0.0.1:16189'
ADDRESS_COMMANDS_TOFCALC = 'tcp://127.0.0.1:16202'
ADDRESS_DATA_SPEC = 'tcp://127.0.0.1:16188'
ADDRESS_DATA_TOFCALC = 'tcp://127.0.0.1:16201'

# =============================================================================
# Specify paths
# =============================================================================

# Location of the raw file that is to be replayed
RAW_FILE = "/home/elias/abcd_data/2024-03-14_run1_PuC_20min_8detectors_CLLBC123/2024-03-14T11-48-49_DT5730_PuC_Ch0_CLLBC1_HV-810_Ch1_CLLBC2_HV-760_Ch2_TheBeast_HV700_Ch3_LaBr19.2_HV626_Ch4_LaBr19.4_HV539_Ch5_LaBr19.6_HV539_Ch6_LaBr19.8_HV620_Ch7_CLLBC3_HV790_raw.adr"
# Config file for this script to change the parameters in
CONFIG_FILE = "config_Elias.json"
# Log file that is used by this script to keep track of what is done
# LOG_FILE = "optimization_log_file_1.csv"
# Location of ABCD's replay script
# REPLAY_RAW = "/home/localusr/abcd/replay/replay_raw.py"
REPLAY_RAW = "/home/elias/abcd/replay/replay_raw.py"
# Where to save csv files
CSV_FOLDER = "PSD_spectra/"

# =============================================================================
# Parameters to change
# =============================================================================

# NOTE: max value is not inclusive!
# Put max above the highest value you want to use!

# 100-1500
REG1_MIN = 100
REG1_MAX = 1501
REG1_STEP = 100

# 500-2000
REG2_MIN = 500
REG2_MAX = 2001
REG2_STEP = 100

# only 0
DIST1_MIN = 0
DIST1_MAX = 1
DIST1_STEP = 1

# only 0
DIST2_MIN = 0
DIST2_MAX = 1
DIST2_STEP = 1

# =============================================================================
# Values
# =============================================================================

s = 1
REPLAY_TIME = 180*s
RESET_TIME = 6*s

# Number of steps for the parameters
REG1_STEPS = int((REG1_MAX - REG1_MIN) / REG1_STEP)
REG2_STEPS = int((REG2_MAX - REG2_MIN) / REG2_STEP)
DIST1_STEPS = int((DIST1_MAX - DIST1_MIN) / DIST1_STEP)
DIST2_STEPS = int((DIST2_MAX - DIST2_MIN) / DIST2_STEP)

# This is the channel to parse (ToF non-reference or the energy spectrum we want)
CHANNEL = 1
# Reference channel, to change parameters of
CHANNELS_INDEX = 1

#ENERGY_MIN = 25000
#ENERGY_MAX = 28000
ENERGY_DELTA = 10

ENERGY_THRESHOLD_GAMMA_MIN = 0
ENERGY_THRESHOLD_GAMMA_MAX = 66000

lock_spec = threading.Lock()
last_reception_spec = dict()

worker_calls = 0
received_messages_spec = 0
msg_ID = 0

context = zmq.Context()

socket_commands_waan = context.socket(zmq.PUSH)
socket_commands_waan.connect(ADDRESS_COMMANDS_WAAN)
socket_commands_spec = context.socket(zmq.PUSH)
socket_commands_spec.connect(ADDRESS_COMMANDS_SPEC)
socket_commands_tofcalc = context.socket(zmq.PUSH)
socket_commands_tofcalc.connect(ADDRESS_COMMANDS_TOFCALC)
socket_data_spec = context.socket(zmq.SUB)
socket_data_spec.connect(ADDRESS_DATA_SPEC)
socket_data_spec.setsockopt(zmq.SUBSCRIBE, "data_spec_histograms".encode("ascii"))
socket_data_tofcalc = context.socket(zmq.SUB)
socket_data_tofcalc.connect(ADDRESS_DATA_TOFCALC)
socket_data_tofcalc.setsockopt(zmq.SUBSCRIBE, "data_tofcalc_histograms".encode("ascii"))

# =============================================================================
# Functions
# =============================================================================

# Gets data from ABCD
def receiver(lock, last_reception, socket_data, received_messages):
    try:
        while 1:
            topic, json_message = socket_data.recv().decode('ascii', 'ignore').split(' ', 1)

            received_messages += 1
            now = datetime.datetime.now()

            print("Message received at: {} with topic: {}".format(now, topic))

            with lock:
                last_reception["timestamp"] = now
                last_reception["payload"] = json.loads(json_message)
                last_reception["topic"] = topic
    except Exception as error:
        print("ERROR: {}".format(error))


# Updates parameters
def send_parameters(parameters):
    global msg_ID
    global socket_commands_waan
    global socket_commands_spec

    reg1, reg2, dist1, dist2 = parameters

    print("Sending parameters: reg1: {:f}".format(reg1))
    print("                    reg2: {:f}".format(reg2))
    print("                    dist1: {:f}".format(dist1))
    print("                    dist2: {:f}".format(dist2))
    print("                    msg_ID: {:d}".format(msg_ID))

    with open(CONFIG_FILE) as config_file:
        config = json.load(config_file)
    
    reg1_start = dist1
    reg1_stop = dist1 + reg1
    reg2_start = dist1 + reg1 + dist2
    reg2_stop = dist1 + reg1 + dist2 + reg2
    
    config['channels'][CHANNELS_INDEX]['user_config']['reg1_start'] = int(reg1_start)
    config['channels'][CHANNELS_INDEX]['user_config']['reg1_stop'] = int(reg1_stop)
    config['channels'][CHANNELS_INDEX]['user_config']['reg2_start'] = int(reg2_start)
    config['channels'][CHANNELS_INDEX]['user_config']['reg2_stop'] = int(reg2_stop)

    message = dict()
    message["msg_ID"] = msg_ID
    msg_ID += 1
    message["timestamp"] = datetime.datetime.now().isoformat()
    message["command"] = "reconfigure"
    message["arguments"] = {"config": config}

    json_message = json.dumps(message)

    socket_commands_waan.send(json_message.encode('ascii'))

    print("Waiting reset: {:f} s".format(RESET_TIME))
    time.sleep(RESET_TIME)
    
    print("Sending reset to spec")
    print("msg_ID: {:d}, received_messages_spec: {:d}".format(msg_ID, received_messages_spec))

    message = dict()
    message["msg_ID"] = msg_ID
    msg_ID += 1
    message["timestamp"] = datetime.datetime.now().isoformat()
    message["command"] = "reset"
    message["arguments"] = {"channel": "all"}

    json_message = json.dumps(message).encode('ascii')

    socket_commands_spec.send(json_message)


def parse_data_spec(message):
    for channel in message["data"]:
        if channel["id"] == CHANNEL:
            print("Found channel {:d} spectrum".format(CHANNEL))

            E_histo = channel["energy"]
            PSDvsE_histo = channel["PSD"]

            E_min = PSDvsE_histo["config"]["min_x"]
            E_max = PSDvsE_histo["config"]["max_x"]
            E_N = PSDvsE_histo["config"]["bins_x"]
            PSD_min = PSDvsE_histo["config"]["min_y"]
            PSD_max = PSDvsE_histo["config"]["max_y"]
            PSD_N = PSDvsE_histo["config"]["bins_y"]

            energies = np.linspace(E_min, E_max, E_N)

            PSDs = np.linspace(PSD_min, PSD_max, PSD_N)

            energy_counts = np.array(E_histo["data"], dtype = np.uint)
            counts2d = np.array(PSDvsE_histo["data"], dtype = np.uint)
            counts2d.shape = (PSD_N, E_N)

            return energies, PSDs, energy_counts, counts2d

    return None


def worker_function(parameters):
    global lock_spec
    global last_reception_spec
    global worker_calls

    worker_calls += 1

    print("Worker: worker_calls: {:d}".format(worker_calls))
    print("        parameters: {}".format(parameters))

    Delta_reg1, Delta_reg2 = parameters
    
    reg1 = int(Delta_reg1)
    dist1 = int(DIST1_MIN)
    dist2 = int(DIST2_MIN)
    reg2 = int(Delta_reg2)

    send_parameters((reg1, reg2, dist1, dist2))
    
    start_replay = datetime.datetime.now()
    
    # launch replay_raw.py with subprocess.run
    # subproc = ["python3", REPLAY_RAW, "-D", "tcp://*:16207", "-T", "1", RAW_FILE]
    subproc = ["python3", REPLAY_RAW, "-D", "tcp://*:16207", "-T", "10", RAW_FILE]
    print("Starting subprocess: {}".format(" ".join(subproc)))
    result = subprocess.run(subproc, capture_output=True, text=True)
    # Check the return code
    if result.returncode == 0:
        print("Command executed successfully!")
        # Print the output
        print(result.stdout)
    else:
        print("Error executing command!")
        # Print the error message
        print(result.stderr)

    # This is needed in the case of a replay always running in the background
    #after = now + datetime.timedelta(seconds = REPLAY_TIME)
    #print("[{}] Waiting accumulation: {:f} s; Stop at: {}".format(now.isoformat(), REPLAY_TIME, after.isoformat()))
    #time.sleep(REPLAY_TIME)
    now = datetime.datetime.now()
    print("Replay time: {:f} s".format((now - start_replay).total_seconds()))

    print("Waiting messages: {:f} s".format(RESET_TIME))
    time.sleep(RESET_TIME)
    
    with lock_spec:
        message_spec = last_reception_spec["payload"]
    
    energies, PSDs, energy_counts, counts2d = parse_data_spec(message_spec)
    
    # print(energies)
    # print(PSDs)
    # print(energy_counts)
    # print(counts2d)
    
    print("Parameters for last analysis: {:d}, {:d}, {:d}, {:d}".format(reg1, reg2, dist1, dist2))
    
    # Write PSD to csv file
    parameters_list = [reg1, reg2, dist1, dist2]
    
    data = zip(energies, energy_counts)
    spec_filename = 'E_' + str(parameters_list)
    with open(CSV_FOLDER + spec_filename + '.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)
    
    stacked_data = np.column_stack((PSDs, counts2d))
    spec_filename = 'PSD_' + str(parameters_list)
    with open(CSV_FOLDER + spec_filename + '.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        # writer.writerows(PSDs.tolist())
        # writer.writerows(energy_counts.tolist())
        writer.writerows(stacked_data)
    return 1


# =============================================================================
# Main
# =============================================================================

# Initialize threads for recieving spec and tof data from ABCD
receiver_spec_thread = threading.Thread(target=receiver,
                                        args=(lock_spec,
                                              last_reception_spec,
                                              socket_data_spec,
                                              received_messages_spec))
receiver_spec_thread.start()

print("Steps: reg1: {:d}".format(REG1_STEPS))
print("       reg2: {:d}".format(REG2_STEPS))
print("       dist1: {:d}".format(DIST1_STEPS))
print("       dist2: {:d}".format(DIST2_STEPS))

total_steps = REG1_STEPS * REG2_STEPS * DIST1_STEPS * DIST2_STEPS

print("Total steps: {:d}".format(total_steps))

total_time = (REPLAY_TIME + RESET_TIME) * total_steps

print("Expected total time: {:f} hr".format(total_time / (3600*s)))
print("Expected finish: {}".format(datetime.datetime.now() + datetime.timedelta(seconds = total_time)))

rranges = (slice(REG1_MIN, REG1_MAX, REG1_STEP), slice(REG2_MIN, REG2_MAX, REG2_STEP))

# resbrute = opt.brute(worker_function, rranges, full_output=True)
resbrute = opt.brute(worker_function, rranges, full_output=True, finish=None)

print(resbrute)

print("Global minimum: {}".format(resbrute[0]))
print("Function value: {}".format(resbrute[1]))

socket_commands_waan.close()
socket_commands_spec.close()
socket_data_spec.close()

receiver_spec_thread.join()

context.destroy()
