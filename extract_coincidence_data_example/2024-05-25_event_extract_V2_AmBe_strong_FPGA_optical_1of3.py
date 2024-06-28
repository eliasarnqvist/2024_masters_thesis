# Elias Arnqvist

import os
import math
# import functools
import numpy as np
# import itertools
import json
from numba import jit

# =============================================================================
# Settings
# =============================================================================

# Channels to use for neutron detection
channels_a = [0, 1, 7]
# Channels to use for gamma detection trigger
channels_b = [2, 3, 4, 5, 6]

# What to read
# file_name = r"E:\Data\\"
file_name = r"F:\abcd_data\2024-05-08_AmBe_strong_FPGA_optical_cut60keV_PSD\\"
file_name = file_name + "2024-05-08T18-42-41_DT5730_FPGA_AmBe-strong_Ch0_CLLBC1_HV-810_Ch1_CLLBC2_HV-760_Ch2_TheBeast_HV700_Ch3_LaBr19.2_HV626_Ch4_LaBr19.4_HV539_Ch5_LaBr19.6_HV539_Ch6_LaBr19.8_HV620_Ch7_CLLBC3_HV790_events.ade"

# Where to save
csv_folder = 'csv_folder'
save_folder = 'AmBe_coin_V2_1of3'
save_name = 'AmBe'

# For 500 MHz sampling, 1/500e6=2e-9 or 2 ns, then divide by 1024
ns_per_sample = 2.0 / 1024

# In units of ns
time_res = 1
time_min = -150
time_max = 100

# In units of keV
energy_res = 50
energy_min = 0
energy_max = 66000

# Using PSDlib
PSD_res = 0.0012
PSD_min = -0.2
PSD_max = 1

# This should be 160 MB
buffer_size = 16 * 10 * 1024 * 1024

# =============================================================================
# Main
# =============================================================================

print("Filename: {}".format(file_name))

# How big is the file, how many chunks have to be read?
file_size = os.path.getsize(file_name)
print("Filesize: {} MB".format(file_size))

chunks_needed = file_size / buffer_size
print("Required chunks: {:d}".format(math.ceil(chunks_needed)))

buffer_size = buffer_size - (buffer_size % 16)
print("Using buffer size: {:d}".format(buffer_size))

event_PSD_dtype = np.dtype([('timestamp', np.uint64),
                            ('qshort', np.uint16),
                            ('qlong', np.uint16),
                            ('baseline', np.uint16),
                            ('channel', np.uint8),
                            ('pur', np.uint8),
                            ])

print("Selected channels for a: {}".format(channels_a))
print("Selected channels for b: {}".format(channels_b))

N_a = len(channels_a)
N_b = len(channels_b)
N_ab = int(N_a * N_b)

# Coincidence array to store events in
coincidence_events = np.empty((0, 7))

counter = 0

with open('jsons\\Co60_zeros_FPGA.json', 'r') as json_file:
    loaded_json = json.load(json_file)

# =============================================================================
# Function
# =============================================================================

@jit(nopython=True)
def process_data(channels, timestamps, qlongs, PSDs,
                 ch_a, ch_b,
                 energy_min, energy_max, PSD_min, PSD_max,
                 time_min, time_max, offset):
    
    # To store coincidence events
    coincidence_events_chunk = np.empty((0, 7))
    
    # Loop over the events
    for i_event, (event_ch, event_timestamp, event_energy, event_PSD) in enumerate(zip(channels, timestamps, qlongs, PSDs)):
        
        # Find channel a
        if event_ch == ch_a and energy_min < event_energy < energy_max and PSD_min < event_PSD < PSD_max:
            
            # The region to look for coincidence detections
            event_timestamp = event_timestamp
            left_edge = time_min + event_timestamp
            right_edge = time_max + event_timestamp
            
            # Check for coincidences BEFORE and AFTER first detection
            for i_other in range(i_event + 1, len(timestamps)):
                # AFTER
                other_timestamp = timestamps[i_other]
                if left_edge + offset < other_timestamp < right_edge + offset:
                    
                    other_ch = channels[i_other]
                    other_energy = qlongs[i_other]
                    other_PSD = PSDs[i_other]
                    
                    if other_ch == ch_b and energy_min < other_energy < energy_max and PSD_min < other_PSD < PSD_max:
                        
                        time_diff = other_timestamp - event_timestamp - offset
                        
                        new_row = np.array([
                            ch_a, ch_b, 
                            event_energy, other_energy,
                            event_PSD, other_PSD,
                            time_diff
                            ])
                        new_row = new_row.reshape(1, 7)
                        coincidence_events_chunk = np.vstack((coincidence_events_chunk, new_row))
                else:
                    # To avoid unnecessary data
                    break
            for i_other in range(i_event - 1, -1, -1):
                # BEFORE
                other_timestamp = timestamps[i_other]
                if left_edge + offset < other_timestamp < right_edge + offset:
                    
                    other_ch = channels[i_other]
                    other_energy = qlongs[i_other]
                    other_PSD = PSDs[i_other]
                    
                    if other_ch == ch_b and energy_min < other_energy < energy_max and PSD_min < other_PSD < PSD_max:
                        
                        time_diff = other_timestamp - event_timestamp - offset
                        
                        new_row = np.array([
                            ch_a, ch_b, 
                            event_energy, other_energy,
                            event_PSD, other_PSD,
                            time_diff
                            ])
                        new_row = new_row.reshape(1, 7)
                        coincidence_events_chunk = np.vstack((coincidence_events_chunk, new_row))
                else:
                    # To avoid unnecessary data
                    break
    return coincidence_events_chunk

# =============================================================================
# Open
# =============================================================================

with open(file_name, "rb") as input_file:
    while True:
        try:
            print("    Reading chunk: {:d}".format(counter))
            
            file_chunk = input_file.read(buffer_size)
            
            # Check if we finished
            if not file_chunk:
                break
            
            data = np.frombuffer(file_chunk, dtype=event_PSD_dtype)
            
            # Only select data with positive energy
            channels_selection = np.logical_or(np.isin(data['channel'], channels_a), np.isin(data['channel'], channels_b))
            energy_selection = data['qlong'] > 0
            selection = np.logical_and(channels_selection, energy_selection)
            selected_data = data[selection]
            sorted_data = np.sort(selected_data, order = 'timestamp')
            # sorted_data = selected_data
            
            channels = sorted_data['channel']
            timestamps = sorted_data['timestamp'] * ns_per_sample
            qlongs = sorted_data['qlong']
            qshorts = sorted_data['qshort']
            PSDs = (qlongs.astype(np.float64) - qshorts) / qlongs
            
            ab_index = 0
            
            # Save detector a AND b in coincidence stuff (energy, PSD, ToF)
            for i_a, ch_a in enumerate(channels_a):
                ch_selection_a = channels == ch_a
                
                for i_b, ch_b in enumerate(channels_b):
                    ch_selection_b = channels == ch_b
                    
                    offset = loaded_json[str(ch_a)][str(ch_b)]
                    if offset == 'failed':
                        print('        did not do a:' + str(ch_a) + ', b:' + str(ch_b))
                        continue
                    
                    new_event_chunk = process_data(channels, 
                                                   timestamps, 
                                                   qlongs, 
                                                   PSDs,
                                                   ch_a, ch_b,
                                                   energy_min, energy_max, 
                                                   PSD_min, PSD_max,
                                                   time_min, time_max, 
                                                   offset)
                    
                    coincidence_events = np.vstack((coincidence_events, new_event_chunk))
                    
                    ab_index += 1
            counter += 1
            
        except Exception as error:
            print("    ERROR: {}".format(error))
            break

# =============================================================================
# Save histograms
# =============================================================================

print('\nStarting to save...')

# make folder to save everything in
main_folder = csv_folder + '\\' + save_folder
try:
    os.makedirs(main_folder)
except FileExistsError:
    print(f"Folder '{main_folder}' already exists!")



sub_folder = main_folder + '\\' + 'both_channels_ab'
try:
    os.makedirs(sub_folder)
except FileExistsError:
    print(f"Folder '{sub_folder}' already exists!")



output_name = sub_folder + '\\' + save_name + '_' + 'coincidence_events'
np.save(output_name + '.npy', coincidence_events)

print('Save step 1 done...')

# save key info
save_info = {
    'channels a':channels_a,
    'channels_b':channels_b,
    'ns_per_sample':ns_per_sample,
    'time_min':time_min,
    'time_max':time_max,
    'time_units':'ns',
    'energy_min':energy_min,
    'energy_max':energy_max,
    'energy_units':'ch',
    'PSD_min':PSD_min,
    'PSD_max':PSD_max,
    'PSD_units':'(qlong-qshort)/qlong'
    }

with open(main_folder + '\\key_info.json', 'w') as json_file:
    json.dump(save_info, json_file, indent=4)

print('Save step 2 done...')

print('\nDONE!')



