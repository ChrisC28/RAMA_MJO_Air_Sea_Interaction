import xarray
#from dask_jobqueue import SLURMCluster
#from dask.distributed import Client
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas
import decimal 

def construct_object_for_variables(file_path,variables_to_read,sampling_frequency='hourly'):
    
    mooring_object_for_variable      = {k: [] for k in variables_to_read}

    
    for i_variable in variables_to_read:
        print('Looking for variable: ', i_variable)
        mooring_object_for_variable[i_variable] = {'label':[],'latitude':[],'longitude':[],'start_date':[],'end_date':[],'file_path':[]}
        
        mooring_labels_list      = []
        mooring_full_path_list   = []
        
        variable_root_path = os.path.join(file_path,i_variable)
        data_directory     = os.path.join(variable_root_path,sampling_frequency)
        if os.path.isdir(data_directory):
            
            
            files_in_subdir = os.listdir(data_directory )
            files_in_subdir.sort()
            for i_file in files_in_subdir:
                if os.path.isfile(os.path.join(data_directory,i_file)) and i_file.endswith('.cdf'):
                    #print(i_file)
                    mooring_dataset = xarray.open_dataset(os.path.join(data_directory,i_file),decode_times=False)
                    #print(mooring_dataset)
                    mooring_latitude  = mooring_dataset['lat'].squeeze().values
                    mooring_longitude = mooring_dataset['lon'].squeeze().values
                    if mooring_latitude>=0:
                        
                        mooring_label = str( normalize_fraction(float(np.abs(mooring_latitude)) )  )   + 'n' \
                                      + str( normalize_fraction(float(np.abs(mooring_longitude)) ) ) + 'e'
                    else:
                        mooring_label = str( normalize_fraction(float(np.abs(mooring_latitude) ) ) ) + 's' \
                                      + str( normalize_fraction(float(np.abs(mooring_longitude)) ) ) + 'e'

                    
                    mooring_labels_list.append(mooring_label)
                    mooring_full_path_list.append(os.path.join(data_directory,i_file)) 

                    mooring_dataset.close()
                #END if os.path.isfile()
            #END for i_file
        #END if os.path.isdir()
        
        unique_moorings = list(set(mooring_labels_list) )
        for i_mooring in unique_moorings:
            idx_current_mooring = [i for i, x in enumerate(mooring_labels_list) if x == i_mooring]
            if len(idx_current_mooring)>1:
                
                for i_current_mooring_index in idx_current_mooring:
                    if mooring_full_path_list[i_current_mooring_index].endswith('.merged_downsampled.cdf'):
                        
                        mooring_file_to_get = mooring_full_path_list[i_current_mooring_index]
                    
                            
            else:
                mooring_file_to_get = mooring_full_path_list[idx_current_mooring[0]]
            #END if len(idx_current_mooring)>1:
                
            #print(mooring_file_to_get)
            #print('==================')
            mooring_object_for_variable[i_variable]['label'].append(i_mooring)
            
            
            if mooring_file_to_get.endswith('downsampled.cdf'):
                mooring_dataset = xarray.open_dataset(os.path.join(mooring_file_to_get),decode_times=True)
                mooring_dates = mooring_dataset['time'].squeeze().values
            else:
                mooring_dataset = xarray.open_dataset(os.path.join(mooring_file_to_get),decode_times=False)

                mooring_time_date          = mooring_dataset['time'].squeeze().values
                mooring_time_hour_seconds  = mooring_dataset['time2'].squeeze().values
                units, reference_date      = mooring_dataset['time2'].attrs['units'].split('since')
                        
                mooring_dates = pandas.to_datetime(mooring_time_date, unit='D', origin='julian') + \
                                pandas.to_timedelta(mooring_time_hour_seconds,unit='milliseconds')
            #END if mooring_file_to_get.endswith('downsampled.cdf')
            mooring_object_for_variable[i_variable]['latitude'].append(mooring_dataset['lat'].squeeze().values)
            mooring_object_for_variable[i_variable]['longitude'].append(mooring_dataset['lon'].squeeze().values)

            mooring_object_for_variable[i_variable]['start_date'].append(mooring_dates.min())
            mooring_object_for_variable[i_variable]['end_date'].append(mooring_dates.max())
            mooring_object_for_variable[i_variable]['file_path'].append(mooring_file_to_get)
                    
        #END for i_mooring
    #END for i_variable    
    return mooring_object_for_variable

def read_data_for_variable(RAMA_data_object,variable_to_get):
    
    
    mooring_labels          = RAMA_data_object[variable_to_get]['label']
    n_mooring_files_for_var = len(mooring_labels)
    
    mooring_data_for_variable      = {k: [] for k in mooring_labels}

    
    for i_mooring in range(0,n_mooring_files_for_var):
        
        current_mooring_label = mooring_labels[i_mooring]

        
        
        mooring_file_to_get = RAMA_data_object[variable_to_get]['file_path'][i_mooring]
        mooring_data_for_variable[current_mooring_label] = { }

        #print(mooring_file_to_get)
        if mooring_file_to_get.endswith('downsampled.cdf'):
            mooring_dataset = xarray.open_dataset(mooring_file_to_get,decode_times=True)
            mooring_dates = mooring_dataset['time'].squeeze().values
        
        else:
            mooring_dataset = xarray.open_dataset(mooring_file_to_get,decode_times=False)
        
            mooring_time_date          = mooring_dataset['time'].squeeze().values
            mooring_time_hour_seconds  = mooring_dataset['time2'].squeeze().values
            units, reference_date = mooring_dataset['time2'].attrs['units'].split('since')
            mooring_dates = pandas.to_datetime(mooring_time_date, unit='D', origin='julian') + pandas.to_timedelta(mooring_time_hour_seconds,unit='milliseconds')
        
        data_variables = [i for i in mooring_dataset.data_vars] 
        
        mooring_data_for_variable[current_mooring_label]['latitude']       = mooring_dataset['lat'].squeeze().values
        mooring_data_for_variable[current_mooring_label]['longitude']      = mooring_dataset['lon'].squeeze().values
        mooring_data_for_variable[current_mooring_label]['data_variables'] = data_variables
        mooring_data_for_variable[current_mooring_label]['time']           = mooring_dates
        for i_data_variable in data_variables:
            mooring_data_for_variable[current_mooring_label][i_data_variable] = mooring_dataset[i_data_variable].squeeze()
            mooring_data_for_variable[current_mooring_label][i_data_variable]['time'] = mooring_dates

       # print(res)
        #print('===================')
    return mooring_data_for_variable


def normalize_fraction(d):
    d = decimal.Decimal(d)
    normalized = d.normalize()
    sign, digit, exponent = normalized.as_tuple()
    return normalized if exponent <= 0 else normalized.quantize(1)

def read_RAMA_surface_fluxes(flux_path):

    mooring_flux_object = {}
    if os.path.isdir(flux_path):
        files_in_dir = os.listdir(flux_path )
        files_in_dir.sort()
        for i_file in files_in_dir:
            #print(i_file)
            if os.path.isfile(os.path.join(flux_path,i_file)) and i_file.endswith('hr.nc'):
                print(i_file)
                flux_dataset = xarray.open_dataset(os.path.join(flux_path,i_file),decode_times=True)
                mooring_latitude  = flux_dataset['lat'].squeeze().values
                mooring_longitude = flux_dataset['lon'].squeeze().values
                if mooring_latitude>=0:
                    mooring_label = str( normalize_fraction(float(np.abs(mooring_latitude)) )  )   + 'n' \
                              + str( normalize_fraction(float(np.abs(mooring_longitude)) ) ) + 'e'
                else:
                    mooring_label = str( normalize_fraction(float(np.abs(mooring_latitude) ) ) ) + 's' \
                              + str( normalize_fraction(float(np.abs(mooring_longitude)) ) ) + 'e'
        
            mooring_flux_object[mooring_label] = {}
        
            mooring_flux_object[mooring_label]['latitude']  = flux_dataset['lat'].squeeze().values
            mooring_flux_object[mooring_label]['longitude'] = flux_dataset['lon'].squeeze().values

            mooring_flux_object[mooring_label]['time']    = flux_dataset['time']
            mooring_flux_object[mooring_label]['file_path']     = os.path.join(flux_path,i_file)
        
            variables = ['wind_stress','sensible_heat_flux','latent_heat_flux','buoyancy_flux','specific_humidity_air','specific_humidity_water','net_LW','net_SW']
            mooring_flux_object[mooring_label]['data_variables'] = mooring_label
            for i_var in variables:
                mooring_flux_object[mooring_label][i_var] = flux_dataset[i_var]
            
    return mooring_flux_object


def RAMA_event_catalogue(RAMA_object,event_object,event_magnitude,event_phase,data_vars_to_get,days_before=5,days_after=5):
    
    unique_RAMA_stations = RAMA_object.keys()
    
    event_catalogue      = {k: [] for k in unique_RAMA_stations}

    
    
    for i_station in unique_RAMA_stations: #unique_T_air_RAMA:
        current_station = RAMA_object[i_station]
        #print('Working on station:', i_station)    
        current_latitude  = current_station['latitude']
        current_longitude = current_station['longitude']
    
        event_catalogue[i_station] = {'PC_magnitude':[],'PC_phase':[],'time':[],'days':[],
                                                 'initiation_time':[],'termination_time':[]}
        for i_data_var in data_vars_to_get:
            if i_data_var != 'time2':
                event_catalogue[i_station][i_data_var] = []
    
    
        event_counter = 0
        for i_event in range(0,event_object['n_events']):
     
            if event_object['event_duration'][i_event]<180:    #Get variable for time period
                event_start_time       = event_object['initiation_time'][i_event]-np.timedelta64(days_before,'D')
                event_termination_time = event_object['termination_time'][i_event]+np.timedelta64(days_after,'D')
         

                phase_for_event     = event_phase.sel(time=slice(event_start_time,
                                                                     event_termination_time) )
                magnitude_for_event = event_magnitude.sel(time=slice(event_start_time,
                                                                         event_termination_time))                                  
                time_to_interp = pandas.date_range(start=event_start_time,end=event_termination_time,freq='1H')

                found_overlapping_data = False
                for i_data_var in data_vars_to_get:
                    if i_data_var != 'time2':
                        current_station_data = current_station[i_data_var].squeeze().sortby(current_station[i_data_var]['time'])
                        test_slice = current_station_data.sel(time=slice(event_start_time,
                                                                               event_termination_time) )
                        if test_slice.size != 0 and not np.all(np.isnan(test_slice.values)):
                        #print('Here')
                         
                            data_var_for_event = current_station[i_data_var].squeeze().interp(
                                     time=time_to_interp,method='nearest',
                                     kwargs={'fill_value':np.nan})
                
                            event_catalogue[i_station][i_data_var].append(data_var_for_event.values)
                            found_overlapping_data = True
                    #END if i_data_var
                #END for i_data_var in data_vars_to_get
                
                if found_overlapping_data:
                    event_counter=event_counter+1
                    event_catalogue[i_station]['initiation_time'].append(event_start_time) 
                    event_catalogue[i_station]['termination_time'].append(event_termination_time) 

                
                    #event_catalogue['Sat_OLR'].append(sat_OLR_for_event.interp(time=time_to_interp,kwargs={'fill_value':np.nan}).values)
                    event_catalogue[i_station]['PC_magnitude'].append(magnitude_for_event.interp(time=time_to_interp,
                                                                                                     kwargs={'fill_value':np.nan}).values)
                    event_catalogue[i_station]['PC_phase'].append(phase_for_event.interp(time=time_to_interp,
                                                                                             kwargs={'fill_value':np.nan}).values)
                    event_catalogue[i_station]['time'].append(time_to_interp)
                    found_overlapping_data = False
               #END if found_overlapping_data
        event_catalogue[i_station]['n_events'] = event_counter
    return event_catalogue