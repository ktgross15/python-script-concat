import dataiku
from dataiku.customrecipe import *
import pandas as pd
import numpy as np
import time

skip_lines = ['-------------------------------------------------------------------------------- NOTEBOOK-CELL: ',
           '# CODE\n# -*- coding: utf-8 -*-\n',
           ' -*- coding: utf-8 -*-',
           '\n# CODE\n',
           '# Read recipe inputs',
           '# Write recipe outputs']

def get_recipe_inputs(rcp):
    '''Get list of all input datasets to recipe'''
    rcp_inputs_dict = rcp["inputs"]
    if rcp_inputs_dict:
        input_key = list(rcp_inputs_dict.keys())[0]
        rcp_inputs_list = [x["ref"] for x in rcp_inputs_dict[input_key]["items"]]
    else:
        rcp_inputs_list = []
    return rcp_inputs_list

def get_recipe_outputs(rcp):
    '''Get list of all output datasets from recipe'''
    rcp_outputs_dict = rcp["outputs"]
    output_key = list(rcp_outputs_dict.keys())[0]
    rcp_outputs_list = [x["ref"] for x in rcp_outputs_dict[output_key]["items"]]
    return rcp_outputs_list


def generate_starttimes_df(job):
    starttimes_dict = {}
    activities = job.get_status()['baseStatus']['activities'].iteritems()
    for recipe, vals in activities:
        # remove stuff from partioned recipes etc too!
        # look into other potential activity keys??
        if recipe[-3:] == '_NP':
            recipe = recipe[:-3]
        starttimes_dict[recipe] = vals['startTime']

    start_times_df = pd.DataFrame(starttimes_dict, index=['start_time']).T.reset_index().sort_values(by='start_time')
    start_times_df.rename(columns={'index':'recipe_name'}, inplace=True)
    start_times_df['flow_order'] = start_times_df['start_time'].rank()
    return start_times_df

def generate_all_recipes_df(project, start_times_df):
    df_data = []
    for recipe in project.list_recipes():
        row_dict = {}
        row_dict['recipe_name'] = recipe['name']
        row_dict['recipe_type'] = recipe['type']
        row_dict['payload'] = project.get_recipe(recipe['name']).get_definition_and_payload().get_payload()
        row_dict['inputs'] = get_recipe_inputs(recipe)
        row_dict['outputs'] = get_recipe_outputs(recipe)
        df_data.append(row_dict)
        
    df_all_recipes = pd.DataFrame(df_data, columns=['recipe_name','recipe_type','payload','inputs','outputs'])
    df_all_recipes = pd.merge(df_all_recipes, start_times_df, on='recipe_name', how='left')
    df_all_recipes.sort_values(by='flow_order', inplace=True)    

    return df_all_recipes


def create_obj_ds_match_dict(ds_obj_dict, all_datasets, line):
    for ds_name in all_datasets:
        if ds_name in line:
            obj_name_end = line.find('=') - 1
            obj_name = line[:obj_name_end]
            ds_obj_dict[ds_name] = obj_name
    return ds_obj_dict


def add_line(all_lines, line, f):
    f.write(line)
    f.write('\n')
    all_lines.append(line)


def generate_csv(project_key, folder_handle, ds_name):
    file_name = '{}.csv'.format(ds_name)
    with folder_handle.get_writer(file_name) as writer:
        project_ds_name = project_key + '.' + ds_name
        dataset = dataiku.Dataset(project_ds_name)
        df = dataset.get_dataframe()
        df.to_csv(writer)
    