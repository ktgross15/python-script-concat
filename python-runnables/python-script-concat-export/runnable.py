# This file is the actual code for the Python runnable python-script-concat-export
from dataiku.runnables import Runnable
# from dataiku.customrecipe import *
import dataiku
import pandas as pd
import numpy as np
from io import BytesIO
from zipfile import ZipFile
import logging
from pythonscriptconcat.python_concat_helpers import *

class PythonConcatenator(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
		"""
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        self.client = dataiku.api_client()


    def create_rebuild_job(self, project, final_datasets):
        '''Format list of outputs for job and run job to recursively rebuild flow'''
        outputs_list = [{"id": ds} for ds in final_datasets]
        job = project.start_job({"type":"RECURSIVE_FORCED_BUILD", "outputs":outputs_list})
        return job


    def check_rebuild_job_status(self, job):
        # check job state every second while job is running
        state = job.get_status()['baseStatus']['state']
        while state != 'DONE' and state != 'FAILED' and state != 'ABORTED':
            time.sleep(1)
            state = job.get_status()['baseStatus']['state']

        # return failure messages if necessary
        if state == 'FAILED':
            activities = job.get_status()['baseStatus']['activities']
            for ak in activities.keys():
                if activities[ak]['state'] == 'FAILED':
                    raise Exception("Recursive rebuild job failed. Please check logs.")
        elif state == 'ABORTED':
            raise Exception("Recursive rebuild job was aborted. Please check logs and re-run macro.")


    def run(self, progress_callback):

        output_filename = self.config.get('output_filename', '')
        # bytes_io = BytesIO()
        # zf = ZipFile('new.zip', 'w')

        f = open(output_filename,"w+")

        project = self.client.get_project(self.project_key)
        all_recipes = project.list_recipes()

        all_input_datasets = set([input_ds for rcp in all_recipes for input_ds in get_recipe_inputs(rcp)])
        all_output_datasets = set([output_ds for rcp in all_recipes for output_ds in get_recipe_outputs(rcp)])
        initial_datasets = list(all_input_datasets - all_output_datasets)
        final_datasets = list(all_output_datasets - all_input_datasets)

        job = self.create_rebuild_job(project, final_datasets)
        self.check_rebuild_job_status(job)

        start_times_df = generate_starttimes_df(job)
        df_all_recipes = generate_all_recipes_df(project, start_times_df)
        # include pyspark too later
        python_recipes_df = df_all_recipes[df_all_recipes['recipe_type']=='python'].reset_index(drop=True)

        all_python_inputs = list(python_recipes_df['inputs'].apply(pd.Series).stack().reset_index(drop=True))
        input_ds_obj_dict = {}
        output_ds_obj_dict = {}
        all_lines = []

        for ix, val in python_recipes_df.iterrows():

            recipe_name_line = '#### {} ####'.format(val['recipe_name'])
            add_line(all_lines, recipe_name_line, f)

            pay_lines = val['payload'].split('\n')
            input_names = val['inputs']
            output_names = val['outputs']

            for line in pay_lines:

                # skip duplicate package imports and "skip string" lines
                if ("import" in line and line in all_lines) or (any(skip_str in line for skip_str in skip_lines)):
                    continue

                # add to filename-object mapping dict
                elif 'dataiku.Dataset(' in line:
                    input_ds_obj_dict = create_obj_ds_match_dict(input_ds_obj_dict, input_names, line)
                    output_ds_obj_dict = create_obj_ds_match_dict(output_ds_obj_dict, output_names, line)

                # read in dataframe either from input file or previous script
                elif '.get_dataframe(' in line:
                    for input_name in input_names:
                        if input_name in input_ds_obj_dict.keys():
                            object_name = input_ds_obj_dict[input_name]
                            if object_name and object_name in line:
                                if input_name in initial_datasets:
                                    # if it's an initial recipe, generate an input file and then read from it
                                    generate_input_csv(input_name, line)
                                    read_input_line = "df = pd.read_csv('{}.csv')".format(input_name)
                                    add_line(all_lines, read_input_line, f)
                                else:
                                    # otherwise generate string to read in auto-generated dataframe name
                                    new_df_name = line[:line.find(' =')]
                                    name_change_str = '{} = {}_df.copy()'.format(new_df_name, input_name)
                                    add_line(all_lines, name_change_str, f)

                # if this is the final line and the ouput is an input to another recipe, copy the df name
                elif '.write_with_schema(' in line:
                    for output_name in output_names:
                        # double check this
                        if output_name in all_python_inputs:
                            object_name = output_ds_obj_dict[output_name]
                            if object_name in line:
                                df_name_loc = line.find('write_with_schema(') + 18
                                df_name = line[df_name_loc:-1]
                                name_change_str = '{}_df = {}.copy()'.format(output_name, df_name)
                                add_line(all_lines, name_change_str, f)

                else:
                    # add all other lines
                    add_line(all_lines, line, f)

        f.close()

        # later test if folder exists, then create if not
        output_folder_name = self.config.get('output_folder', '')
        folder_handle = project.create_managed_folder(output_folder_name)
        fr = open(output_filename, "r")
        folder_handle.put_file(output_filename, fr.read())

        result = '<span>Python code written to folder: {}</span>'.format(output_folder_name)
      
        return result
        