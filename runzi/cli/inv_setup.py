from ast import Num
from runzi.cli.cli_helpers import unique_id
from prompt_toolkit import prompt 
import inquirer
from config.inversion_builder import CrustalConfig, SubductionConfig
from cli_helpers import NumberValidator, display
from datetime import date

def base_config():
    global global_vars
    global_vars = {}
    
    global_vars['task_title'] = prompt('Enter the task title - str: ')
    global_vars['task_description'] = prompt('Enter the task description - str: ')
    global_vars['worker_pool_size'] = int(prompt('Enter the worker pool size - int: ',
                validator=NumberValidator(), validate_while_typing=True))
    global_vars['jvm_heap_max'] = int(prompt('Enter the jvm heap max - int: ', 
                validator=NumberValidator(), validate_while_typing=True))
    global_vars['java_threads'] = int(prompt('Enter the java threads - int: ', 
                validator=NumberValidator(), validate_while_typing=True))
    global_vars['use_api'] = prompt('Enter the use api - yes or no: ') in ['yes', 'y'] or False
    global_vars['general_task_id'] = prompt('Enter the general task id - string: ')
    global_vars['file_id'] = prompt('Enter the file id - string: ')
    global_vars['mock_mode'] = prompt('Would you like to use mock mode? - yes or no: ') in ['yes', 'y'] or False
    global_vars['rounds_range'] = int(prompt('How many rounds would you like to run? - int: ', 
                validator=NumberValidator(), validate_while_typing=True))

def crustal_setup(*args):
    global global_config
    base_config()

    global_config = CrustalConfig(global_vars['task_title'], 
    global_vars['task_description'], 
    global_vars['file_id'], 
    global_vars['worker_pool_size'], 
    global_vars['jvm_heap_max'],
    global_vars['java_threads'], 
    global_vars['use_api'], 
    global_vars['general_task_id'], 
    global_vars['mock_mode'],
    global_vars['rounds_range'])

    print('Here\'s your crustal config')
    display(global_config)


def subduction_setup(*args):
    global global_config
    base_config()

    global_config = SubductionConfig(global_vars['task_title'], 
    global_vars['task_description'],
    global_vars['file_id'], 
    global_vars['worker_pool_size'], 
    global_vars['jvm_heap_max'],
    global_vars['java_threads'], 
    global_vars['use_api'], 
    global_vars['general_task_id'], 
    global_vars['mock_mode'],
    global_vars['rounds_range'])

    print('Here\'s your subduction config')
    display(global_config)

def show_values(*args):
    global global_config
    try: 
        global_config
    except NameError: 
        print("Load or create a config first!")
    else:
        display(global_config)   

def change_general_values(*args): 
    global global_config
    change_values(global_config.get_general_args)

def change_job_values(*args): 
    global global_config
    change_values(global_config.get_job_args)

def change_task_values(*args): 
    global global_config
    change_values(global_config.get_task_args)

def change_values(value_callback):
    global global_config
    try:
        global_config
    except NameError:
        print("Load or create a config first!")
    else:
        global_config._unique_id = unique_id()
        arg_list = value_callback()
        question_list = [
            inquirer.List('arg',
                message="Choose a value to edit",
                choices=arg_list
            ),
            inquirer.Text('value',
                message=f'What would you like the new value to be? If you enter multiple values put spaces in between!',
            ),
            inquirer.Confirm('continue',
                message='Would you like to change another value?',
            ),    
        ]
        answers = inquirer.prompt(question_list)

        if answers['continue'] == False:
            save_to_json = inquirer.confirm('Would you like to save this config to JSON?')

        arg = answers['arg']
        val = answers['value']

        if value_callback == global_config.get_task_args:
            val = val.split(' ')
        if arg in ['_worker_pool_size', '_jvm_heap_max', '_java_threads', '_rounds_range']:
            if val == '':
                val = 0
            val = int(val)

        if arg in ['_mock_mode', '_use_api']:
            if val in ['yes', 'y', 'true', 'True', '1', 'Yes']:
                val = True
            else:
                val = False
        
        global_config.__setitem__(arg, val)

    def save_query():
        if save_to_json == True:
            global_config.to_json()

    if answers['continue'] == True:
        print(f'You changed {arg} to: {val}')
        change_values(value_callback)

    if answers['continue'] == False:
        print("Here are your new values!")
        display(global_config)
        save_query()

def save_to_json(*args):
    global_config.to_json()

def subduction_run(*args):
    global_config.run_subduction()

