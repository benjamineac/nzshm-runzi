from ast import Num
import pprint
from prompt_toolkit import prompt 
from pprint import PrettyPrinter
import inquirer
from inquirer.themes import GreenPassion
from config.inversion_builder import Crustal, Subduction
from cli_helpers import pprint_color, NumberValidator

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

def crustal_setup(*args):
    global global_config
    base_config()

    global_config = Crustal(global_vars['task_title'], 
    global_vars['task_description'], 
    global_vars['worker_pool_size'], 
    global_vars['jvm_heap_max'],
    global_vars['java_threads'], 
    global_vars['use_api'], 
    global_vars['general_task_id'], 
    global_vars['file_id'], 
    global_vars['mock_mode'])

    print('Here\'s your crustal config')
    pprint_color(global_config.get_all())


def subduction_setup(*args):
    global global_config
    base_config()

    global_config = Subduction(global_vars['task_title'], 
    global_vars['task_description'],
    global_vars['worker_pool_size'], 
    global_vars['jvm_heap_max'],
    global_vars['java_threads'], 
    global_vars['use_api'], 
    global_vars['general_task_id'], 
    global_vars['file_id'], 
    global_vars['mock_mode'])

    print('Here\'s your subduction config')
    pprint_color(global_config.get_all())

def show_values(*args):
    global global_config
    pprint_color(global_config.get_all())   

# def show_one(*args):
#     global global_config
#     choice = inquirer.List("Which value?", choices=global_config.get_args())
#     if choice: pprint_color(global_config.__getitem__(choice))
def change_values(*args):
    global global_config
    args = global_config.get_args()
    question_list = [
        inquirer.List('arg',
            message="Choose a value to edit",
            choices=args
        ),
        inquirer.Text('value',
            message='What would you like the new value to be? If more than one put a space in between each value',
        ),
        inquirer.Confirm('continue',
            message='Would you like to change another value?',
        ),    
    ]
    answers = inquirer.prompt(question_list, theme=GreenPassion())

    arg = answers['arg']
    val = answers['value'].split(' ')
    global_config.__setitem__(arg, val)
   

    if answers['continue'] == True:
        print(f'You changed {arg} to: {val}')
        change_values()

    if answers['continue'] == False:
        print("Here are your new values!")
        pprint_color(global_config.get_args())
        save_to_json = inquirer.Confirm('save_to_json',
            message='Would you like to save this config to JSON?',
        )
        if save_to_json == True:
            global_config.to_json()
            print(f'Saved your config to JSON as {global_config._file_id}_config.json')

