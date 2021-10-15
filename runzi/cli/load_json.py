import json
from platform import version
from runzi.cli.cli_helpers import from_json_format
import sys
import os
from pathlib import Path

from config.inversion_builder import Config
import inquirer
from inquirer.themes import GreenPassion
from cli_helpers import pprint_color
import inv_setup

def load_from_json(*args):
    saved_configs = os.listdir(Path(__file__).resolve().parent / 'config' / 'saved_configs')
    filepath = inquirer.list_input("Choose from past configs, select to preview", choices=saved_configs)
    file = open(Path(__file__).resolve().parent / 'config' / 'saved_configs' / filepath)
    loaded_config = json.load(file)
    pprint_color(loaded_config)
    choice = inquirer.list_input("Would you like to load this config?", 
        choices = ['Yes', 'No', 'Exit'])
    if choice == 'Yes':
        parse_config(loaded_config)
    if choice == 'No':
        load_from_json()
    if choice == 'Exit':
        return

def parse_config(config):
    formatted_json = from_json_format(config)
    inv_setup.global_config = Config()
    inv_setup.global_config.from_json(formatted_json)



    
