import json
import sys
import os

from inquirer import List, Text, Confirm, prompt

def load_from_json(*args):

    saved_configs = os.listdir('./saved_configs')

    files = List('files',
    message='choose from past files',
    choices=saved_configs)
