import sys
import inv_setup
from cli_helpers import MenuHandler
from inv_setup import crustal_setup, subduction_setup, save_to_json
from load_json import load_from_json


context = 'runZLI'

def main():

    value_menu = MenuHandler(context + '/values', {
        # 'check1': inv_setup.show_one,
        'show': inv_setup.show_values,
        'edit': inv_setup.change_values
    })

    inversions_menu = MenuHandler(context + '/inversions', {
        'crustal': crustal_setup,
        'subduction': subduction_setup,
    })

    main_menu = MenuHandler(context, {
    'save': save_to_json,
    'load': load_from_json,
    'inversions': inversions_menu.run,
    'values': value_menu.run,
    'quit': quit
    })

    main_menu.run()


if __name__ == '__main__':
    main()