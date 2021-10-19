import sys
import random
import string
from prompt_toolkit import prompt
from prompt_toolkit.validation import Validator, ValidationError
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit import PromptSession

session = PromptSession()

from pygments import highlight
from pygments.lexers.python import PythonLexer
from pygments.formatters import Terminal256Formatter
from pprint import pformat
from pyfiglet import Figlet
from termcolor import cprint


def landing_banner():
    f = Figlet(font='univers')
    cprint('Welcome to the...', 'green')
    cprint(f.renderText('runzi CLI'), 'red')
    cprint('try inputting help to get started...', 'green')

def unique_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))

def pprint_color(obj):
    print(highlight(pformat(obj), PythonLexer(), Terminal256Formatter()))

def display(obj):
    print("General Arguments:")
    pprint_color(obj.get_general_args())
    print("Task Arguments:")
    pprint_color(obj.get_task_args())
    print("Job Arguments:")
    pprint_color(obj.get_job_args())

class MenuHandler():

    def __init__(self, menu_context, options=None, exit_options=None):
        self.context = menu_context
        self.options = options or {'quit', sys.exit}
        self.exit_options = exit_options or ['done']
        self.options.update({'help': self.option_help, 'done': self.option_done})
        self.value = None
        self.cmd = ''


    def help_about(self):
            return "Valid commands: %s" % ", ".join(self.options.keys())

    def option_help(self, option):
        '''help
        help "some command" will print the docstring of the optinos function
        '''
        command = option.strip()
        if not command:
            return self.help_about()

        func = self.options.get(command)
        if func:
            doc =  func.__doc__
            if doc:
                return doc
            else:
                return "Oops, no docstring for command:%s " % command
        return "unknown command:%s " % command


    def prompt(self):
        my_completer = WordCompleter(self.options.keys(), sentence=True )
        self.value = session.prompt('%s>' % self.context, completer=my_completer)
        return self.value

    def interrogate(self, cmd):

        matched = None
        for option in self.options.keys():
            cmd_part, remainder = cmd[:len(option)], cmd[len(option):]
            if cmd_part == option:
                matched = True

                # this might be a helper method, or another menuhandler
                fn = self.options[cmd_part]
                res = fn(cmd_part, remainder)
                # print(type(fn), dir(fn))
                if not (hasattr(fn, '__self__') and isinstance(fn.__self__, MenuHandler)):
                    None
                    # print(res)
                    # print('This is where the problem is lmao')
                    # print(fn.__self__)?
                continue
        if not matched:
            print('type "help" to see valid commands')
            # print('Ooops, can\'t handle "%s"' % cmd)
            # return self.help


    def option_done(self, cmd, value=None):
        '''done
        Exit the current menu level, if at the top level, this quits the CLI
        '''
        return None


    def run(self, *args):
        while True:
            self.cmd = self.prompt().strip()

            if self.cmd.lower()[:4] == 'help':
                print(self.option_help(self.cmd.lower()[4:]))
                continue
            if self.cmd.lower() in self.exit_options:
                return self.cmd.lower() #leave this menu
            self.interrogate(self.cmd)


class NumberValidator(Validator):
    def validate(self, document):
        text = document.text

        if text and not text.isdigit():
            i = 0

            # Get index of first non numeric character.
            # We want to move the cursor here.
            for i, c in enumerate(text):
                if not c.isdigit():
                    break

            raise ValidationError(message='This input must be an integer -- no non-numeric characters please',
                                  cursor_position=i)


def to_json_format(config):
    return {k[1:] : v for k, v in config.items()}

def from_json_format(config):
    return {'_' + k : v for k, v in config.items()}