
"""
Place the script into your home folder and set PYTHONSTARTUP
variable like this:
export PYTHONSTARTUP=$HOME/.python-autocomplete.py
"""
import os
import sys
import readline
import rlcompleter

history = os.path.join(os.environ["HOME"], ".pyhist")
try:
    readline.read_history_file(history)
except IOError:
    pass

import atexit
atexit.register(readline.write_history_file, history)

if os.uname()[0] == 'Darwin':
    readline.parse_and_bind ("bind ^I rl_complete")  # Mac OS X only!
else:
    readline.parse_and_bind("tab: complete")  # other systems

del os, history

