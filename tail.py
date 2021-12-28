import os
import sys
import time


# Based on  https://github.com/kasun/python-tail

class Tail(object):
    stop_event = False  # changes

    def __init__(self, tailed_file):

        self.check_file_validity(tailed_file)
        self.tailed_file = tailed_file
        self.callback = sys.stdout.write

    def follow(self, s=1):
        with open(self.tailed_file) as file_:
            # Go to the end of file
            file_.seek(0, 2)
            while not self.stop_event:  # changes
                curr_position = file_.tell()
                line = file_.readline()
                if not line:
                    file_.seek(curr_position)
                    time.sleep(s)
                else:
                    self.callback(line)

    def register_callback(self, func):
        """ Overrides default callback function to provided function. """
        self.callback = func

    def check_file_validity(self, file_):
        """ Check whether the a given file exists, readable and is a file """
        if not os.access(file_, os.F_OK):
            raise TailError("File '%s' does not exist" % (file_))
        if not os.access(file_, os.R_OK):
            raise TailError("File '%s' not readable" % (file_))
        if os.path.isdir(file_):
            raise TailError("File '%s' is a directory" % (file_))


class TailError(Exception):
    def __init__(self, msg):
        self.message = msg

    def __str__(self):
        return self.message
