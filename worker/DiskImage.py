"""
DiskImage
~~~~~~~~~~~~~~~~~

This module provides DiskImage class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback



class DiskImage(object):
    """Describes disk image"""

    def __init__(self):
        return

    def open(self, path):
        return

    def writeImageData(self , data , offset):
        """The image data is written to the stream"""
        return

    def writeDiskData(self , data , offset):
        """The disk data is written in raw format and thus has to be encoded (if necessary)"""
        return

    def close(self):
        return

    