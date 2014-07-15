"""
Decoder
~~~~~~~~~~~~~~~~~

This module provides Decoder class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback



class Decoder(object):
    """simple interface to decode data"""

    def __init__(self):
        pass

    def decode(self , data):
        return data