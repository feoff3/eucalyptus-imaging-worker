"""
ImagingDestination
~~~~~~~~~~~~~~~~~

This module provides ImagingDestination class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback



class ImagingDestination(object):
    """this class represents any endpoint of imaging in the cloud: volume, file share, etc"""

    def __init__(self):
        return

    def open(self, config):
        """
        Opens the destination connection
        
        Args:
            config:dict - cloud-dependent connection configs
        """
        return

    def writeData(self, data, offset):
        """
        Writes data to the specific offset
        """

    def confirm(self):
        """
        Confirm data is written
        """
        return

    def close(self):
        """
        Close relesing all connections etc
        """
        return