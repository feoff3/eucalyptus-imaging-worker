"""
LocalPathDestination
~~~~~~~~~~~~~~~~~

This module provides LocalPathDestination class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback

import ImagingDestination

class LocalPathDestination(ImagingDestination.ImagingDestination):
    """represernts destination on local path\network share"""
    
    def __init__(self , path , disk_image):
        return super(LocalPathDestination, self).__init__()
        self.__path = path
        self.__diskImage = disk_image
    
    def open(self, config):
        """
        Opens the destination connection
        
        Args:
            config:dict - cloud-dependent connection configs, may include credentials to access network shares
        """
        self.__diskImage.open(self.__path)
        return

    def writeData(self, data, offset):
        """
        Writes data to the specific offset
        """
        self.__diskImage.writeDiskData(data , offset)

    def confirm(self):
        """
        Confirm data is written
        """
        return

    def close(self):
        """
        Close relesing all connections etc
        """
        self.__diskImage.close()