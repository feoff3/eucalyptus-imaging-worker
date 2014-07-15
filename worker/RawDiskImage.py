"""
RawDiskImage
~~~~~~~~~~~~~~~~~

This module provides RawDiskImage class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import DiskImage


class RawDiskImage(DiskImage.DiskImage):
    """Represents a raw file or drive, writes data as-is or implies an optional encoder"""

    def __init__(self , encoder = None):
        self.__deviceObject = None
        self.__streamEncoder = encoder
        return

    def open(self , path):
        """opens the image by path"""
        self.__deviceObject = open(path, "w+b")
       
    def writeImageData(self , data , offset):
        """The image data is written to the stream"""
        self.__deviceObject.seek(offset)
        self.__deviceObject.write(data)
        return

    def writeDiskData(self , data , offset):
        """The disk data is written in raw format and thus has to be encoded (if necessary)"""
        if self.__streamEncoder:
            encoded_data = self.__streamEncoder.encode(data)
            data = encoded_data
        self.__deviceObject.seek(offset)
        self.__deviceObject.write(data)
        return

    def close(self):
        """closes the image"""
        self.__deviceObject.close()


