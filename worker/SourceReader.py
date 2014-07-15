"""
SourceReader
~~~~~~~~~~~~~~~~~

This module provides SourceReader class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback



class SourceReader(object):
    """this class reads data from the storage according to the manifest"""

    def __init__(self , manifest, downloader=None, decoder=None):
        """
            Constructor.

            Args:
                manifest: object - objectified manifest XML
                downloader: Downloader - a class to download stuff from images
                decoder: StreamDecoder - how to decode chunks
        """
        self.__manifest = manifest
        self.__decoder = decoder
        self.__lastDecodedChunk = 0

    def getRawChunk(self, chunk_index):
        """
            Returns chunk as-is
        """
        chunk_entry = self.__manifest['import'].part[chunk_index]
        url = chunk_entry['get-url'].text
        return downloader.download(url)
        

    def getRawChunkCount(self):
        """
            Returns the whole number of blocks to be downloaded
        """
        return int(self.__manifest['import'].parts.attrib["count"])

    def decodeAvailable(self):
        """checks if decoding available"""
        return not (self.__decoder == None)

    def getNextDecodedChunk(self):
        """
            Returns tuple (offset:long,chunk:str) unpacked\decoded (if it was packed somehow) 
            or 
            None if no decoding available
            None if there is no more chunks left
        """
        if self.__decoder == None:
            return None

        while 1:
            raw_chunk = getRawChunk(self.__lastDecodedChunk)
            #TODO: specify how decoder works
            # it returns empty chunk if it needs more data to decode
            (offset , chunk) = self.__decoder(raw_chunk)
            self.__lastDecodedChunk =  self.__lastDecodedChunk + 1
            if chunk:
                break
        return (offset , chunk)
        
    
    def getImageSize(self):
        """gets the total number of bytes in source"""
        return int(self.__manifest['import'].size)

    def getDiskSize(self):
        """gets the total number of bytes for the new disk which source represents"""
        return

    def getChunkSize(self):
        """returns the chunk size or 0 if there is no fixed chunk size"""
        return 0
