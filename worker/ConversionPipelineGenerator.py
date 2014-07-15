"""
ConversionPipelineGenerator
~~~~~~~~~~~~~~~~~

This module provides ConversionPipelineGenerator class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback

import ImagingCloudVolumeDestination

import libcloud
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

import RawDiskImage

import SourceReader
import Downloader
import LocalPathDestination

DEST_LOCAL = "local"
DEST_TEST_VHD = "vhd"
DEST_CLOUD = "cloud"
DEST_LOCAL_PATH = "path"

class ConversionPipelineGenerator(object):
    """This class is to generate pipelines of SourceReader : Image : Destination due to configs, manifest, task options"""
    _GIG_ = 1073741824

    def __init__(self):

        return

    def __chooseDestination(self , manifest , config = None , task_opts = None):
        """creates destination object due to config"""
        destination = DEST_LOCAL
        #choose destination
        if config:
            destination = config.get_destination_type()

        if destination == DEST_TEST_VHD:
            import LocalVhdDestination
            return LocalVhdDestination.LocalVhdDestination()
        elif destination == DEST_CLOUD:
            cloud_name = config.get_cloud_name()
            cls = get_driver(cloud_name)
            cloud_user = config.get_user_name()
            cloud_secret_key = config.get_secret_name()
            driver = cls(user, key)
            return ImagingCloudVolumeDestintaion.ImagingCloudVolumeDestintaion(driver, RawDiskImage.RawDiskImage() , manifest['import']['volume-size']*_GIG_)
        elif destination == DEST_LOCAL_PATH:
            path = config.get_local_path()      
            if manifest.local():
                path = manifest.extension.path
            return LocalPathDestination.LocalPathDestination(path , RawDiskImage.RawDiskImage())
            #TODO: GENERATE LOCAL DESTINATION

        return None

    def __createSource(self, manifest, config , task_opts):
        """creates the source according to manifest file"""
        #here we should create some decoders

        #here we should analyze manifest\config to get some decoders
        return SourceReader.SourceReader(manifest , Downloader.Downloader())

    def generateConversionPipeline(self , manifest , config = None , task_opts = None):
        """
            Generates Pipeline to convert the volume
            Args:
                manifest - manifest object representing task got from the client
                config - config for this one worker
                task_opts - options passed from Eucalyptus cloud controller

            Returns list of [(SourceReader, Destination)] tupls each one representing actions to write one volume covered in manifest
        """
        
        source = self.__createSource(manifest, config , task_opts)
        dest = self.__chooseDestination(manifest , config , task_opts)
        return (source , dest)
