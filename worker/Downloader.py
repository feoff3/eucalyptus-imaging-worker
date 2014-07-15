"""
Downloader
~~~~~~~~~~~~~~~~~

This module provides Downloader class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import httplib2


class Downloader(object):
    """a class representing a connection to euca cloud to download stuff"""

    def __init__(self):
        pass

    def download(self , url):
        """downloads from URL"""
        resp, content = httplib2.Http().request(url)
        if resp['status'] != '200' or len(content) <= 0:
            raise FailureWithCode('Could not download data from the storage', DOWNLOAD_DATA_FAILURE)
        return content
