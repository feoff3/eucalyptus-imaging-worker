"""
GzipDecoder
~~~~~~~~~~~~~~~~~

This module provides GzipDecoder class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import Decoder
import StringIO
import gzip

class GzipDecoder(Decoder):
    """simple decoder to decode gzipped chunks"""
    

    def decode(self , data):
      inmemfile = StringIO.StringIO(data)
      gzipfile = gzip.GzipFile("tmpgzip", "rb" , fileobj = inmemfile)
      decoded = gzipfile.read()
      gzipfile.close()
      inmemfile.close()
      return decoded
