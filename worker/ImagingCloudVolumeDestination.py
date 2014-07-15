"""
ImagingCloudVolumeDestination
~~~~~~~~~~~~~~~~~

This module provides ImagingCloudVolumeDestination class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import ImagingDestination

# simport libcloud

class ImagingCloudVolumeDestination(ImagingDestination.ImagingDestination):
    """Represents cloud volume connected to this VM"""

    def __init__(self , cloud_driver , disk_image , volume_size):
        self.__cloudDriver = cloud_driver
        self.__cloudVolume = None
        self.__newDeviceName = None
        self.__deviceObject = None
        self.__diskImage = None
        self.__volumeSize = volume_size
        return super(ImagingCloudVolumeDestination, self).__init__()()
        

    def open(self, config = None):
        """
        Opens the destination connection
        
        Args:
            config:dict - cloud-dependent connection configs
        """
        #getting my instance id
        my_instance_id = config.getMyInstanceId()
        #using libcloud here
        self.__cloudVolume = self.__cloudDriver.create_volume(self.__volumeSize , volumename) 
        #attach new volume , wait till it appears
        self.__newDeviceName = self.__waitAttached(my_instance_id , vol)
        self.__diskImage.open(self.__newDeviceName)
        return

    def writeData(self, data, offset):
        """
        Writes data to the specific offset
        """
        self.__diskImage.writeDiskData(data, offset)
        

    def confirm(self):
        """
        Confirm data is written
        """
        return

    def close(self):
        """
        Close relesing all connections etc
        """
        self.__cloudDriver.dettach_volume(self.__cloudVolume)
        ####
        #### FEOFF-TODO: This is not generic: transfer stuff to other user
        ####
        self.__cloudDriver.transferVolumeToAnotherUser()
        return


    ###
    ### Auxillaries:
    ###

    def __get_block_devices():
        ret_list = []
        for filename in os.listdir('/dev'):
            if any(filename.startswith(prefix) for prefix in ('sd', 'xvd', 'vd', 'xd')):
                filename = re.sub('\d', '', filename)
                if not '/dev/' + filename in ret_list:
                    ret_list.append('/dev/' + filename)
        return ret_list

    def __waitAttached(self , volume_id , instance_id):
        devices_before = self.__get_block_devices()
        device_name = self.__next_device_name(devices_before)
        

        self.__cloudDriver.attach_volume(instance_id , volume_id , device_name)
        log.debug('Attaching volume {0} to {1} as {2}'.
                         format(volume_id, instance_id, device_name), self.task_id)

        elapsed = 0
        start = time.time()
        while elapsed < local_dev_timeout and not new_device_name:
            new_block_devices = get_block_devices()
            log.debug('Waiting for local dev for volume: "{0}", '
                             'elapsed:{1}'.format(self.volume.id, elapsed), self.task_id)
            diff_list = list(set(new_block_devices) - set(devices_before))
            if diff_list:
                for dev in diff_list:
                    new_device_name = dev
                    break
            elapsed = time.time() - start
            if elapsed < local_dev_timeout:
                time.sleep(2)
        if not new_device_name:
            raise FailureWithCode('Could find local device for volume:"%s"' % self.volume.id, ATTACH_VOLUME_FAILURE)
        return new_device_name


    def __next_device_name(self, all_dev):
        device = all_dev[0]
        device = re.sub('\d', '', device.strip())
        last_char = device[-1]
        device_prefix = device.rstrip(last_char)
        # todo use boto here instead of metadata? Need sec token then?
        # block_device_mapping = self.ec2_conn.conn.get_instance_attribute(
        #   instance_id=self.instance_id, attribute='blockdevicemapping')

        block_device_mapping = self._get_block_device_mapping_metadata()
        # iterate through local devices as well as cloud block device mapping
        for x in xrange(ord(last_char) + 1, ord('z')):
            next_device = device_prefix + chr(x)
            if (not next_device in all_dev) and \
                    (not next_device in block_device_mapping) and \
                    (not os.path.basename(next_device) in block_device_mapping):
                # Device is not in use locally or in block dev map
                return next_device
        # if a free device was found increment device name and re-enter
        return self.next_device_name(device_prefix + "aa")

    def _get_metadata(self, path, basepath='http://169.254.169.254/'):
        for x in xrange(0, 3):
            try:
                r = requests.get(os.path.join(basepath, path.lstrip('/')))
                r.raise_for_status()
                break
            except:
                if x >= 2:
                    raise
                time.sleep(1)
        return r.content

    def _get_block_device_mapping_metadata(self):
        devlist = []
        bdm_path = '/latest/meta-data/block-device-mapping'
        bdm = self._get_metadata(path=bdm_path)
        for bmap in bdm.splitlines():
            new_dev = self._get_metadata(path=os.path.join(bdm_path, bmap))
            if new_dev:
                devlist.append(new_dev.strip())
        return devlist