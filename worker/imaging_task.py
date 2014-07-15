# Copyright 2009-2014 Eucalyptus Systems, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 3 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see http://www.gnu.org/licenses/.
#
# Please contact Eucalyptus Systems, Inc., 6755 Hollister Ave., Goleta
# CA 93117, USA or visit http://www.eucalyptus.com/licenses/ if you need
# additional information or have any questions.
import tempfile
import time
import json
import os
import re
import requests
import config

import string
import subprocess
import traceback
import httplib2
import base64
import threading
from lxml import objectify
import ssl
from task_exit_codes import *
from failure_with_code import FailureWithCode

from logutil import *


class TaskThread(threading.Thread):
    def __init__(self, function):
        threading.Thread.__init__(self)
        self.function = function
        self.result = None

    def run(self):
        self.result = self.function()

    def get_result(self):
        return self.result


class ImagingTask(object):
    FAILED_STATE = 'FAILED'
    DONE_STATE = 'DONE'
    EXTANT_STATE = 'EXTANT'

    EXTANT_STATUS_REPORT_INTERVAL = 30

    def __init__(self, task_id, task_type):
        self.task_id = task_id
        self.task_type = task_type
        self.is_conn = ws.connect_imaging_worker(aws_access_key_id=config.get_access_key_id(),
                                                        aws_secret_access_key=config.get_secret_access_key(),
                                                        security_token=config.get_security_token())
        self.should_run = True
        self.bytes_transferred = None
        self.volume_id = None
        self.task_thread = None

    def get_task_id(self):
        return self.task_id

    def get_task_type(self):
        return self.task_type

    def run_task(self):
        raise NotImplementedError()

    def cancel_cleanup(self):
        raise NotImplementedError()

    def process_task(self):
        self.task_thread = TaskThread(self.run_task)
        self.task_thread.start()
        while self.task_thread.is_alive():
            time.sleep(self.EXTANT_STATUS_REPORT_INTERVAL)
            if not self.report_running():  # cancelled by imaging service
                log.debug('task is cancelled by imaging service', self.task_id)
                self.cancel()
        if not self.is_cancelled():
            if self.task_thread.get_result() == TASK_DONE:
                self.report_done()
                return True
            else:
                self.report_failed(self.task_thread.get_result())
                return False
        else:
            return True

    def cancel(self):
        #set should_run=False (to stop the task thread)
        self.should_run = False
        if self.task_thread:
            self.task_thread.join()  # wait for the task thread to release
        try:
            self.cancel_cleanup()  # any task specific cleanup
        except Exception, err:
            log.warn('Failed to cleanup task after cancellation: %s' % err, self.task_id)

    def is_cancelled(self):
        return not self.should_run

    def report_running(self):
        return self.is_conn.put_import_task_status(self.task_id, ImagingTask.EXTANT_STATE, self.volume_id,
                                                   self.bytes_transferred)

    def report_done(self):
        self.is_conn.put_import_task_status(self.task_id, ImagingTask.DONE_STATE, self.volume_id,
                                            self.bytes_transferred)

    def report_failed(self, error_code):
        self.is_conn.put_import_task_status(self.task_id, ImagingTask.FAILED_STATE, self.volume_id,
                                            self.bytes_transferred, error_code)

    """
    param: instance_import_task (object representing ImagingService's message)
    return: ImagingTask
    """

    @staticmethod
    def from_import_task(import_task):
        if not import_task:
            return None
        task = None
        if import_task.task_type == "import_volume" and import_task.volume_task:
            volume_id = import_task.volume_task.volume_id
            manifests = import_task.volume_task.image_manifests
            manifest_url = None
            if manifests and len(manifests) > 0:
                manifest_url = manifests[0].manifest_url

            # there should be kinda hierarchy
            task = PadImagingTask(import_task.task_id, manifest_url, volume_id)
            #ec2_cert = import_task.volume_task.ec2_cert.decode('base64')
            #ec2_cert_path = '%s/cloud-cert.pem' % config.RUN_ROOT
            #ssl.write_certificate(ec2_cert_path, ec2_cert)
        return task


import ConversionPipelineGenerator

class PadImagingTask(ImagingTask):
    def __init__(self, task_id, manifest_url=None, volume_id=None):
        ImagingTask.__init__(self, task_id, "import_volume")
        self.manifest_url = manifest_url
        self.instance_id = config.get_worker_id()
        self.process = None

    def __repr__(self):
        return 'Pad volume conversion task:%s' % self.task_id

    def __str__(self):
        return ('Pad Task: {0}, manifest url: {1}'
                .format(self.task_id, self.manifest_url))

    def run_task(self):
        device_to_use = None
        try:
            manifest = self.__getManifest()
            image_size = int(manifest['import'].size.text)

            #We got manifest, now let's create a pipeline
            generator = ConversionPipelineGenerator.ConversionPipelineGenerator()
            (source_reader , destination) = generator.generateConversionPipeline(manifest , config , self)
            destination.open() #what kind of configs do we need here?

            if source_reader.decodeAvailable():
                while 1:
                    (chunk , offset) = source_reader.getNextDecodedChunk()
                    if chunk:
                        destination .writeData(chunk , offset)
                    else:
                        break
            else:
                chunk_index = 0
                max_index = source_reader.getRawChunkCount()
                offset = 0
                while chunk_index < max_index:
                    data = source_reader.getRawDataChunk(chunk_index)
                    destination.writeData(data , offset)
                    chunk_index = chunk_index + 1
                    offset = offset + len(data)
            
            destination.confirm()
        except Exception as err:
            log.error('Failed to execute task task after cancellation: %s' % err, self.task_id)
        finally:
            destination.close()

    def __getManifest(self):
        if "imaging@" not in self.manifest_url:
            raise FailureWithCode('Invalid manifest URL', INPUT_DATA_FAILURE)
        resp, content = httplib2.Http().request(self.manifest_url.replace('imaging@', ''))
        if resp['status'] != '200' or len(content) <= 0:
            raise FailureWithCode('Could not download the manifest file', DOWNLOAD_DATA_FAILURE)
        root = objectify.XML(content)
        return root

  


class VolumeImagingTask(ImagingTask):
    _GIG_ = 1073741824

    def __init__(self, task_id, manifest_url=None, volume_id=None):
        ImagingTask.__init__(self, task_id, "import_volume")
        self.manifest_url = manifest_url
        self.ec2_conn = ws.connect_ec2(
            aws_access_key_id=config.get_access_key_id(),
            aws_secret_access_key=config.get_secret_access_key(),
            security_token=config.get_security_token())
        self.volume = None
        self.volume_id = volume_id
        if self.volume_id:
            self.volume = self.ec2_conn.conn.get_all_volumes([self.volume_id,'verbose'])
        if not self.volume:
            raise ValueError('Request for volume:"{0}" returned:"{1}"'
                             .format(volume_id, str(self.volume)))
        self.volume = self.volume[0]
        self.volume_attached_dev = None
        self.instance_id = config.get_worker_id()
        self.process = None

    def __repr__(self):
        return 'volume conversion task:%s' % self.task_id

    def __str__(self):
        return ('Task: {0}, manifest url: {1}, volume id: {2}'
                .format(self.task_id, self.manifest_url, self.volume.id))

    def get_partition_size(self, partition):
        p = subprocess.Popen(["sudo", "blockdev", "--getsize64", partition], stdout=subprocess.PIPE)
        t = p.communicate()[0]
        log.debug('The blockdev reported %s for %s' % (t.rstrip('\n'), partition), self.task_id)
        return int(t.rstrip('\n'))

    def add_write_permission(self, partition):
        log.debug('Setting permissions for %s' % partition)
        subprocess.call(["sudo", "chmod", "a+w", partition])

    def get_manifest(self):
        if "imaging@" not in self.manifest_url:
            raise FailureWithCode('Invalid manifest URL', INPUT_DATA_FAILURE)
        resp, content = httplib2.Http().request(self.manifest_url.replace('imaging@', ''))
        if resp['status'] != '200' or len(content) <= 0:
            raise FailureWithCode('Could not download the manifest file', DOWNLOAD_DATA_FAILURE)
        root = objectify.XML(content)
        return root

    def next_device_name(self, all_dev):
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

    def attach_volume(self, local_dev_timeout=120):
        new_device_name = None
        if not self.volume:
            raise FailureWithCode('This import does not have a volume', INPUT_DATA_FAILURE)
        instance_id = self.instance_id
        devices_before = get_block_devices()
        device_name = self.next_device_name(devices_before)
        log.debug('Attaching volume {0} to {1} as {2}'.
                         format(self.volume.id, instance_id, device_name), self.task_id)
        self.ec2_conn.attach_volume_and_wait(self.volume.id,
                                             instance_id,
                                             device_name)
        elapsed = 0
        start = time.time()
        while elapsed < local_dev_timeout and not new_device_name:
            new_block_devices = get_block_devices()
            log.debug('Waiting for local dev for volume: "{0}", '
                             'elapsed:{1}'.format(self.volume.id, elapsed), self.task_id)
            diff_list = list(set(new_block_devices) - set(devices_before))
            if diff_list:
                for dev in diff_list:
                    # If this is virtio attempt to verify vol to dev mapping
                    # using serial number field info
                    if not os.path.basename(dev).startswith('vd'):
                        try:
                            self.verify_virtio_volume_block_device(
                                volume_id=self.volume.id,
                                blockdev=dev)
                        except ValueError, ex:
                            raise FailureWithCode(ex, ATTACH_VOLUME_FAILURE)
                    new_device_name = dev
                    break
            elapsed = time.time() - start
            if elapsed < local_dev_timeout:
                time.sleep(2)
        if not new_device_name:
            raise FailureWithCode('Could find local device for volume:"%s"' % self.volume.id, ATTACH_VOLUME_FAILURE)
        self.volume_attached_dev = new_device_name
        return new_device_name

    def verify_virtio_volume_block_device(self,
                                          volume_id,
                                          blockdev,
                                          syspath='/sys/block/'):
        '''
        Attempts to verify a given volume id to a local block device when
        using kvm. In eucalyptus the serial number provides the volume
        id and the requested block device mapping in the
        format: vol-<id>-<dev name>.
        Example: "vol-abcd1234-dev-vdf"
        :param volume_id: string volume id. example. vol-abcd1234
        :param blockdev: block device. Example 'vdf' or '/dev/vdf'
        :param syspath: option dir to begin looking for dev serial num to map
        '''
        if not blockdev.startswith('vd'):
            return
        for devdir in os.listdir(syspath):
            serialpath = os.path.join(syspath + devdir + '/serial')
            if os.path.isfile(serialpath):
                with open(serialpath) as devfile:
                    serial = devfile.read()
                if serial.startswith(volume_id):
                    break
        if os.path.basename(blockdev) == devdir:
            log.debug('Validated volume:"{0}" at dev:"{1}" '
                             'via serial number: '
                             .format(volume_id, blockdev), self.task_id)
            return
        else:
            raise ValueError('Device for volume: {0} could not be verfied'
                             ' against dev:{1}'.format(volume_id, blockdev))

    # errors are catch by caller
    def start_download_process(self, device_name):
        manifest = self.manifest_url.replace('imaging@', '')
        cloud_cert_path = '%s/cloud-cert.pem' % config.RUN_ROOT
        params = ['/usr/libexec/eucalyptus/euca-run-workflow',
                  'down-parts/write-raw',
                  '--import-manifest-url', manifest,
                  '--output-path', device_name,
                  '--cloud-cert-path', cloud_cert_path]
        log.debug('Running %s' % ' '.join(params), self.task_id)
        # create process with system default buffer size and make its stderr non-blocking
        self.process = subprocess.Popen(params, stderr=subprocess.PIPE)
        fd = self.process.stderr
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    #TODO: Not in use, remove?
    def detach_volume(self, timeout_sec=3000, local_dev_timeout=30):
        log.debug('Detaching volume %s' % self.volume.id, self.task_id)
        if self.volume is None:
            raise FailureWithCode('This import does not have volume id', INPUT_DATA_FAILURE)
        devices_before = get_block_devices()
        self.volume.update()
        # Do not attempt to detach a volume which is not attached/attaching, or
        # is not attached to this instance
        this_instance_id = config.get_worker_id()
        attached_state = self.volume.attachment_state()
        if not attached_state \
                or not attached_state.startswith('attach') \
                or (hasattr(self.volume, 'attach_data')
                    and self.volume.attach_data.instance_id != this_instance_id):
            self.volume_attached_dev = None
            return True
        # Begin detaching from this instance
        if not self.ec2_conn.detach_volume_and_wait(self.volume.id, timeout_sec=timeout_sec, task_id=self.task_id):
            raise FailureWithCode('Can not detach volume %s' % self.volume.id, DETACH_VOLUME_FAILURE)
        # If the attached dev is known, verify it is no longer present.
        if self.volume_attached_dev:
            elapsed = 0
            start = time.time()
            devices_after = devices_before
            while elapsed < local_dev_timeout:
                new_block_devices = get_block_devices()
                devices_after = list(set(devices_before) - set(new_block_devices))
                if not self.volume_attached_dev in devices_after:
                    break
                else:
                    time.sleep(2)
                elapsed = time.time() - start
            if self.volume_attached_dev in devices_after:
                self.volume.update()
                log.error('Volume:"{0}" state:"{1}". Local device:"{2}"'
                                 'found on guest after {3} seconds'
                                 .format(self.volume.id,
                                         self.volume.status,
                                         self.volume.local_blockdev,
                                         timeout_sec), self.task_id)
                return False
        return True

    def run_task(self):
        device_to_use = None
        try:
            manifest = self.get_manifest()
            image_size = int(manifest.image.size)



            #if self.volume is not None:
            #    if long(int(self.volume.size) * self._GIG_) < image_size:
            #        raise FailureWithCode('Volume:"{1}" size:"{1}" is too small '
            #                         'for image to be processed:"{2}"'
            #                         .format(self.volume.id,
            #                                 self.volume.size,
            #                                 image_size), INPUT_DATA_FAILURE)
            #    if self.is_cancelled():
            #        return TASK_CANCELED
            #    log.info('Attaching volume %s' % self.volume.id, self.task_id)
            #    device_to_use = self.attach_volume()
            #    log.debug('Using %s as destination' % device_to_use, self.task_id)
            #    device_size = self.get_partition_size(device_to_use)
            #    log.debug('Attached device size is %d bytes' % device_size, self.task_id)
            #    log.debug('Needed for image/volume %d bytes' % image_size, self.task_id)
            #    if image_size > device_size:
            #        raise FailureWithCode('Device is too small for the image/volume', INPUT_DATA_FAILURE)
            #    try:
            #        self.add_write_permission(device_to_use)
            #        if self.is_cancelled():
            #            return TASK_CANCELED
            #        self.start_download_process(device_to_use)
            #    except Exception, err:
            #        log.error('Failure to start workflow process %s' % err)
            #        return WORKFLOW_FAILURE
            #    if self.process is not None:
            #        self.wait_with_status(self.process)
            #    else:
            #        log.error('Cannot start workflow process')
            #        return WORKFLOW_FAILURE
            #    if self.process.returncode is None:
            #        if self.is_cancelled():
            #            return TASK_CANCELED
            #        else:
            #            log.error('Process was killed')
            #            return WORKFLOW_FAILURE
            #    elif self.process.returncode != 0:
            #        log.error('Return code from the workflow process is not 0. Code: %d' % self.process.returncode)
            #       return WORKFLOW_FAILURE
            #else:
            #    log.error('No volume id is found for import-volume task')
            #    return INPUT_DATA_FAILURE

            return TASK_DONE

        except Exception, err:
            tb = traceback.format_exc()
            log.error(str(tb) + '\nFailed to process task: %s' % err, self.task_id)
            if type(err) is FailureWithCode:
                return err.failure_code
            else:
                return GENERAL_FAILURE

        finally:
            if device_to_use is not None and self.volume_id:
                log.info('Detaching volume %s' % self.volume_id, self.task_id)
                try:
                    self.ec2_conn.detach_volume_and_wait(volume_id=self.volume_id, task_id=self.task_id)
                except Exception:
                    return DETACH_VOLUME_FAILURE

    def wait_with_status(self, process):
        log.debug('Waiting for download process', self.task_id)
        while not self.is_cancelled() and process.poll() is None:
            try:
                 # get bytes transferred
                line = process.stderr.readline()
                if line:
                    line = line.strip()
                    try:
                        res = json.loads(line)
                        self.bytes_transferred = res['status']['bytes_downloaded']
                    except Exception, ex:
                        log.warn(
                            "Download image subprocess reports invalid status. Output: %s. Error: %s" % (line, ex),
                            self.task_id)
                    if self.bytes_transferred:
                        log.debug("Status %s, bytes transferred: %d" % (line, self.bytes_transferred), self.task_id)
            except:
                pass

    # don't catch exceptions since they should be catch by the caller
    def cancel_cleanup(self):
        if self.process and self.process.poll() is None:
            self.process.kill()