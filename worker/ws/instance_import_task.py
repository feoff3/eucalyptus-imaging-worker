from boto.resultset import ResultSet

def match_name(name, param):
    if name==param or 'euca:%s'%name == param:
        return True
    return False

class InstanceImportTask(object):
    def __init__(self, parent=None):
        self.task_id = None
        self.task_type = None
        self.volume_task = None
        self.instance_store_task = None

    def __repr__(self):
        return 'InstanceImportTask:%s' % self.task_id
 
    def startElement(self, name, attrs, connection): 
        if match_name('instanceStoreTask', name):
            self.instance_store_task = InstanceStoreTask()
            return self.instance_store_task
        elif match_name('volumeTask', name):
            self.volume_task = VolumeTask()
            return self.volume_task
        else:
            return None
        
    def endElement(self, name, value, connection):
        if match_name('importTaskId', name):
            self.task_id = value
        elif match_name('importTaskType',name):
            self.task_type = value
        else:
            setattr(self, name, value)

class InstanceStoreTask(object):
    def __init__(self, parent=None):
        self.accountId = None
        self.accessKey = None
        self.uploadPolicy = None
        self.s3Url = None
        self.ec2Cert = None
        self.import_images = []
        self.converted_image = None

    def startElement(self, name, attrs, connection): 
        if match_name('importImageSet',name):
            self.import_images =  ResultSet([('item', ImportImage)])
            return self.import_images
        elif match_name('convertedImage',name):
            self.converted_image = ConvertedImage()
            return self.converted_image
        else:
            return None

    def endElement(self, name, value, connection):
        if match_name('accountId',name):
            self.accountId = value
        elif match_name('accessKey',name):
            self.accessKey = value
        elif match_name('uploadPolicy',name):
            self.uploadPolicy = value
        elif match_name('s3Url',name):
            self.s3Url = value
        elif match_name('ec2Cert',name):
            self.ec2Cert = value
        else:
            setattr(self, name, value)

class VolumeTask(object):
    def __init__(self, parent=None):
        self.volume_id = None
        self.image_manifests = []

    def startElement(self, name, attrs, connection): 
        if match_name('imageManifestSet', name):
            self.image_manifests =  ResultSet([('item', ImageManifest)])
            return self.image_manifests
        else:
            return None

    def endElement(self, name, value, connection):
        if match_name('volumeId', name):
            self.volume_id = value
        else:
            setattr(self, name, value)

class ImportImage(object):
    def __init__(self, parent=None):
        self.id = None
        self.format = None
        self.bytes = 0L
        self.download_manifest_url = None

    def startElement(self, name, attrs, connection): 
        pass

    def endElement(self, name, value, connection):
        if match_name('id', name):
            self.id = value
        elif match_name('format', name):
            self.format = value
        elif match_name('bytes', name):
            self.bytes = long(value)
        elif match_name('downloadManifestUrl', name): 
            self.download_manifest_url = value
        else:
            setattr(self, name, value)
   
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return 'import-image (id=%s,format=%s,manifest_url=%s)' % (self.id, self.format, self.download_manifest_url)

class ConvertedImage(object):
    def __init__(self, parent=None):
        self.bucket = None
        self.prefix = None
        self.architecture = None
        self.image_id = None

    def startElement(self, name, attrs, connection): 
        pass

    def endElement(self, name, value, connection):
        if match_name('bucket', name):
            self.bucket = value
        elif match_name('prefix', name):
            self.prefix = value
        elif match_name('architecture', name):
            self.architecture = value
        elif match_name('imageId', name): 
            self.image_id = value
        else:
            setattr(self, name, value)
   
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return 'converted-image (bucket=%s,prefix=%s,architecture=%s)' % (self.bucket, self.prefix, self.architecture)
