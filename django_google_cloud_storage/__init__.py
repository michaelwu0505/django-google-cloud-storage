"""
Google Cloud Storage file backend for Django
"""

import os
from datetime import datetime
import mimetypes
from django.conf import settings
from django.core.files.storage import Storage
from google.appengine.api.blobstore import create_gs_key
import cloudstorage as gcs
import sys
import StringIO
__author__ = "ckopanos@redmob.gr, me@rchrd.net"
__license__ = "GNU GENERAL PUBLIC LICENSE"


class GoogleCloudStorage(Storage):

    def __init__(self, location=None, base_url=None):
        if location is None:
            location = settings.GOOGLE_CLOUD_STORAGE_BUCKET
        self.location = location
        if base_url is None:
            base_url = settings.GOOGLE_CLOUD_STORAGE_URL
        self.base_url = base_url

    def _open(self, name, mode='r'):
        filename = self.location + "/" + name

        # rb is not supported
        if mode == 'rb':
            mode = 'r'

        if mode == 'w':
            type, encoding = mimetypes.guess_type(name)
            cache_control = settings.GOOGLE_CLOUD_STORAGE_DEFAULT_CACHE_CONTROL
            gcs_file = gcs.open(filename, mode=mode, content_type=type,
                                options={'x-goog-acl': 'public-read',
                                         'cache-control': cache_control})
        else:
            gcs_file = gcs.open(filename, mode=mode)

        return gcs_file

    def _save(self, name, content):
        filename = self.location + "/" + name
        type, encoding = mimetypes.guess_type(name)
        cache_control = settings.GOOGLE_CLOUD_STORAGE_DEFAULT_CACHE_CONTROL

        # Files are stored with public-read permissions.
        # Check out the google acl options if you need to alter this.
        gss_file = gcs.open(filename, mode='w', content_type=type,
                            options={'x-goog-acl': 'public-read',
                                     'cache-control': cache_control})
        try:
            content.open()
        except:
            pass
        gss_file.write(content.read())
        try:
            content.close()
        except:
            pass
        gss_file.close()

        return name

    def delete(self, name):
        filename = self.location+"/"+name
        try:
            gcs.delete(filename)
        except gcs.NotFoundError:
            pass

    def exists(self, name):
        try:
            self.statFile(name)
            return True
        except gcs.NotFoundError:
            return False

    def listdir(self, path=None):
        if path != "":
            path_prefix=self.location+"/"+path+"/"
        else:
            path_prefix=self.location+"/"
        bucketContents = gcs.listbucket(path_prefix=path_prefix, delimiter="/")

        directories, files = [], []
        for entry in bucketContents:
            if entry.filename == path_prefix:
                continue

            head, tail = os.path.split(entry.filename)
            if entry.is_dir:
                head, tail = os.path.split(os.path.normpath(entry.filename))
                directories.append(tail)
            else:
                head, tail = os.path.split(entry.filename)
                files.append(tail)

        return directories, files
                
    def size(self, name):
        stats = self.statFile(name)
        return stats.st_size

    def accessed_time(self, name):
        raise NotImplementedError

    def created_time(self, name):
        stats = self.statFile(name)
        return datetime.fromtimestamp(stats.st_ctime)

    def modified_time(self, name):
        return self.created_time(name)

    def url(self, name):
        server_software = os.getenv("SERVER_SOFTWARE", "")
        if not server_software.startswith("Google App Engine"):
            # we need this in order to display images, links to files, etc
            # from the local appengine server
            filename = "/gs" + self.location + "/" + name
            key = create_gs_key(filename)
            local_base_url = getattr(settings, "GOOGLE_CLOUD_STORAGE_DEV_URL",
                                     "http://localhost:8001/blobstore/blob/")
            return local_base_url + key + "?display=inline"
        return self.base_url + "/" + name

    def statFile(self, name):
        filename = self.location + "/" + name
        return gcs.stat(filename)

    def isdir(self, name):
        if name=="":
            return True

        if name!="":
            path_prefix = self.location + "/" + name + "/"
        else:
            path_prefix = self.location + "/"

        bucketContents = gcs.listbucket(path_prefix=path_prefix)
        for entry in bucketContents:
            return True

        return False

    def isfile(self, name):
        if self.exists(name):
            return True
        else:
            return False

    def makedirs(self, path):
        sio = StringIO.StringIO("")
        self._save(path+"/", sio)

    def rmtree(self, path):
        if path != "":
            path_prefix=self.location+"/"+path+"/"
        else:
            path_prefix=self.location+"/"
        bucketContents = gcs.listbucket(path_prefix=path_prefix)

        for entry in bucketContents:
            gcs.delete(entry.filename)

    def move(self, old_file_name, new_file_name, allow_overwrite=False):

        if self.isdir(old_file_name):
            raise Exception("Rename of directory '%s' is not supported." % old_file_name)

        if self.exists(new_file_name):
            if allow_overwrite:
                self.delete(new_file_name)
            else:
                raise Exception("The destination file '%s' exists and allow_overwrite is False" % new_file_name)

        #FIXME: There seems to be a bug in copy2? The data in __BlobInfo__ is NOT copied...
        #gcs.copy2(self.location + "/" + old_file_name, self.location + "/" + new_file_name)
        old_file = self._open(old_file_name)
        self._save(new_file_name, old_file)

        self.delete(old_file_name)

    def path(self, name):
        return None
