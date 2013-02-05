import sys, os, socket, shutil
import tempfile
import logging
import urllib, urllib2, urlparse

from django.core.files.storage import default_storage
from django.conf import settings
from django.core.mail import send_mail


logger = logging.getLogger(__name__)


def ffmpeg_cmd():
    return 'ffmpeg'


class VideoError(Exception):
    pass


def create_working_dirs(job, subdir_name):
    """
    Create local temp director where we will do our work.  If this fails, we log 
    something but otherwise let the exception go since we want it to fail violently 
    which will leave the job on the work queue.
    """

    working_dir_parent = getattr(settings, "KELVINATOR_WORKING_DIR", "/tmp")
    working_dir=tempfile.mkdtemp(prefix=job+"-", dir=working_dir_parent,
                                 suffix="-" + str(os.getpid()))
    logger.info("Working directory: " + working_dir)

    subdir = working_dir + "/" + subdir_name
    os.mkdir(subdir)
    if not os.path.isdir(subdir):
        raise VideoError("Unable to create dir: " + subdir)
    return (working_dir, subdir)

def cleanup_working_dir(working_dir):
    logging.info("Cleaning up working dir: " + working_dir)
    dirRemove(working_dir)

def dirRemove(path):
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)
        if os.path.isdir(path):
            raise VideoError("Unable to remove dir: %s" % path)

def get_video(working_dir, video_filename, source_path):
    logging.info("Source file: " + source_path)
    source_file = default_storage.open(source_path)
    logging.info("Original file size: %s" % str(default_storage.size(source_path)))

    video_file = working_dir + "/" + video_filename
    logging.info("Writing to working (local) file: " + video_file)
    working_file = open(video_file, 'wb')
    working_file.write(source_file.read())
    working_file.close()
    source_file.close()
    
    # TODO: consider some retry logic here
    try:
        filesize = os.path.getsize(video_file)
    except os.error as e:
        logging.error("Unable to download video file")
        cleanup_working_dir(working_dir)
        raise e

    if filesize == 0:
        cleanup_working_dir(working_dir)
        raise VideoError("file size zero, video file did not download properly")

