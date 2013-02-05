# Video handling utilities.
#
# Two things in this file
# 1. Kelvinator - simple method of extracting key frames for a video.  
# 2. Resize - simple transcoder to create smaller versions of videos.
#
# Requirements:
# 1. ffmpeg 
# 2. x264 for transcoding
# 3. Python Imaging: PIL, or Image
#
# For more info on transcoding, see: 
# - http://ffmpeg.org/trac/ffmpeg/wiki/x264EncodingGuide
# size abbreviations: 
# - http://linuxers.org/tutorial/how-extract-images-video-using-ffmpeg

import sys
import os
from django.core.files.storage import default_storage
from django.conf import settings
import subprocess
import math
import operator
import Image
import time
from celery import task
from utility import *
import numpy as np
import shutil


class FilePath():
    def __init__(self, raw=None, prefix=None, suffix=None, video_id=None, basename=None):
        if raw:
            self.raw = raw
            self.store_path = urlparse.urlsplit(raw).geturl()
            sp = store_path.split("/")
            self.prefix = sp[-5]
            self.suffix = sp[-4]
            self.video_id = sp[-2]
            self.basename = sp[-1]
        else:
            assert prefix
            assert suffix
            assert video_id
            assert basename
            

    def local(self):
        return self.raw

    def remote(self):
        return self.raw

    
def create_directory(store_path, store_loc):
    # Filesystem API is different than remote API
    if store_loc == 'local':
        root = getattr(settings, 'MEDIA_ROOT')
        store_path = os.path.join(root, store_path)
        if default_storage.exists(store_path):
            logging.info("Found prior directory, removing: %s" % store_path)
            dirRemove(store_path) 
        os.mkdir(store_path)
    else:
        store_path = prefix + "/" + suffix + "/videos/" + str(video_id) + "/jpegs"
        default_storage.delete(store_path)

class Kelvinator():
    def __init__(self, task_name, file_path, notify_addr=None):
        self.file_path = file_path
        self.email = None
        if notify_addr:
            self.email = logging.handlers.SMTPHandler(
                fromaddr = 'noreply@class.stanford.edu',
                toaddrs = [notify_addr],
                subject = "%s result: %s %s %s" % (task, file_path.prefix, file_path.suffix, file_path.video_filename))
            self.email.handle("course: %s %s" % (file_path.prefix, file_path.suffix))
            self.email.handle("video file: %s" % file_path.raw)
            self.email.handle("machine: %s" % socket.gethostname())
            self.email.handle("\n-------- LOG --------\n")


    def report(self):
        if self.email:
            self.email.emit()
            self.email = None

    @staticmethod
    def _extract(working_dir, jpeg_dir, video_file, start_offset, extraction_frame_rate):
        logging.info("Kicking off ffmpeg, hold onto your hats")
        cmdline = [ ffmpeg_cmd() ]
        cmdline += \
            [ '-i', working_dir + "/" + video_file, # input
              '-ss', str(start_offset),             # start a few seconds late
              '-r', str(extraction_frame_rate),     # thumbs per second to extract
              '-f', 'image2',                       # thumb format
              jpeg_dir + '/img%5d.jpeg',            # thumb filename template
            ]
        logging.info("EXTRACT: " + " ".join(cmdline))
        returncode = subprocess.call(cmdline)

        if returncode == 0:
            logging.info("ffmpeg completed, returncode %d" % returncode)
        else:
            logging.error("ffmpeg completed, returncode %d" % returncode)
            cleanup_working_dir(working_dir)
            raise VideoError("ffmpeg error %d" % returncode)


    # from http://mail.python.org/pipermail/image-sig/1997-March/000223.html
    @staticmethod
    def _computeDiff(file1, file2):
        h1 = Image.open(file1).histogram()
        h2 = Image.open(file2).histogram()
        rms = math.sqrt(reduce(operator.add, map(lambda a,b: (a-b)**2, h1, h2))/len(h1))
        return rms

    @staticmethod
    def _localMaximum(candidates, threshold):
        '''
        Return local maximum values from an array that are also larger
        the threshold. Local maximum is extracted by comparing current
        with three neighbors in both directions.
        '''
        cuts = [];
        for i in range(3, len(candidates)-4):
            score = candidates[i][0]
            if ( score > threshold and
                 score > candidates[i-1][0] and
                 score > candidates[i-2][0] and
                 score > candidates[i-3][0] and
                 score > candidates[i+1][0] and
                 score > candidates[i+2][0] and
                 score > candidates[i+3][0] ):
                cuts.append(candidates[i])
        return cuts


    def _difference(self, working_dir, jpeg_dir, extraction_frame_rate, frames_per_minute_target):
        """
        Sherif's method for figuring out what frames to keep from the candidate
        set to reach the targeted number of slides.
        """

        image_list = os.listdir(jpeg_dir)
        if len(image_list) == 0:
            raise VideoError("Failed to extract keyframes from video file")

        image_list.sort()
        logging.info("Extraction frame rate: %d fps" % extraction_frame_rate)
        duration = len(image_list) / extraction_frame_rate    # in seconds
        logging.info("Video duration: %d seconds" % duration)
        logging.info("Initial keyframes: %d" % len(image_list))
        logging.info("Target keyframes per minute: %d" % frames_per_minute_target) 
        max_keyframes = int(math.ceil(frames_per_minute_target * duration/60.0))
        logging.info("Upper bound on number of keyframes kelvinator will output: %d" % max_keyframes)
        logging.info("Internal differencing threshold: average of all scores")
        image_num = len(image_list)

        # window size, all the frames in the window centered at 
        # current frame are used to determine shot boundary. 
        # this value doesn't need to be changed for different videos.
        k = 5

        # calculate difference matrix
        difference_matrix = np.zeros((image_num,image_num))
        for i in range(0, image_num-1-k):
            for j in range(i+1, i+k):
                difference_matrix[i, j] = self._computeDiff(jpeg_dir+"/"+image_list[i], jpeg_dir+"/"+image_list[j])
        difference_matrix = difference_matrix + difference_matrix.transpose()

        # calculate shot boundary scores for each frames, score = cut(A,B)/associate(A) + cut(A,B)/associate(B) 
        candidates = []
        for i in range(k, image_num-1-k):
            cutAB = np.sum(difference_matrix[i-k:i, i:i+k])
            assocA = np.sum(difference_matrix[i-k:i, i-k:i])
            assocB = np.sum(difference_matrix[i:i+k, i:i+k])
            if (assocA!=0) and (assocB!=0):
                score = cutAB/assocA + cutAB/assocB
            else:
                score = 0
            candidates.append((score, i))

        # extract local maximum as the shot boundaries.
        # the threshold is assigned to be the mean of all the scores. 
        # [important] we may want to change the threshold to control the number of key frames. 
        # higher threshold generates fewer shot boundaries.
        threshold = np.mean([pair[0] for pair in candidates]);
        cuts = self._localMaximum(candidates, threshold)

        # limit shot boundary number fewer than max_keyframes
        if len(cuts) >= max_keyframes :
            # sort key frames by score 
            cuts.sort(reverse=True)
            cuts = cuts[:max_keyframes];

        # select the 3rd frame after each shot boundary as the key frame.
        # alternatively, we can also select middle frame between two shot boundaries as the key frame.
        cut_offset = 2

        # below code is used to output keyframes.
        keep_frames = []
        keep_times = []
        jpeg_dir_parent = os.path.abspath(os.path.join(jpeg_dir, os.path.pardir))
        jpeg_dir_result = os.path.join(jpeg_dir_parent, "tmp")
        if os.path.exists(jpeg_dir_result):
            pass
        else:
            os.mkdir(jpeg_dir_result)
        # sort key frames by index
        cuts.sort(key=lambda x: x[1])     

        # move key frames into a tmp folder, then move back.
        for i in range(len(cuts)):
            index = min(cuts[i][1] + cut_offset, len(image_list) - 1)
            shutil.move(jpeg_dir+"/"+image_list[index], jpeg_dir_result)
            keep_frames.append(image_list[index])
            keep_times.append(index)

        cleanup_working_dir(jpeg_dir)
        os.rename(jpeg_dir_result, jpeg_dir)  

        return (keep_frames, keep_times)


    def write_manifest(self, jpeg_dir, keep_frames, keep_times):
        outfile_name = os.path.join(jpeg_dir, "manifest.txt")
        outfile = open(outfile_name, 'w')
        index = 0
        outfile.write("{")

        while(index < len(keep_frames)):
            outfile.write("\"")
            toWrite = str(keep_times[index])
            outfile.write(toWrite)
            outfile.write("\":{\"imgsrc\":\"")
            outfile.write(keep_frames[index])
            outfile.write("\"}")
            if index < len(keep_frames)-1:
                outfile.write(",")
            index += 1
        outfile.write("}")
        outfile.close()


    def put_thumbs(self, jpeg_dir, prefix, suffix, video_id, store_loc):
        create_directory(os.path.join(prefix, suffix, "videos", str(video_id), "jpegs")
                         store_loc)

        # not doing write to tmp and then mv because of file storage API limitation
        image_list = os.listdir(jpeg_dir)
        image_list.sort()
        for fname in image_list:
            logging.info("Uploading: %s" % fname)
            local_file = open(jpeg_dir + "/" + fname, 'rb')
            store_file = default_storage.open(store_path + "/" + fname, 'wb')
            file_data = local_file.read()
            store_file.write(file_data)
            local_file.close()
            store_file.close()
        logging.info("Uploaded: %s files" % str(len(image_list)))


    # Main Kelvinator Task
    def kelvinate(frames_per_minute_target=2):
        """
        Given a path to a video in a readable S3 bucket, extract the frames and 
        upload back to S3.

        store_path must be the full path to the video file, not just to its parent folder.
        """

        logging.info("Kelvinate: extracting %s" % store_path_raw)

        extraction_frame_rate = 1   # seconds
        start_offset = 3            # seconds

        frames_per_minute_target = float(frames_per_minute_target)

        file_path = FilePath(store_path_raw)

        store_loc = "remote"
        if getattr(settings, 'AWS_ACCESS_KEY_ID') == "local":
            store_loc = "local"

        work_dir = None
        try:
            (work_dir, jpegs) = create_working_dirs("kelvinator", "jpegs")
            get_video(work_dir, file_path.video_filename, file_path.store_path)
            self._extract(work_dir, jpegs, file_path.video_filename, start_offset, extraction_frame_rate)
            (thumbs, times) = self._difference(work_dir, jpegs, extraction_frame_rate, frames_per_minute_target)
            self.write_manifest(jpegs, thumbs, times)
            self.put_thumbs(jpegs, course_prefix, course_suffix, file_path.video_id, store_loc)
        finally:
            cleanup_working_dir(work_dir)
            self.report()


    ##
    ##  RESIZE
    ##

    # video sizes we support: key is size name (used for target subdirectory) and value
    # are the parameters (as a list) that we'll pass to ffmpeg.
    sizes = { "large":  [ "-crf", "23", "-s", "1280x720" ],   # original size, compressed
              "medium": [ "-crf", "27", "-s", "852x480" ],    # wvga at 16:9
              "small":  [ "-crf", "30", "-s", "640x360" ],    
              "tiny":   [ "-crf", "40", "-s", "320x180" ],
            }


    # Actually transcode the video down 
    def do_resize(self, working_dir, target_dir, video_file, target_size):
        cmdline = [ ffmpeg_cmd() ]
        cmdline += [ "-i", working_dir + "/" + video_file,  # infile
                     "-c:v", "libx264",          # video codec
                     "-profile:v", "baseline",   # most compatible
                     "-strict", "-2",            # magic to allow aac audio enc
                   ]
        cmdline += sizes[target_size]
        cmdline += [ target_dir + "/" + video_file ]  # outfile

        logging.info("RESIZE: " + " ".join(cmdline))
        returncode = subprocess.call(cmdline)

        if returncode == 0:
            logging.info("completed with returncode %d" % returncode)
        else:
            errorLog(self, "completed with returncode %d" % returncode)
            raise VideoError("do_resize error %d" % returncode)


    def upload(target_dir, target_part, prefix, suffix, video_id, video_file, store_loc):
        create_directory(store_loc)

        statinfo = os.stat(target_dir + "/" + video_file)
        logging.info("Final file size: %s" % str(statinfo.st_size))

        local_file = open(target_dir + "/" + video_file, 'rb')
        store_file = default_storage.open(store_path + "/" + video_file, 'wb')
        store_file.write(local_file.read())
        local_file.close()
        store_file.close()


    # Main Resize Task
    def resize(self, target):
        """
        Scale down video and save the result alongside the original
        video So we can provide different download options.

        Preset is either "small" or "large".
        """

        logging.info("Resize: converting %s version of %s" % (target, self.file_path.raw))

        target = target.lower()
        if target not in sizes.keys():
            VideoError("Target size \"%s\" not supported" % target)

        store_loc = 'remote'
        if getattr(settings, 'AWS_ACCESS_KEY_ID') == 'local':
            store_loc = 'local'

        work_dir = None
        try:
            (work_dir, smaller_dir) = create_working_dirs("resize", target)
            self.get_video(work_dir, video_file, store_path)
            self.do_resize(work_dir, smaller_dir, video_file, target)
            self.upload(smaller_dir, target, course_prefix, course_suffix, video_id, video_file, store_loc)
        finally:
            self.cleanup_working_dir(work_dir)
            self.report()


@task()
def kelvinate(store_path_raw, frames_per_minute_target=2, notify_addr=None):    
    kelvinator = Kelvinator(FilePath(store_path_raw), notify_addr)
    return kelvinator.kelvinate(frames_per_minute_target=2)

@task()
def resize(store_path_raw, target_raw, notify_addr=None):
    kelvinator = Kelvinator(FilePath(store_path_raw), notify_addr)
    return kelvinator.resize(target_raw)
