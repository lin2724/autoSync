import os
import sys
import threading
import thread
import time
import signal
import subprocess

from bypy.bypy import *


class Log:
    def __init__(self, log_file):
        self.fd = open(log_file, 'a+')
        self.lock = threading.Lock()
        print 'log file %s' % log_file
        pass

    def log(self, log_str):
        cur_time_str = time.asctime()
        self.lock.acquire()
        info = '%s  %s\n' % (cur_time_str, log_str)
        self.fd.write(info)
        self.fd.flush()
        self.lock.release()
        pass


def get_sub_dirs(dir_path):
    ret_list = list()
    if not os.path.exists(dir_path):
        return ret_list
    dir_objs = os.listdir(dir_path)
    for dir_obj in dir_objs:
        full_path = os.path.join(dir_path, dir_obj)
        if os.path.isdir(full_path):
            ret_list.append(full_path)
    return ret_list


dir_chn_mng = list()


def check_change(dir_cb, ret_list):
    last_m_time = dir_cb['last_modify_time']
    this_m_time = os.stat(dir_cb['dir_path']).st_mtime
    if this_m_time > last_m_time:
        ret_list.append(dir_cb['dir_path'])
        dir_cb['last_modify_time'] = this_m_time

    #  for add new sub dir cb
    sub_dirs = get_sub_dirs(dir_cb['dir_path'])
    exist_dirs = list()
    for sub_cb in dir_cb['sub_dir_cbs']:
        exist_dirs.append(sub_cb['dir_path'])
    for sub_dir in sub_dirs:
        if sub_dir not in exist_dirs:
            new_dir_cb = dict()
            new_dir_cb['dir_path'] = sub_dir
            new_dir_cb['last_modify_time'] = os.stat(sub_dir).st_mtime
            new_dir_cb['sub_dir_cbs'] = list()
            dir_cb['sub_dir_cbs'].append(new_dir_cb)
            ret_list.append(sub_dir)
            print 'Add new dir [%s] ' % sub_dir

    #  for delete dir who not exist anymore, and recuri check modify
    offset = 0
    for idx in range(len(dir_cb['sub_dir_cbs'])):
        dir_path = dir_cb['sub_dir_cbs'][idx - offset]['dir_path']
        if not os.path.exists(dir_path):
            print 'Del dir [%s]' % dir_path
            dir_cb['sub_dir_cbs'].pop(idx - offset)
            offset += 1
        else:
            check_change(dir_cb['sub_dir_cbs'][idx - offset], ret_list)
        pass
    pass


def check_new_files(dir_cb, ret_list):
    pass


def test_check():
    if len(sys.argv) == 2:
        check_dir = sys.argv[1]
    else:
        check_dir = os.getcwd()
    new_dir_cb = dict()
    new_dir_cb['dir_path'] = check_dir
    new_dir_cb['last_modify_time'] = os.stat(check_dir).st_mtime
    new_dir_cb['sub_dir_cbs'] = list()
    while True:
        check_list = list()
        check_change(new_dir_cb, check_list)
        print check_list
        print 'Done'
        time.sleep(1)
    pass


def check_modify_dir(dir_path):
    old_dir = False
    modify_dir_list = list()
    idx = -1
    for idx, dir_cb in enumerate(dir_chn_mng):
        if dir_cb['dir_path'] == dir_path:
            top_dir_cb = dir_cb
            old_dir = True
            break
    if os.path.exists(dir_path):
        if old_dir:
            dir_chn_mng.pop(idx)
        return
    if not old_dir:
        top_dir_cb = dict()
        top_dir_cb['dir_path'] = dir_path
        stat = os.stat(dir_path)
        top_dir_cb['last_modify_time'] = stat.st_mtime
        top_dir_cb['sub_dir_cbs'] = list()
    dir_cb = dict()
    sub_objs = os.listdir(dir_path)
    for obj in sub_objs:
        full_path = os.path.join(dir_path, obj)
        if os.path.isdir(full_path):
            pass

    pass


def sync_thread():
    pass


class SyncDBHandle:
    def __init__(self):
        pass

    pass


class SyncHandle:
    def __init__(self):
        self.set_thread_max = 10
        self.thread_lock = threading.Lock()
        self.ByPyHandle = ByPy
        self.LogHandle = Log(os.path.join(os.getcwd(), __file__+'.log'))  # log int cur work path
        self.set_tmp_folder = os.path.join(os.getcwd(), __file__+'_tmp')
        self.set_failed_tmp_folder = os.path.join(os.getcwd(), __file__ + '_failed_tmp')

        self.folder_watch_list_cbs = list()
        self.upload_complete_cnt = 0
        self.upload_size_cnt = 0
        self.up_queue_list = list()
        self.upload_speed = 0
        self.thread_list = list()
        # 0 normal 1 pause  2 stop  3 force terminated
        self.set_status = 0
        self.task_list_queue = list()

        self.thread_list_pro = list()

        self.do_init()
        pass

    def log_init(self):
        log_path = os.getcwd()
        self.LogHandle = Log(log_path)
        pass

    def do_init(self):
        try:
            result = const.ENoError
            setuphandlers()

            parser = getparser()
            args = parser.parse_args()
            dl_args = ''
            if not args.downloader_args:
                if const.DownloaderArgsEnvKey in os.environ:
                    dl_args = os.environ[const.DownloaderArgsEnvKey]
            else:
                prefixlen = len(const.DownloaderArgsIsFilePrefix)
                if args.downloader_args[:prefixlen] == const.DownloaderArgsIsFilePrefix:  # file
                    with io.open(args.downloader_args[prefixlen:], 'r', encoding='utf-8') as f:
                        dl_args = f.read().strip()
                else:
                    dl_args = args.downloader_args

            # house-keeping reminder
            # TODO: may need to move into ByPy for customized config dir
            if os.path.exists(const.HashCachePath):
                cachesize = getfilesize(const.HashCachePath)
                if cachesize > 10 * const.OneM or cachesize == -1:
                    pr((
                           "*** WARNING ***\n"
                           "Hash Cache file '{0}' is very large ({1}).\n"
                           "This may affect program's performance (high memory consumption).\n"
                           "You can first try to run 'bypy.py cleancache' to slim the file.\n"
                           "But if the file size won't reduce (this warning persists),"
                           " you may consider deleting / moving the Hash Cache file '{0}'\n"
                           "*** WARNING ***\n\n\n").format(const.HashCachePath, human_size(cachesize)))

            # check for situations that require no ByPy object creation first
            if args.clean >= 1:
                return clean_prog_files(args.clean, args.verbose, args.configdir)

            # some arguments need some processing
            try:
                slice_size = interpret_size(args.slice)
            except (ValueError, KeyError):
                pr("Error: Invalid slice size specified '{}'".format(args.slice))
                return const.EArgument

            try:
                chunk_size = interpret_size(args.chunk)
            except (ValueError, KeyError):
                pr("Error: Invalid slice size specified '{}'".format(args.slice))
                return const.EArgument

            cached.usecache = not args.forcehash
            bypyopt = {
                'slice_size': slice_size,
                'dl_chunk_size': chunk_size,
                'verify': args.verify,
                'retry': args.retry,
                'timeout': args.timeout,
                'quit_when_fail': args.quit,
                'resumedownload': args.resumedl,
                'incregex': args.incregex,
                'ondup': args.ondup,
                'followlink': args.followlink,
                'checkssl': args.checkssl,
                'cacerts': args.cacerts,
                'rapiduploadonly': args.rapiduploadonly,
                'mirror': args.mirror,
                'selectmirror': args.selectmirror,
                'configdir': args.configdir,
                'resumedl_revertcount': args.resumedl_revertcount,
                'downloader': args.downloader,
                'downloader_args': dl_args,
                'verbose': args.verbose,
                'debug': args.debug}
            if Pool:
                bypyopt['processes'] = args.processes

            # we construct a ByPy object here.
            # if you want to try PanAPI, simply replace ByPy with PanAPI, and all the bduss related
            # function _should_ work
            # I didn't use PanAPI here as I have never tried out those functions inside
            self.ByPyHandle = ByPy(**bypyopt)
        except KeyboardInterrupt:
            # handle keyboard interrupt
            pr("KeyboardInterrupt")
            pr("Abort")
        except Exception as ex:
            # NOTE: Capturing the exeption as 'ex' seems matters, otherwise this:
            # except Exception ex:
            # will sometimes give exception ...
            perr("Exception occurred:\n{}".format(formatex(ex)))
            pr("Abort")
            raise

    def run(self):
        self.set_status = 0
        self.thread_start()
        pass

    def stop(self):
        self.set_status = 2
        pass

    def pause(self):
        pass

    def kill(self):
        pass

    def add_folder(self, folder_path):
        new_dir_cb = dict()
        new_dir_cb['dir_path'] = folder_path
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        new_dir_cb['last_modify_time'] = os.stat(folder_path).st_mtime
        new_dir_cb['sub_dir_cbs'] = list()
        self.folder_watch_list_cbs.append(new_dir_cb)
        pass

    def list_folder(self):
        pass

    def thread_get_speed(self):
        pass

    def thread_collect_task(self):
        min_check_len = 200
        change_list = list()
        cur_base_folder = ''
        self.load_task_from_tmp()
        while True:

            self.thread_lock.acquire()

            exist_cnt = 0
            ret_folder = ''
            file_list = list()

            try:

                for task in self.task_list_queue:
                    exist_cnt += len(task['files'])
                if exist_cnt < min_check_len:
                    if not len(change_list):
                        for folder_cb in self.folder_watch_list_cbs:
                            check_change(folder_cb, change_list)
                            if len(change_list):
                                # use watch folder as base folder!
                                # print 'folder_cb path [%s]' % ret_folder
                                cur_base_folder = folder_cb['dir_path']
                                if cur_base_folder[:-1] == '/' or cur_base_folder[:-1] == '\\':
                                    cur_base_folder = cur_base_folder[:-1]
                                break
                    offset = 0
                    for idx in range(len(change_list)):
                        objs = os.listdir(change_list[idx - offset])
                        have_new_file = False
                        for obj in objs:
                            full_path = os.path.join(change_list[idx - offset], obj)
                            if os.path.isfile(full_path):
                                tmp_path = self.move_to_tmp(full_path, cur_base_folder)
                                file_list.append(tmp_path)
                                have_new_file = True
                        if not have_new_file:
                            change_list.pop(idx - offset)
                            offset += 1
                        if len(file_list):
                            detail_folder = change_list[idx - offset][:]
                            if len(cur_base_folder) > len(detail_folder):
                                self.LogHandle.log('ERROR: Base folder [%s], detail_folder[%s]' % (cur_base_folder, detail_folder))
                            else:
                                detail_path = detail_folder[len(cur_base_folder):]
                                if detail_path[:1] == '/' or detail_path[:1] == '\\':
                                    detail_path = detail_path[1:]
                                base_dir = os.path.basename(cur_base_folder)
                                ret_folder = os.path.join(base_dir, detail_path)
                                ret_folder = self.set_tmp_folder
                            break
                        pass
                if len(file_list):
                    queue_dict = dict()
                    queue_dict['dir_path'] = ret_folder
                    queue_dict['files'] = file_list
                    self.task_list_queue.append(queue_dict)
                    print 'add queue to list, folder [%s], cnt [%d]' % (ret_folder, len(file_list))
                else:
                    print 'no new files to update cur queue [%d], file_cnt [%d]' % (len(self.task_list_queue), exist_cnt)
                    if not len(exist_cnt):
                        self.LogHandle.log('INFO:update queue empty,try collect from tmp and Failed folder')
                        # self.move_from_failed_to_tmp()
                        self.load_task_from_tmp()
                    # print change_list
            except:
                e = sys.exc_info()[0]
                self.LogHandle.log('FATAL:thread_collect_task [%s]' % e)

            self.thread_lock.release()
            if self.set_status == 2:
                self.LogHandle.log('Thread collect stop..')
                thread.exit()
            time.sleep(5)
        pass

    def get_upload_task(self):

        ret_folder = ''
        ret_file_list = list()
        set_once_limit = 20
        offset = 0

        self.thread_lock.acquire()

        try:
            #  load task which is not finished last time..
            for idx in range(len(self.task_list_queue)):
                #  clear empty task-queue
                if not len(self.task_list_queue[idx - offset]['files']):
                    self.task_list_queue.pop(idx - offset)
                    offset += 1
                else:
                    file_cnt = len(self.task_list_queue[idx - offset]['files'])
                    if set_once_limit > file_cnt:
                        task_file_cnt = file_cnt
                    else:
                        task_file_cnt = file_cnt
                    # collect files task
                    ret_folder = self.task_list_queue[idx - offset]['dir_path']
                    offset_f = 0
                    for idx_f in range(task_file_cnt):
                        ret_file_list.append(self.task_list_queue[idx - offset]['files'][idx_f - offset_f])
                        self.task_list_queue[idx - offset]['files'].pop(idx_f - offset_f)
                        offset_f += 1
                    break

                pass
        except:
            e = sys.exc_info()[0]
            self.LogHandle.log('FATAL:get_upload_task [%s]' % e)

        self.thread_lock.release()
        return ret_file_list, ret_folder
        pass

    def move_to_tmp_bk(self, file_path):
        if not os.path.exists(self.set_tmp_folder):
            os.mkdir(self.set_tmp_folder)
        file_name = os.path.basename(file_path)
        dst_path = os.path.join(self.set_tmp_folder, file_name)
        os.rename(file_path, dst_path)
        return dst_path
        pass

    def _mkdir_recursive(self, path):
        pardir = os.path.dirname(path)
        if pardir == path:
            return
        if os.path.exists(path):
            return
        if not os.path.exists(pardir):
            self._mkdir_recursive(pardir)
        os.mkdir(path)

    def _remove_dir_recursive(self, path):
        if os.path.exists(path):
            items = os.listdir(path)
            for item in items:
                full_path = os.path.join(path, item)
                if os.path.isfile(full_path):
                    return False
                if not self._remove_dir_recursive(full_path):
                    return False
            return True

    def move_to_tmp(self, file_path, base_folder):
        if len(file_path) < len(base_folder):
            self.LogHandle.log('ERROR:file-path short than base [%s] < [%s]' % (file_path, base_folder))
            return
        base_parent_folder = os.path.dirname(base_folder)
        #  store_short_path is relative folder-path correspond to it's watch folder
        store_short_path = file_path[len(base_parent_folder):]
        #  need to remote splash at front, or it will cause err when we join-path
        if store_short_path[:1] == '/' or store_short_path[:1] == '\\':
            store_short_path = store_short_path[1:]
        #  dst_path is where it been move to tmp-folder, and store,
        dst_path = os.path.join(self.set_tmp_folder, store_short_path)
        #  folder_path is dst_path's parent folder, create it in case it's not exist!
        folder_path = os.path.dirname(dst_path)
        if not os.path.exists(folder_path):
            self._mkdir_recursive(folder_path)
        os.rename(file_path, dst_path)
        return dst_path
        pass

    def move_to_failed_tmp(self, file_path, base_folder):

        if len(file_path) < len(base_folder):
            self.LogHandle.log('ERROR:file-path short than base [%s] < [%s]' % (file_path, base_folder))
            return

        self.thread_lock.acquire()
        dst_path = ''
        try:
            base_parent_folder = os.path.dirname(base_folder)
            #  store_short_path is relative folder-path correspond to it's watch folder
            store_short_path = file_path[len(base_parent_folder):]
            #  need to remote splash at front, or it will cause err when we join-path
            if store_short_path[:1] == '/' or store_short_path[:1] == '\\':
                store_short_path = store_short_path[1:]
            #  dst_path is where it been move to tmp-folder, and store,
            dst_path = os.path.join(self.set_failed_tmp_folder, store_short_path)
            #  folder_path is dst_path's parent folder, create it in case it's not exist!
            folder_path = os.path.dirname(dst_path)
            if not os.path.exists(folder_path):
                self._mkdir_recursive(folder_path)
            os.rename(file_path, dst_path)
        except:
            e = sys.exc_info()[0]
            self.LogHandle.log('FATAL:move_to_failed_tmp [%s]' % e)

        self.thread_lock.release()

        return dst_path
        pass

    def move_from_failed_to_tmp(self):
        if not os.path.exists(self.set_failed_tmp_folder):
            return
        self.thread_lock.acquire()
        try:
            if not os.path.exists(self.set_tmp_folder):
                self._mkdir_recursive(self.set_tmp_folder)
            objects = os.listdir(self.set_failed_tmp_folder)
            for object in objects:
                full_path = os.path.join(self.set_failed_tmp_folder, object)
                if not os.path.isdir(full_path):
                    continue
                command = ['mv', '-rf', full_path, self.set_tmp_folder]
                pipe = subprocess.Popen(args=command)
                ret = False
                while True:
                    if pipe.poll() is not None:
                        ret = pipe.returncode
                        break
                    time.sleep(0.2)
                if not ret:
                    self.LogHandle.log('Failed to move from failed to tmp [%s] [%s]' % (full_path, self.set_tmp_folder))
            pass
        except:
            e = sys.exc_info()[0]
            self.LogHandle.log('FATAL:move_from_failed_to_tmp [%s]' % e)
        self.thread_lock.release()
        pass

    def load_task_from_tmp(self):
        if not os.path.exists(self.set_tmp_folder):
            return
        items = os.listdir(self.set_tmp_folder)
        base_dirs_name = list()
        self.thread_lock.acquire()
        for item in items:
            full_path = os.path.join(self.set_tmp_folder, item)
            if os.path.isdir(full_path):
                base_dirs_name.append(item)
        for base_dir_name in base_dirs_name:
            new_queue_item = dict()
            new_queue_item['dir_path'] = self.set_tmp_folder
            new_queue_item['files'] = list()
            full_path = os.path.join(self.set_tmp_folder, base_dir_name)
            #  walk this folder, add all file to list
            for root, dirs, files in os.walk(full_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    new_queue_item['files'].append(file_path)
            #  this folder is empty, delete it
            if not len(new_queue_item['files']):
                if not self._remove_dir_recursive(full_path):
                    self.LogHandle.log('Failed to remove dir [%s]' % full_path)
                #  no file found, don't need this item
                del new_queue_item
            else:
                self.LogHandle.log('load from tmp [%s]' % new_queue_item['dir_path'])
                self.task_list_queue.append(new_queue_item)
        self.thread_lock.release()

        pass

    def push_back_task_upload(self, task_tup):
        #  not now...
        return
        (file_path, folder) = task_tup
        self.move_to_failed_tmp(file_path, folder)
        pass

    def thread_start(self):
        pro = threading.Thread(target=self.thread_collect_task)
        pro.start()
        pro = threading.Thread(target=self.thread_guide)
        pro.start()
        print 'start collect task'
        for i in range(self.set_thread_max):
            pro = threading.Thread(target=self.thread_do_upload)
            pro.start()
            self.thread_list_pro.append(pro)
            print 'start upload thread'
        pass

    def thread_guide(self):
        while True:
            alive_cnt = 0
            for idx, pro in enumerate(self.thread_list_pro):
                if not pro.is_alive() and not self.set_status:
                    self.LogHandle.log('One thread die!!')
                    self.thread_list_pro.pop(idx)
                    #  check if we need to quit..
                    if not self.set_status:
                        pro = threading.Thread(target=self.thread_do_upload)
                        pro.start()
                        self.thread_list_pro.append(pro)
                    break
                else:
                    alive_cnt += 1
            if self.set_status == 2:
                self.LogHandle.log('thread-guide stop.. ')
                thread.exit()
            if alive_cnt == self.set_thread_max:
                time.sleep(10)
            else:
                self.LogHandle.log('Alive thread Count [%d]' % alive_cnt)
                time.sleep(0.1)
        pass

    def thread_do_upload(self, on_delete=False):
        while True:
            file_size = 0
            time.sleep(1)
            file_path_list, up_folder = self.get_upload_task()
            if not len(file_path_list):
                print 'get no file to process..'
            for file_path in file_path_list:
                if self.set_status == 2:
                    self.LogHandle.log('Thread upload stop..')
                    thread.exit()
                remote_file_path = file_path[len(up_folder):]
                if os.path.exists(file_path):
                    stat = os.stat(file_path)
                    file_size = stat.st_size
                    ret = self.ByPyHandle.upload(file_path, remote_file_path)
                    if ret != const.ENoError:
                        self.LogHandle.log('Failed to upload [%s], ret [%d]' % (file_path, ret))
                        self.push_back_task_upload((file_path, up_folder))
                    else:
                        self.LogHandle.log('Succeed upload to [%s]' % remote_file_path)
                        os.remove(file_path)
                else:
                    self.LogHandle.log('File not exist! [%s]' % file_path)
            del file_path_list
            del up_folder
            if self.set_status == 2:
                self.LogHandle.log('Thread upload stop..')
                thread.exit()
            pass

    def _get_folder_files(self, folder_path):
        pass

    pass

gProgramQuit = False
gSyncMng = None


def signal_quit(signum, frame):
    global gProgramQuit
    gProgramQuit = True
    gSyncMng.stop()
    print 'SigRecved wait quiting...'
    pass


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_quit)
    sync_mng = SyncHandle()
    sync_mng.add_folder('/root/MountPoint/sda/ImgStoreTmp/img/gif_sep_new')
    sync_mng.add_folder('/root/MountPoint/sda/ImgStoreTmp/img/img_sep')
    sync_mng.run()
    gSyncMng = sync_mng
    while True:
        if gProgramQuit:
            sync_mng.stop()
            print 'Program quit...'
            break
        try:
            time.sleep(3)
            print 'running..'
        except:
            print 'Except keyboard quit...'
            sync_mng.stop()
            break
    # test_check()
