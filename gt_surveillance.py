# -*- coding: utf-8 -*-
"""
Created on Thu Nov 05 16:44:30 2015

@brief:  download TCGA bam files
@author: okada

$Id: gt_surveillance.py 85 2015-12-11 08:04:59Z aokada $
$Rev: 85 $

# before run
@code
export DRMAA_LIBRARY_PATH=/geadmin/N1GE/lib/lx-amd64/libdrmaa.so.1.0

# run
@code
gt_surveillance.py {path to working dir} {auth key} {manifest file} --config_file {option: config file}

* use --ignore_list option, skip download/
create this list, for example this command,

ls -l data/ | egrep -v .gto$ | egrep -v .partial$ | cut -f 13 -d ' ' > complete.txt
@endcode
"""

from multiprocessing import Process
import glob
import time
import datetime
import sys
import os
import drmaa
import ConfigParser
import argparse
import subprocess
import re

cmd_format = """
#!/bin/bash
#
# download TCGA bam file
#
#$ -S /bin/bash         # set shell in UGE
#$ -cwd                 # execute at the submitted dir
#$ -e {log}
#$ -o {log}
pwd                     # print current working directory
hostname                # print hostname
date                    # print date
set -xv

{gtdownload} -vv --max-children 4 -d {manifest} -p {download_dir} -c {key}
"""

splitter_format = """
#!/bin/bash
#
# split manifest
#

perl {xmlsplitter} {manifest} {output_prefix} 1
"""

def write_log(path, mode, text, date, printer):

    t = ""
    if date == True:
        now = datetime.datetime.now()
        t = "{0:0>4d}/{1:0>2d}/{2:0>2d} {3:0>2d}:{4:0>2d}:{5:0>2d}: ".format(
                                now.year, now.month, now.day, now.hour, now.minute, now.second)

    f = open(path, mode)
    f.write(t + text + "\n")
    f.close()

    if printer == True:
        print (t + text)


def qsub_process(name, output_dir, key, manifest, config):
    
    script_path = output_dir + "/scripts/" + name + ".sh"
    log_path = output_dir + "/log/" + name + ".log"
    
    cmd = cmd_format.format(gtdownload = config.get('TOOLS', 'gtdownload'), \
                            manifest = manifest, download_dir = output_dir + "/data",  \
                            key = key, log = output_dir + "/log")

    f_sh = open(script_path, "w")
    f_sh.write(cmd)
    f_sh.close()
    os.chmod(script_path, 0750)

    write_log(log_path, "w", name + ": Subprocess has been started, with script " + script_path, True, False)

    s = drmaa.Session()

    return_val = 1
    retry_max = config.getint('JOB_CONTROL', 'retry_max')
    for i in range(retry_max):
        
        s.initialize()
        jt = s.createJobTemplate()
        jt.jobName = "gt_surveilance"
        jt.outputPath = ':' + output_dir + '/log'
        jt.errorPath = ':' + output_dir + '/log'
        jt.nativeSpecification = config.get('JOB_CONTROL', 'qsub_option')
        jt.remoteCommand = script_path

        jobid = s.runJob(jt)
    
        write_log(log_path, "a", name + ": Job has been submitted with id: " + jobid, True, True)

        log_text = ""
        err = False
        
        try:
            wait_time = config.getint('JOB_CONTROL', 'wait_time')
            if wait_time == 0:
                wait_time = drmaa.Session.TIMEOUT_WAIT_FOREVER
                
            retval = s.wait(jobid, wait_time)
            log_text = 'with status: ' + str(retval.hasExited) + ' and exit status: ' + str(retval.exitStatus)
            if retval.exitStatus != 0 or retval.hasExited == False:
                err = True

        except Exception as e:
            s.control(jobid, drmaa.JobControlAction.TERMINATE)
            log_text = "with error " + e.message
            err = True
        
        write_log(log_path, "a", name + ": Job: " + str(jobid) + ' finished ' + log_text, True, True)

        s.deleteJobTemplate(jt)
        s.exit()

        if err == False:
            return_val = 0
            break
        
    write_log(log_path, "a", name + ": Subprocess has been finished: " + str(return_val), True, True)

    return return_val
    
def xml_splitter(name, output_dir, manifest, config):
    prefix = os.path.splitext(os.path.basename(manifest))[0]
    cmd = splitter_format.format(xmlsplitter = config.get('TOOLS', 'xmlsplitter'), \
                        output_prefix = output_dir + "/manifests/" + prefix, manifest = manifest)
                        
    script_path = output_dir + "/scripts/" + name + ".sh"    

    f_sh = open(script_path, "w")
    f_sh.write(cmd)
    f_sh.close()
    os.chmod(script_path, 0750)

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_data, stderr_data = p.communicate()

    li = []
    if p.returncode == 0:
        print "xml_splitter finish: %s" % (stdout_data)
        li = glob.glob(output_dir + "/manifests/" + prefix + "*")
    else:
        print "xml_splitter error!: %s" % (stderr_data)

    return li

def isbam_downloaded(manifest, bam_dir):
    f = open(manifest)
    text = f.read()
    f.close()
    analysis = re.split('<analysis_id>|</analysis_id>', text)[1]
    name = re.split('<filename>|</filename>', text)[1]
    bam = "%s/%s/%s" % (bam_dir, analysis, name)
 
    return os.path.exists(bam)
    
def main():
    name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    
    # get args
    parser = argparse.ArgumentParser(prog = name)
    
    parser.add_argument("--version", action = "version", version = name + "-1.0.0")
    parser.add_argument('output_dir', help = "output root directory", type = str)
    parser.add_argument('key', help = "key file download from TCGA", type = str)
    parser.add_argument('manifest', help = "manifest file downloaded from TCGA", type = str)
    parser.add_argument("--config_file", help = "config file", type = str, default = "")
    parser.add_argument("--ignore_list", help = "ignore analysis_id list file", type = str, default = "")
    args = parser.parse_args()
    
    output_dir = os.path.abspath(args.output_dir)
    key = os.path.abspath(args.key)
    manifest = os.path.abspath(args.manifest)
    
    if len(args.config_file) > 0:
        config_file = os.path.abspath(args.config_file)
    else:
        config_file = os.path.splitext(os.path.abspath(sys.argv[0]))[0] + ".cfg"
    
    # read config file
    config = ConfigParser.RawConfigParser()
    config.read(config_file)
    
    # make dir
    if (os.path.exists(output_dir) == False):
        os.makedirs(output_dir)
    if (os.path.exists(output_dir + "/log") == False):
        os.makedirs(output_dir + "/log")
    if (os.path.exists(output_dir + "/data") == False):
        os.makedirs(output_dir + "/data")
    if (os.path.exists(output_dir + "/scripts") == False):
        os.makedirs(output_dir + "/scripts")
    if (os.path.exists(output_dir + "/manifests") == False):
        os.makedirs(output_dir + "/manifests")
        
    now = datetime.datetime.now()
    log_path = output_dir + "/log/" \
                + "gt_surveilance_{0:0>4d}{1:0>2d}{2:0>2d}_{3:0>2d}{4:0>2d}{5:0>2d}.log".format(
                now.year, now.month, now.day, now.hour, now.minute, now.second)
    write_log(log_path, "w", "Start main process.", True, True)

    # split manifest
    splitter_name = "xml_splitter{0:0>4d}{1:0>2d}{2:0>2d}_{3:0>2d}{4:0>2d}{5:0>2d}".format(
                now.year, now.month, now.day, now.hour, now.minute, now.second)
    manifests_org = xml_splitter(splitter_name, output_dir, manifest, config)
    
    ignore = ""
    if os.path.exists(args.ignore_list) == True:
        f = open(args.ignore_list)
        ignore = f.read()
        f.close()

    manifests = []
    for man in manifests_org:
        f = open(man)
        data = f.read()
        f.close()

        man_split = re.split('<analysis_id>|</analysis_id>', data)
        if len(man_split) < 3:
            continue

        analysis = man_split[1]

        if ignore.find(analysis) >= 0:
            write_log(log_path, "a", "Skip this manifest because of ignore list. %s, %s" % (man, analysis), True, True)
        elif isbam_downloaded(man, output_dir + "/data") == True:
            write_log(log_path, "a", "Skip this manifest because of already downloaded. %s, %s" % (man, analysis), True, True)
        else:
            manifests.append(man)

    manifests.sort()
    
    write_log(log_path, "a", "Files = %d." % (len(manifests)), True, True)

    process_list = []
    if len(manifests) < 1:
        print ("no manifests.")
        
    elif len(manifests) == 1:
        now = datetime.datetime.now()
        man_name = os.path.splitext(os.path.basename(manifest))[0]
        name = "{name}_{man_name}_{year:0>4d}{month:0>2d}{day:0>2d}_{hour:0>2d}{minute:0>2d}{second:0>2d}".format(
                name = "gt_surveilance", man_name = man_name,
                year = now.year, month = now.month, day = now.day, 
                hour = now.hour, minute = now.minute, second = now.second)
        qsub_process(name, output_dir, key, manifests[0], config)
        
    else:
        max_once_jobs = config.getint('JOB_CONTROL', 'max_once_jobs')
        max_all_jobs = max_once_jobs * 2
        interval = config.getint('JOB_CONTROL', 'interval')
        
        j = 0
        while j < len(manifests):
            alives = 0
            for process in process_list:
                if process.exitcode == None:
                   alives += 1
        
            jobs = max_all_jobs - alives
            if jobs > max_once_jobs:
                jobs = max_once_jobs
            
            for i in range(jobs):
                if j >= len(manifests):
                    break
                    
                manifest = manifests[j]
                now = datetime.datetime.now()
                man_name = os.path.splitext(os.path.basename(manifest))[0]
                name = "{name}_{man_name}_{year:0>4d}{month:0>2d}{day:0>2d}_{hour:0>2d}{minute:0>2d}{second:0>2d}".format(
                        name = "gt_surveilance", man_name = man_name,
                        year = now.year, month = now.month, day = now.day, 
                        hour = now.hour, minute = now.minute, second = now.second)
                process = Process(target=qsub_process, name=name, args=(name, output_dir, key, manifest, config))
                process.daemon == True
                process.start()
                
                write_log(log_path, "a", name + ": Start sub process.", True, False)

                process_list.append(process)
                j += 1
            
            time.sleep(interval)

    
    for process in process_list:
        process.join()

        plog_file = output_dir + '/log/' + process.name + ".log"
        if os.path.exists(plog_file) == False:
            continue
        
        f_plog = open(plog_file)
        plog = f_plog.read()
        f_plog.close()
        os.remove(output_dir + '/log/' + process.name + ".log")

        write_log(log_path, "a", plog, False, False)

    failure = 0
    for manifest in manifests:
        if isbam_downloaded(manifest, output_dir + "/data") == False:
            write_log(log_path, "a", "Failed file to download = %s" % manifest, True, True)
            failure += 1
            
    write_log(log_path, "a", "End main process. Failed file to download = %d" % failure, True, True)

if __name__ == "__main__":
    main()
