from pcap_splitter.splitter import PcapSplitter
import time
from multiprocessing import Pool, Process, current_process
import functools
import subprocess
from subprocess import call
import shlex
import os

count = 0


def _nprint_files(input_file, output_file, flag):
    if flag == 0:
        cmd1 = (
            "nprint -P "
            + input_file
            + " --absolute_timestamps  --tcp --udp --ipv4 --icmp -W "
            + output_file
        )
        subprocess.call(cmd1, shell=True)
        # cmd2 = 'mv ' + input_file + ' ' + path
        # subprocess.run(cmd2, shell = True)
    elif flag == 1:
        # Please note that you are including the payload and you need a modified processing script to convert this npt into field wise or byte wise representation. The existing script doesn't support this functionality.
        cmd1 = (
            "nprint -P "
            + input_file
            + " --absolute_timestamps  --tcp --udp --ipv4 --icmp -p 20 -W "
            + output_file
        )
        subprocess.call(cmd1, shell=True)
        # cmd2 = 'mv ' + input_file + ' ' + path
        # subprocess.run(cmd2, shell = True)
    # elif


def nprint_conversion(input_dir, output_dir):
    arg_list = []
    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".pcap"):

                filename = output_dir + "/" + file + ".npt"
                lst = (subdir + "/" + file, filename, 1)
                arg_list.append(lst)


    _paralell_process(_nprint_files, arg_list)


def _time_series(input_file, output_file):
    try:
        with open(output_file, "w") as outfile:
            subprocess.run(
                [
                    "tshark",
                    "-r",
                    input_file,
                    "-T",
                    "fields",
                    "-E",
                    "separator=/t",
                    "-e",
                    "frame.time_relative",
                    "-e",
                    "tcp.flags",
                    "-e",
                    "frame.cap_len",
                    "-e",
                    "ip.src",
                    "-e",
                    "tcp.srcport",
                    "-e",
                    "udp.srcport",
                    "-e",
                    "ip.dst",
                    "-e",
                    "tcp.dstport",
                    "-e",
                    "udp.dstport",
                    "-e",
                    "ip.proto",
                    "-e",
                    "tls.handshake.extensions_server_name",
                ],
                stdout=outfile,
                check=True,
            )
    except:
        print("Process stopped working due to: " + input_file)


def time_series_conversion(input_dir, output_dir):
    arg_list = []
    for label in range(2):
        curr_dir = input_dir + str(label)
        for file in os.listdir(curr_dir):
            if file.endswith(".pcap"):
                filename = output_dir + str(label) + "/" + file
                lst = (curr_dir + "/" + file, filename + ".csv")
                arg_list.append(lst)
    _paralell_process(_time_series, arg_list)


def pcap_split(file, output_dir):
    arg_list = []
    # for file in os.listdir(input_dir):
    if file.endswith(".pcap"):
        lst = (file, output_dir)
        arg_list.append(lst)

    _paralell_process(_pcap_split_impl, arg_list)


def _pcap_split_impl(input_file, output_folder):
    ps = PcapSplitter(input_file)
    start = time.time()
    print(ps.split_by_session(output_folder))
    end = time.time()
    print("Time taken is :", start - end)


def _patator_traffic_split(input_file, attack_folder, benign_folder):
    x = subprocess.run(
        [
            "tshark",
            "-r",
            input_file,
            "-T",
            "fields",
            "-E",
            "separator=/t",
            "-e",
            "ip.src",
            "-e",
            "ip.dst",
        ],
        check=True,
        capture_output=True,
    )
    if any(
        [
            k.strip()
            in [
                "169.231.8.224",
                "169.231.208.176",
                "169.231.9.149",
                "169.231.124.86",
                "169.231.54.0",
                "169.231.86.147",
                "169.231.111.207",
                "169.231.178.136",
                "169.231.29.156",
                "169.231.147.74",
                "169.231.216.129",
                "169.231.180.92",
            ]
            for k in flatten(
                [
                    j.split("\t")
                    for j in [i for i in x.stdout.decode().strip().split("\n")]
                ]
            )
        ]
    ):
        subprocess.run(["mv", input_file, attack_folder], check=True)
    else:
        subprocess.run(["mv", input_file, benign_folder], check=True)


def patator_traffic_split(input_folder, attack_folder, benign_folder):
    args = []
    for i in os.listdir(input_folder):
        if "satya" in i:
            args.append((input_folder + i, attack_folder, benign_folder))
    _paralell_process(_patator_traffic_split, args)


def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)


def flatten(l):
    return [item for sublist in l for item in sublist]


def nprintConversionPerUser(input_dir, output_dir):
    arg_list = []

    # Creating directory for each user in output_dir
    # for subdir, dirs, files in os.walk(input_dir):
    #     if dirs == []:
    #         break
    #     for dir in dirs:
    #         call(f'mkdir -p {output_dir}{dir}', shell=True)
    # print("finished")
    # return
    for subdir, dirs, files in os.walk(input_dir):

        for file in files:
            if file.endswith(".pcap"):
                user = subdir.split('/')[-1]
                filename = f'{output_dir}{user}/{file}.npt'
                # filename = output_dir + "/" + file + ".npt"
                lst = (subdir + "/" + file, filename, 1)
                arg_list.append(lst)



    _paralell_process(_nprint_files, arg_list)

if __name__ == "__main__":
    print("here")
    # _pcap_split_impl("/data/patator-multi-cloud-benign2.pcap", "/data/patator-multi-cloud-pcaps/1")
    # include 0 for third argument to not include payload. The default is no payload. Update this to 1 if you want to include payload.
    # time_series_conversion("/data/patator-multi-cloud-pcaps/", "/data/patator-multi-cloud-Curtains-timeseries/")
    # _time_series("/data/UCSB_labelled_Pcaps/1/2022-12-05-1415-merged-673912.pcap", "/data/tbd.csv")
    # patator_traffic_split("/data/patator-pcaps/", "/data/patator-pcaps/attack", "/data/patator-pcaps/benign")
    # nprint_conversion("/mnt/md0/jaber/nprintPcap/", "/mnt/md0/jaber/nprintFiles/")
    # pcap_split("/data/heartbleed-new.pcap", "/data/heartbleed")
    #print(count)
    #pass
    nprintConversionPerUser('/mnt/md0/jaber/groupedPcap/', '/mnt/md0/jaber/groupedNprint/')
