import argparse
from ast import arg
import glob
import logging
import os
from concurrent import futures
from logging import getLogger,FileHandler,Formatter

LOG = getLogger('__main__')
FHD = FileHandler('./result.lst', mode='w')
FMT = Formatter('%(message)s')
LOG.setLevel(logging.INFO)
FHD.setLevel(logging.INFO)
FHD.setFormatter(FMT)
LOG.addHandler(FHD)

PARSER = argparse.ArgumentParser(description='Find an empty file with parallel processing.')
PARSER.add_argument(
    '--max_workers',
    type=int,
    default=2,
    help='Specifies the number of max_workers. (default: 2)'
)
PARSER.add_argument(
    '--non_empty',
    action='store_true',
    help='Search for non-empty files. (default: empty files.)'
)

ARGS = PARSER.parse_args()

def get_target_d(root_point):
    result = glob.glob(f'./dirs/{root_point}/*/*')
    return result

def check_file_f(target_dirs, i):
    for target_dir in target_dirs[i]:
        tmp = target_dir + '/test_file'
        if ARGS.non_empty:
            if os.path.isfile(tmp) and os.path.getsize(tmp) != 0: LOG.info(tmp)
        else:
            if os.path.isfile(tmp) and os.path.getsize(tmp) == 0: LOG.info(tmp)

def main():
    root_point_dirs = glob.glob('./dirs/[0-F]')
    future_results = []
    with futures.ThreadPoolExecutor(max_workers=8) as executor:
        for root_point_dir in root_point_dirs:
            root_point = root_point_dir.split('/')[-1]
            future = executor.submit(get_target_d, f'{root_point}')
            future_results.append(future)

    tmp_dirs = [future_result.result() for future_result in future_results if future_result.result()]

    with futures.ThreadPoolExecutor(max_workers=ARGS.max_workers) as executor:
        for i in range(len(tmp_dirs)):
            future = executor.submit(check_file_f, tmp_dirs, i)

if __name__ == '__main__':
    main()