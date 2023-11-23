# adapted from https://superfastpython.com/multithreaded-unzip-files/#How_to_Unzip_Files_Concurrently
# unzip a large number of files concurrently
import os
import argparse
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str, help='ZIP file to be extracted')
    parser.add_argument('-d', '--dir', type=str, default='./', help='Extract files into exdir. Default: "./"')
    parser.add_argument('-n', '--num_threads', type=int, default=100, help='Number of threads. Default: 100')
    parser.add_argument('-p', '--password', type=str, default=None, help='Password for encrypted file. Default: None')
    return parser

def main(args):
    os.makedirs(args.dir, exist_ok=True)
    # open the zip file
    with ZipFile(args.file, 'r') as zip_file:
        # set password
        if args.password is not None:
            zip_file.setpassword(args.password)
        
        # make all sub-dirs
        dir_names = zip_file.namelist()
        dir_names = list(filter(lambda x: x.endswith('/'), dir_names))
        for d in dir_names:
            os.makedirs(os.path.join(args.dir, d), exist_ok=True)
        
        # start the thread pool
        with ThreadPoolExecutor(args.num_threads) as exe:
            # unzip each file from the archive
            name_list = zip_file.namelist()
            future_list = [exe.submit(zip_file.extract, m, args.dir) for m in name_list]
            
            # print errors
            for future in tqdm(as_completed(future_list), total=len(name_list)):
                e = future.exception()
                if e is not None:
                    print(f'ERROR: {e}')

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args)
