import argparse
import random
import time
# import pandas as pd
from utils.fastaParser import FastaParser
from utils.fileReader import FileReader

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--old_dir", type=str, default="/zhouyuyang/ESM2-data/data/afdb/DB_clu_90_rep_p70.fasta",
        help="Path to old fasta"
    )
    parser.add_argument(
        "--selected_dir", type=str, default="/zhouyuyang/ESM2-data/data/afdb/seq/train-0.fasta",
        help="Path to new fasta"
    )
    parser.add_argument(
        "--removed_dir", type=str, default="/zhouyuyang/ESM2-data/data/afdb/seq/validation-0.fasta",
        help="Path to new fasta"
    )
    parser.add_argument(
        "--pool_size", type=int, default="63",
        help="Number of threads"
    )
    args = parser.parse_args()
    
    random.seed(4)

    old = FileReader(args.old_dir)
    print(f"Finished readeing {args.old_dir}", flush=True)

    old_len = old.pointer.shape[0]//2

    trash = set(random.sample(range(old_len), old_len//200))
    print(list(trash)[:20])

    selector = FastaParser(range(old_len), args.removed_dir, args.old_dir, args.pool_size, exclude_indexes=trash)
    selector.run()
