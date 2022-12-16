import argparse
import random
import time
import pandas as pd
from utils.nameExtractor import NameExtractor
from utils.fileReader import FileReader


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--train_dir", type=str, default="/zhouyuyang/ESM2-data/data/afdb/seq/train-0.fasta",
        help="Path to new fasta"
    )
    parser.add_argument(
        "--train_tsv", type=str, default="/zhouyuyang/ESM2-data/data/afdb/seq/train.tsv",
        help="Path to new fasta"
    )
    parser.add_argument(
        "--validation_dir", type=str, default="/zhouyuyang/ESM2-data/data/afdb/seq/validation-0.fasta",
        help="Path to new fasta"
    )
    parser.add_argument(
        "--validation_tsv", type=str, default="/zhouyuyang/ESM2-data/data/afdb/seq/validation.tsv",
        help="Path to new fasta"
    )
    parser.add_argument(
        "--align_dir", type=str, default="/zhouyuyang/ESM2-data/data/afdb/seq/align-0.tsv",
        help="Path to new fasta"
    )
    parser.add_argument(
        "--pool_size", type=int, default="63",
        help="Number of threads"
    )
    args = parser.parse_args()
    
    random.seed(4)


    align = pd.read_csv(args.align_dir, header=None)
    align.drop_duplicates()
    aligned = align.values[:, 0].tolist()
    trash = set(aligned)
    print(f"trash size: {len(trash)}")

    trainReader = FileReader(args.train_dir)
    print(f"Finished readeing {args.train_dir}", flush=True)

    train_len = trainReader.pointer.shape[0]//2

    selector = NameExtractor(range(train_len), args.train_tsv, args.train_dir, args.pool_size, exclude_indexes=trash)
    selector.run()

    validationReader = FileReader(args.validation_dir)
    print(f"Finished readeing {args.validation_dir}", flush=True)

    validation_len = validationReader.pointer.shape[0]//2

    selector = NameExtractor(range(validation_len), args.validation_tsv, args.validation_dir, args.pool_size, exclude_indexes=trash)
    selector.run()
