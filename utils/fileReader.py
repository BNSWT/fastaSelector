import os
import pandas as pd


# efficiently read large file by using a pointer
class FileReader:
    @classmethod
    # build a pointer for file
    def build_pointer(cls, file_path, save_path):
        with open(save_path, 'w') as w:
            w.write("location\n")
            with open(file_path, 'r') as r:
                loc = r.tell()
                line = r.readline()

                # record the index of the beginning of every line in byte stream
                cnt = 0
                while line:
                    w.write(f'{loc}\n')
                    loc = r.tell()
                    line = r.readline()

                    cnt += 1
                    # print(f"\ralready loaded {cnt} rows", end='')

    def __init__(self, file_path):
        name, suffix = os.path.splitext(file_path)

        # if the pointer doesn't exist, build it
        pointer_path = name + "_pointer.tsv"
        if not os.path.exists(pointer_path):
            FileReader.build_pointer(file_path, pointer_path)

        self.file = open(file_path, 'r')
        self.pointer = pd.read_csv(pointer_path, sep='\t')

    # return specific line given index(i.e. the (index-1)th row)
    def get(self, index):
        loc = self.pointer.iloc[index][0]
        self.file.seek(loc)

        return self.file.readline()

    def __len__(self):
        return self.pointer.shape[0]