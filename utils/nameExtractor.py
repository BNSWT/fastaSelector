import pandas as pd

from utils.Multiprocess import MultipleProcessRunner
from math import ceil
from utils.fileReader import FileReader

class NameExtractor(MultipleProcessRunner):
    def __init__(self, data, outpath, inpath, n_process=1, exclude_indexes=None, exclude_names=None, exclude_out_dir=None):
        super(NameExtractor, self).__init__(data, outpath, n_process)
        self.exclude_indexes = exclude_indexes
        self.exclude_names = exclude_names
        self.exclude_out_dir = exclude_out_dir
        self.length = len(data)
        self.inpath = inpath
        print(f"self.inpath:{self.inpath}")
        print(f"Data length: {self.length}", flush=True)
    def _target(self, process_id, subdata, sub_path, *args):
        content = ""
        fileReader = FileReader(self.inpath)
        written = 0
        for index in subdata:
            if self.exclude_indexes is not None and index in self.exclude_indexes:
                pass
            elif self.exclude_names is not None and self.get_name(index, fileReader) in self.exclude_names:
                pass
            else:
                written += 1
                content += self.get_id(index, fileReader)+"\n"
                
            if index % 1000 == 0 or index == subdata[-1] or index == 100:
                with open(sub_path, "a") as fp:
                    fp.write(content)
                    content = ""
                    print(f"SubProcess {process_id}: Successfully written the {index}th sequences at position {fp.tell()}", flush=True)
                    fp.close()
                # pass
            # if index == 20:
            #     print(f"index:{index}")
            #     print(f"written:{written}")
            #     break
    def _aggregate(self, final_path: str, sub_paths):
        print(f"entered aggregate method\nfinal_path: {final_path}\nsubpath: {sub_paths}")
        chunk_size = 1048576
        with open(final_path, "wb") as outfile:
            for sub_path in sub_paths:
                with open(sub_path, "rb") as infile:
                    while True:
                        content = infile.read(chunk_size)   
                        if not content:
                            break
                        print(f"{sub_path} is read to {infile.tell()} ")
                        outfile.write(content)
                        print(f"{final_path} is written to {outfile.tell()}")
                    infile.close()
                    print(f"Successfully written {sub_path} to {final_path}")
            outfile.close()
        return	
    def __len__(self):
        return self.length
    def get_name(self, index, fr):
        # print(fr.get(index*2)[1:-2].split(' ')[0])
        return fr.get(index*2)[1:-1].split(' ')[0]
    def get_longname(self, index, fr):
        return fr.get(index*2)[1:-1]
    def get_seq(self, index, fr):
        return fr.get(index*2+1)[1:-1]
    def get_name_list(self, fr):
        name_list = []
        for index in range(self.length):
            name_list.append(self.get_name(index, fr))
        return name_list
    def get_id(self, index, fr):
        return fr.get(index*2)[1:-1].split(' ')[0][len("AFDB:AF-"):-3]
