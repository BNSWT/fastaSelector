from Bio import SeqIO
with open("/zhouyuyang/ESM2-data/data/artificial-standard.fasta", "w") as outfile:
    with open("/zhouyuyang/ESM2-data/data/artificial.fasta") as fp:
        for (tag, seq) in SeqIO.FastaIO.SimpleFastaParser(fp):
            outfile.write(">"+tag+"\n")
            outfile.write(seq+"\n\n")
        fp.close()
    outfile.close()