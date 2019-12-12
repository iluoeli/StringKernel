import numpy as np
import pandas as pd
import random
from ssk.string_kernel import string_kernel
import time
import sys

if __name__ == '__main__':
    i = int(sys.argv[1])
    print('generating %d' % i)
    # load data sets
    train = pd.read_csv('cat_data/train_%d.tsv' % i, sep='\t')
    dev = pd.read_csv('cat_data/dev_%d.tsv' % i, sep='\t')
    test = pd.read_csv('cat_data/test_%d.tsv' % i, sep='\t')

    train_list = train.essay.tolist()
    # list = random.sample(list, 10)
    xs = np.array(train_list).reshape((len(train_list), 1))
    as_feature = xs
    start = time.time()
    # print(string_kernel(xs, ys, 15, 1.))
    result = string_kernel(xs, as_feature, 15, 1.)
    with open("string_kernel_train_%d.txt" % i, "w") as f:
        for row in result:
            for col in row:
                f.write(str(col) + "\t")
            f.write("\n")
    cur = time.time()
    print(cur - start)

    test_list = test.essay.tolist()
    xs = np.array(test_list).reshape((len(test_list), 1))
    result = string_kernel(xs, as_feature, 15, 1.)
    with open("string_kernel_test_%d.txt" % i, "w") as f:
        for row in result:
            for col in row:
                f.write(str(col) + "\t")
            f.write("\n")
    cur = time.time()
    print(cur - start)

    dev_list = dev.essay.tolist()
    xs = np.array(dev_list).reshape((len(dev_list), 1))
    result = string_kernel(xs, as_feature, 15, 1.)
    with open("string_kernel_dev_%d.txt" % i, "w") as f:
        for row in result:
            for col in row:
                f.write(str(col) + "\t")
            f.write("\n")
    cur = time.time()
    print(cur - start)
