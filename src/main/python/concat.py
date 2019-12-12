import pandas as pd


train = pd.read_table('/home/luoyl/IdeaProjects/StringKernel/data/essay_data/train.tsv', index_col=0)
dev = pd.read_table('/home/luoyl/IdeaProjects/StringKernel/data/essay_data/dev.tsv', index_col=0)
test = pd.read_table('/home/luoyl/IdeaProjects/StringKernel/data/essay_data/test.tsv', index_col=0)


for i in range(1, 9):
    train_set = train[train['essay_set'] == i]['essay']
    dev_set = dev[dev['essay_set'] == i]['essay']
    test_set = test[test['essay_set'] == i]['essay']

    concated = pd.concat([train_set, dev_set, test_set])

    concated.to_csv("/home/luoyl/IdeaProjects/StringKernel/data/concat/set-%d.tsv" % i,
                    header=False, index=True, sep='\t')