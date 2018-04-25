import os
import pandas as pd
from csv import DictWriter

# 11420039 lines
def transfer_data2csv():
    userFeature_data = []
    with open('../data/userFeature.data', 'r') as f:
        for i, line in enumerate(f):
            line = line.strip().split('|')
            userFeature_dict = {}
            for each in line:
                each_list = each.split(' ')
                userFeature_dict[each_list[0]] = ' '.join(each_list[1:])
            userFeature_data.append(userFeature_dict)
        if i % 100000 == 0:
            print(i)
            user_feature = pd.DataFrame(userFeature_data)
            user_feature.to_csv('../data/userFeature.csv', index=False)

def readlines():
    file = open('../data/userFeature.data')
    lines = len(file.readlines()) 
    print lines

def clip_data():
    os.chdir('../data')
    ix = 0

    fo =  open('userFeature%s.csv' %ix, 'w')
    headers = ['uid', 'age', 'gender', 'marriageStatus', 'education', 'consumptionAbility', 'LBS', 'interest1', 'interest2',
        'interest3', 'interest4', 'interest5', 'kw1', 'kw2', 'kw3',  'topic1', 'topic2', 'topic3', 'appIdInstall',
        'appIdAction', 'ct', 'os', 'carrier', 'house']
    writer = DictWriter(fo, fieldnames=headers, lineterminator='\n')
    writer.writeheader()

    fi = open('userFeature.data', 'r')
    for line in fi :
        line = line.replace('\n', '').split('|')
        userFeature_dict = {}
        for each in line:           
            each_list = each.split(' ')
            userFeature_dict[each_list[0]] = ' '.join(each_list[1:])
        ix = ix+1
        writer.writerow(userFeature_dict)
        if ix % 200000==0:
            print(ix)
            #fo.close()
            fo = open('userFeature%s.csv' %ix, 'w')
            writer = DictWriter(fo, fieldnames=headers, lineterminator='\n')
            writer.writeheader()
    fo.close()
    fi.close()

def merge_data(target_file, current_file):
	data1 = pd.read_csv(target_file)
	data2 = pd.read_csv(current_file)
	merge = data1.append(data2)
	num = pd.DataFrame(merge)
	num.to_csv(target_file, index=False)

def main():
    clip_data()
	target_file = os.path.join(BASE_DIR, 'userFeature0.csv')
	for i in range(1,58):
		current_file = os.path.join(BASE_DIR, 'userFeature'+str(i*200000)+'.csv')
		print 'now we loading current_file...\n', current_file
		merge_data(target_file, current_file)
		print 'we merged target file with '+current_file

if __name__ == '__main__':
    main()