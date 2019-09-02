import re
import csv
import sys

class SqlEngine():
    def __init__(self):
        self.tables = []
        self.tabs = {}
        self.readMetadata()
        self.readTables()


    #for processing data from metadata to slef.tables
    def readMetadata(self):
        file = open('./metadata.txt')
        try:
            data = file.readlines()
            for i in range(len(data)):
                if data[i].strip() == '<begin_table>':
                    tb = Tab()
                    i += 1
                    tb.tableName = data[i].strip()
                    i += 1
                    rowName = data[i].strip()
                    while rowName != '<end_table>':
                        attr = data[i].strip()
                        tb.attributes.append(attr)
                        i += 1
                        rowName = data[i].strip()
                    self.tables.append(tb)
                    self.tabs[tb.tableName] = tb
                i -= 1                
        finally:
            file.close()


    #reading self.tables
    def readTables(self):
        for t in self.tables:
            file = open(t.tableName + '.csv')
            try:
                for line in file:
                    line = line.split(',')
                    ind = 0
                    # print(line[0])
                    for cols in t.attributes:
                        t.columns[cols] = []

                    for cols in t.attributes:
                        # print(cols)
                        line[ind] = line[ind].strip()
                        # print(line[ind])
                        line[ind] =int(line[ind])
                        # print(line[ind])
                        # t.columns[cols].append(0)
                        t.columns[cols].append(line[ind])
                        ind += 1
                    t.rows.append(line)
                    # print(t.columns['D'])
         
            finally:
                file.close()        

    def proCols(self,query):
        pass
    def procRows(self,query):
        pass
    def procAgg(self,query):
        pass
    def procDist(self,query):
        pass
    def procCond(self,query,idx):
        pass



class Query():
    def __init__(self):
        self.tables = []
        self.cols = []
        self.line = "select * from A"
        self.flag = 1
    def parse(self):
        # line = line.strip()
        # print('bt')
        patt = ['select', 'from']
        for p in patt:
            if not re.search(p,self.line):
                print('Error in syntax')


class Tab():
    def __init__(self):
        self.tableName = ' '
        self.attributes = []
        self.rows =  []
        self.columns = {}









if __name__ == '__main__':
    # minisql = SqlEngine()
    # minisql.engine()
    hummahumma = Query()
    hummahumma.parse()
