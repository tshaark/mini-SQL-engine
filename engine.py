import re
import csv
import sys
from prettytable import PrettyTable
from itertools import product

class SqlEngine():
    def __init__(self):
        self.tables = []
        self.tabs = {}
        self.outable=[]
        self.outcols = []
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
    def checkOpr(self,l,r,op):
        pass
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
    def retrieveTables(self, table):
        return self.tabs[table].rows        
    def minisqlengine(self):
        while 1:
            q = input('miniSQLEngine>>')
            if q == 'quit':
                break
            quer = q.split(';')
            for i in range(len(quer)):
                quer[i] = quer[i] + ';'
                query = Query(quer[i])
                flg = query.parse()
                if flg == 0:
                    continue
                flag = 0    
                for j in query.tables:
                    if j not in self.tabs:
                        flag = 1
                        print('Table not found')
                if flag == 1:
                    continue
                for j in product(*map(self.retrieveTables,query.tables)):
                    self.outable.append(j)
                # for t in self.outable:
                #     print(t)    
                for t in query.tables:
                    self.outcols.append(self.tabs[t].attributes)  
                # for t in self.outcols:
                #     print(t)


class Query():
    def __init__(self,line):
        self.tables = []
        self.cols = []
        self.conds=[]
        self.line = line
        self.flag = 1
        self.conditions = []
    def parse(self):
        # line = line.strip() isko baad me uncomment karna hai
        # print('bt')
        patt = ['select', 'from']
        for p in patt:
            if not re.search(p,self.line,re.IGNORECASE):
                print('Error in syntax')
                return 0

        
        cond = re.search(r'(\where)[\ ]*$',self.line,re.IGNORECASE)
        if cond:
            print('Where condition is not provided')
        
        if re.search(r'(\ where\ )',self.line, re.IGNORECASE):
            tables = re.sub(r'^(select\ ).+(\ from\ )(.+)(\ where\ )(.+)[;]$', r'\3' , self.line,flags = re.IGNORECASE)
            tables = tables.split(',')
            for t in tables:
                self.tables.append(t.strip())

            # conds = re.sub(r'^(select\ ).+(\ from\ )(.+)(\ where\ )(.+)[;]$', r'\4' , self.line,flags = re.IGNORECASE)
            conds = re.sub(r'^(select\ ).+(\ from\ ).+(\ where\ )(.+)[;]$', r'\4' , self.line, flags = re.IGNORECASE)
            conds = conds.strip()
            # conds = conds.strip()
            # print(conds)
        
            

            if re.search(r'(\ or\ )',conds,re.IGNORECASE):
                conds = re.sub(r'^(.+)(or)(.+)$', r'\1 or \3',conds,flags = re.IGNORECASE)
                
                # for con in conds:
                #     print(con)
                conds = conds.split('or')
                for i in conds:
                    i = i.strip()
                    # print(i)
                    self.conditions.append(i)
                # for c in self.conditions:
                #     print(c)    
                return 3
            
            
            elif re.search(r'(\ and\ )',conds,re.IGNORECASE):
                conds = re.sub(r'^(.+)(and)(.+)$', r'\1 and \3',conds,flags = re.IGNORECASE)
                conds = conds.split('and')
                # print('baka')
                for i in conds:
                    i = i.strip()
                    self.conditions.append(i)
                # for c in self.conditions:
                #     print(c)                  
                return 4 
            
            else:
                conds = conds.strip()
                self.conditions.append(conds)
                return 2
        else:
            tables = re.sub(r'^(select\ ).+(\ from\ )(.+)[;]$', r'\3' , self.line,flags = re.IGNORECASE)        
            tables = tables.split(',')
            for t in tables:
                self.tables.append(t.strip())
            return 1

               


class Tab():
    def __init__(self):
        self.tableName = ' '
        self.attributes = []
        self.rows =  []
        self.columns = {}









if __name__ == '__main__':
    minisql = SqlEngine()
    minisql.minisqlengine()
    # hummahumma = Query()
    # hummahumma.parse()
