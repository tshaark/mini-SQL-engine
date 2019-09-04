import re
import csv
import sys
from prettytable import PrettyTable
from itertools import product

class SqlEngine():
    def __init__(self):
        self.tables = []
        self.tabs = {}
        self.tn = {}
        # self.outable=[]
        # self.outcols = []
        self.readMetadata()
        self.readTables()


    #for processing data from metadata to slef.tables
    def readMetadata(self):
        file = open('./metadata.txt')
        try:
            # print('baka')
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
                        self.tabs[t.tableName].columns[cols].append(line[ind])
                        ind += 1
                    t.rows.append(line)
                    self.tabs[t.tableName].rows.append(line)
                    # t.num += 1
                    self.tabs[t.tableName].num += 1
                    # print(t.columns['D'])
         
            finally:
                file.close()        
    
    def proc(self,flag,query):
        if flag == 1:
            num = 1
            for i in query.tables:
                num *= self.tabs[i].num
            idx = range(num)
        elif flag == 2:
            val = self.procCond(0, query)
            if val > -1:
                idx = val
            else:
                return    
        else:
            val1 = self.procCond(0, query)
            val2 = self.procCond(1, query)
            if val1 == -1:
                return
            elif val2 == -2:
                return
            if flag == 3:
                rnge = (set(val1) | set(val2))
                idx = list(rnge)
            elif flag == 4:
                rnge = (set(val1) & set(val2))
                idx = list(rnge)    
        self.id = idx                               
        if any(re.match(r'(distinct)', string, re.IGNORECASE) for string in query.cols):
            self.procDist(query)
            return
        elif any(re.match(r'.+\(.+\)', string) for string in query.cols):
            self.procAgg(query)
            return
        else:
            # print('baka')
            self.procRows(query)    

    def resolveOpr(self,l,r,opr):
        if opr == '>':
            return l > r
        elif opr == '<':
            return l < r
        elif opr == '=':
            return l == r
        elif opr == '>=':
            return l >= r
        elif opr == '<=':
            return l <= r
    
    def procRows(self,query):
        prcsdCols = self.proCols(query)
        if prcsdCols == -1:
            return

        ptable = PrettyTable(prcsdCols)
        for i in self.id:
            row = []
            for cols in prcsdCols:
                col = re.sub(r'(.+)\.(.+)', r'\2', cols)
                tab = re.sub(r'(.+)\.(.+)', r'\1', cols)
                # print(tab)
                if tab not in self.tabs:
                    print('Table ' + tab + ' does not exist')
                    return
                if col not in self.tabs[tab].attributes:
                    print('Column ' + col + ' does not exist')
                    return
                
                cnt = 0
                for j in self.tabs[tab].attributes:
                    if j != col:
                        cnt += 1
                    else:
                        break    
                row.append(self.outable[i][self.tn[tab]][cnt])
            ptable.add_row(row)
        print(ptable)            



    
    def proCols(self,query):
        prcsdCols = []
        # for p in query.cols:
        #     print(p)
        if '*' not in query.cols:
            for cols in query.cols:
                if not re.match(r'.+\..+', cols):
                    count = 0
                    for t in query.tables:
                        if cols not in self.tabs[t].columns:
                            continue
                        tab = t
                        prcsdCols.append(tab + '.' + cols)
                        count += 1    
                    if count == 0:
                        print('Column ' + cols + ' does not exist' )
                        return -1        
                else:
                    prcsdCols.append(cols)
        else:
            query.cols = []
            for t in query.tables:
                prcsdCols  += ([ t + '.' + x for x in self.tabs[t].attributes])
        
        # print('baka')
        # for p in prcsdCols:
        #     print(p)
        return prcsdCols

    def procAgg(self,query):
        pass
    def procDist(self,query):
        pass
    
    
    def procCond(self, idx, query):
        if not re.match(r'([^<>=]+)(<|=|<=|>=|>)([^<>=]+)', query.conds[idx]):
            print('Invalid Operators')
            return -1 

        lhs = re.sub(r'(.+)(<|=|<=|>=|>)(.+)', r'\1', query.conds[idx])
        lhs = lhs.strip()    
        opr = re.sub(r'(.+)(<|=|<=|>=|>)(.+)', r'\2', query.conds[idx])
        opr = opr.strip()    
        rhs = re.sub(r'(.+)(<|=|<=|>=|>)(.+)', r'\3', query.conds[idx])
        rhs = rhs.strip()

        val = 0
        tabs = ''
        if type(rhs) is int:
            rhs = int(rhs)
            val = 1

        if not re.match(r'(.+)\.(.+)', lhs):
            condL = lhs
            count = 0
            for t in query.tables:
                if lhs in self.tabs[t].columns:
                    count += 1
                    tabs = t
            if count == 0:
                print('Column ' + condL + ' does not exist')
                return -1
        else:
            tabs = re.sub(r'(.+)\.(.+)', r'\1', lhs)
            condL = re.sub(r'(.+)\.(.+)', r'\2', lhs)
        
        index = []
        
        if val == 0:
            leftTab = tabs
            rightTab = ''
            leftCo = condL
            if not re.match(r'(.+)\.(.+)', rhs):
                count = 0
                rightCo = rhs
                for t in query.tables:
                    if rightCo not in self.tabs[t].columns:
                        continue
                    else:
                        rightTab = t
                        count += 1
                if count == 0:
                    print('column ' + rightTab + ' does not exist' )
                    return -1        
            else:
                rightTab = re.sub(r'(.+)\.(.+)', r'\1', rhs)
                rightCo = re.sub(r'(.+)\.(.+)', r'\2', rhs)    
            
            if rightTab not in self.tabs:
                print('Table ' + rightTab + ' does not exist')
                return -1
            if rightCo not in self.tabs[rightTab].attributes:
                print('Column ' + rightCo + ' does not exist')
                return -1
            countL = 0
            num = 1
            for i in query.tables:
                num *= self.tabs[i].num
            for i in self.tabs[leftTab].attributes:
                if i != leftCo:
                    countL += 1
                else:
                    break
            countR = 0
            for i in self.tabs[rightTab].attributes:
                if i != rightCo:
                    countR += 1
                else:
                    break        
            for i in range(num):
                if self.resolveOpr(self.outable[i][self.tn[leftTab]][countL],self.outable[i][self.tn[rightTab]][countR],opr):
                    index.append(i)




        else:    
            cnt = 0
            num = 1
            for t in query.tables:
                num *= self.tabs[t].num
            for t in self.tabs[tabs].attr:
                if t != condL:
                    cnt += 1
                else:
                    break
            for i in range(num):
                if self.resolveOpr(self.outable[i][self.tn[tabs]][cnt],rhs,opr):
                    index.append(i)


        return index




    
    
    
    
    
    def retTables(self, table):
        return self.tabs[table].rows        
    
    
    def minisqlengine(self):
        while 1:
            q = input('miniSQLEngine>>')
            if q == 'quit':
                break
            quer = q.split(';')
            for i in range(len(quer)):
                self.outable = []
                self.outcols = []
                quer[i] = quer[i] + ';'
                query = Query(quer[i])
                flg = query.parse()
                if flg == 0:
                    continue
                flag = 0    
                count = 0
                for j in query.tables:
                    if j not in self.tabs:
                        flag = 1
                        print('Table not found')
                    self.tn[j] = count
                    count += 1    
                if flag == 1:
                    continue
                # print(flag)
                for j in product(*map(self.retTables,query.tables)):
                    self.outable.append(j)
                del self.outable[::2]    
                # for t in self.outable:
                #     print(t)    
                for t in query.tables:
                    self.outcols.append(self.tabs[t].attributes)  
                # for t in self.outcols:
                #     print(t)
                self.id = []
                self.proc(flg, query)



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
        # print('baka1')
        line = self.line.split(' ')
        for p in patt:
            # print(p,self.line)
            if p not in line:
                print('Error in syntax')
                return 0
        
        cols = re.sub(r'^(select\ )(.+)(\ from\ ).+[;]$', r'\2' , self.line, re.IGNORECASE).split(',')
        for col in cols:
            self.cols.append(col.strip())
        
        cond = re.search(r'(\where)[\ ]*$',self.line,re.IGNORECASE)
        # if not cond:
        #     print('Where condition is not provided')
        
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
            # print('baka')
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
        self.num = 0









if __name__ == '__main__':
    minisql = SqlEngine()
    minisql.minisqlengine()
    # hummahumma = Query()
    # hummahumma.parse()
