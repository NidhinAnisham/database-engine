# -*- coding: utf-8 -*-
'''
 *
 *
 *  @author Nidhin Anisham
 *  This code is the implementation of DavisNanoBase.
 *  Requires texttable (pip install texttable)
 *
 *
'''

import os
import struct
from texttable import Texttable
from datetime import datetime

dataTypeSize = { "null": 0,
                 "tinyint": 1,
                 "smallint": 2 ,
                 "int": 4,
                 "bigint": 8,
                 "long": 8,
                 "float": 4,
                 "double": 8,
                 "year": 1,
                 "time": 4,
                 "datetime": 8,
                 "date": 8,
                 "text": 0 }

dataTypeHex = {  "null": 0,
                 "tinyint": 1,
                 "smallint": 2 ,
                 "int": 3,
                 "bigint": 4,
                 "long": 4,
                 "float": 5,
                 "double": 6,
                 "year": 8,
                 "time": 9,
                 "datetime": 10,
                 "date": 11,
                 "text": 12 }

hexDataType = {v: k for k, v in dataTypeHex.items()}


#prints the table in an sql-esque format
def prettyPrint(matrix):
    table = Texttable()   
    table.set_max_width(0)
    table.add_rows(matrix)
    print(table.draw() + "\n")


#gets hex value
def getHex(value,dataType):
    if dataType == 'null':
        return
    if dataType == 'tinyint' or dataType == 'year':
        return struct.pack('>b',int(value))
    if dataType == 'smallint':
        return struct.pack('>h',int(value))
    if dataType == 'int' or dataType == 'time':
        return struct.pack('>i',int(value))
    if dataType == 'bigint' or dataType == 'long' or dataType == 'datetime' or dataType == 'date':
        return struct.pack('>q',int(value))
    if dataType == 'float':
        return struct.pack('>f',float(value))
    if dataType == 'double':
        return struct.pack('>d',float(value))
    if dataType == 'text':
        return value.encode()


#gets value from hex
def getData(value,dataType):            
    if dataType == 'null':
        return 'NULL'
    if dataType == 'tinyint':
    	return struct.unpack('>b',value)[0]
    if dataType == 'smallint':
    	return struct.unpack('>h',value)[0]
    if dataType == 'int':
    	return struct.unpack('>i',value)[0]
    if dataType == 'bigint' or dataType == 'long':
    	return struct.unpack('>q',value)[0]
    if dataType == 'float':
    	return struct.unpack('>f',value)[0]
    if dataType == 'double':
    	return struct.unpack('>d',value)[0]
    if dataType == 'year':
        return struct.unpack('>b',value)[0] + 2000
    if dataType == 'time':
        ms = struct.unpack('>i',value)[0]
        return datetime.fromtimestamp(ms//1000).time()
    if dataType == 'date':
        ms = struct.unpack('>q',value)[0]
        return datetime.fromtimestamp(ms//1000).date()
    if dataType == 'datetime':
        ms = struct.unpack('>q',value)[0]
        return datetime.fromtimestamp(ms//1000)
    if dataType == 'text':
    	return value.decode()
   
 
#utility to get row id and last insert location   
def getRecordData(file):
    file.seek(1)
    ri = bytearray(file.read(2))
    li = bytearray(file.read(2))
    return getData(ri,'smallint'),getData(li,'smallint')
   
 
def parseCreate(command):
    tokens = command.split(" ")
    table = tokens[2]+".tbl"
    columns = ' '.join(tokens[3:])
    if columns[0] != '(' or columns[-1] != ')':
        print("Invalid Syntax. Type \"help;\"\n")
        return
    
    columns = columns[1:-1].split(",")
    if not os.path.exists("data/"+table):
        with open("data/davisbase_columns.tbl","rb") as dc:
            rowId,lastInsert = getRecordData(dc)
            rowId += 1
        
        for c in range(len(columns)):
            values = columns[c].split(" ")
            if(len(values)<2 or len(values)>4):
                print("Invalid Syntax. Type \"help;\"\n")
                return
            
            if(len(values) == 2):
                insertValues("data/davisbase_columns.tbl",[rowId,tokens[2],values[0],values[1],c+1,"YES"],['int','text','text','text','tinyint','text','null'])
            elif(len(values) == 4 and values[2] == "not" and values[3] == "null"):
                insertValues("data/davisbase_columns.tbl",[rowId,tokens[2],values[0],values[1],c+1,"NO"],['int','text','text','text','tinyint','text','null'])
            elif(c==0 and len(values)==4 and values[2] == 'primary' and values[3] == 'key'):
                insertValues("data/davisbase_columns.tbl",[rowId,tokens[2],values[0],values[1],c+1,"NO","PRI"],['int','text','text','text','tinyint','text','text'])
            else:
                print("Invalid Syntax. Type \"help;\"\n")
                return
                
        with open("data/davisbase_tables.tbl","rb") as dt:
            rowId,lastInsert = getRecordData(dt)
            rowId += 1
        
        insertValues("data/davisbase_tables.tbl",[rowId,tokens[2],0],['int','text','tinyint'])
        
        with open("data/"+table,"wb") as tbl:
            tbl.write(getHex(13,'tinyint'))
            tbl.seek(3)
            tbl.write(getHex(pageSize,'smallint'))
            tbl.write(getHex(-1,'int'))
        
        print("Table created.\n")
        
    else:
        print("Table already exists.\n")


#gets header information of the table
def getTableData(columnName,tableName):
    
    column_id = []
    column_header = []
    data_types = []
    
    with open("data/davisbase_columns.tbl","rb") as dc:
        rowId,lastInsert = getRecordData(dc)
        for i in range(rowId):
            dc.seek(lastInsert)
            payLoadSize = getData(bytearray(dc.read(2)),'smallint')
            dc.seek(lastInsert+6)
            nCol = getData(bytearray(dc.read(1)),'tinyint')
            types = []
            sizes = []
            for j in range(nCol):
                size = getData(bytearray(dc.read(1)),'tinyint')
                if size>12:
                    types.append('text')
                    sizes.append(size-12)
                else:
                    hexValue = getHex(size,'smallint')
                    dt = hexDataType[getData(hexValue,"smallint")]
                    types.append(dt)
                    sizes.append(dataTypeSize[dt])
                    
            dc.seek(lastInsert+7+nCol+4)
            table_name = dc.read(sizes[1]).decode("utf-8")
            column_name = dc.read(sizes[2]).decode("utf-8")
            data_type = dc.read(sizes[3]).decode("utf-8")
            ord_no = getData(bytearray(dc.read(sizes[4])),'tinyint')
            
            if table_name == tableName:
                column_id.append(ord_no)
                column_header.append(column_name)
                data_types.append(data_type)
            lastInsert += payLoadSize+6
        
        return column_id[::-1],column_header[::-1],data_types[::-1]
 
    
def parseSelect(command):
    tokens = command.split(" ")
    if tokens[2] != "from" or len(tokens)<4:
        print("Invalid Syntax. Type \"help;\"\n")
        return
    
    if not os.path.exists("data/"+tokens[3]+".tbl"):
        print("Table does not exist.\n")
        return
    
    if len(tokens)>4 and tokens[4]!="where" and len(tokens)!=8:
        print("Invalid Syntax. Type \"help;\"\n")
        return
    
    columnId, columnNames, dataTypes = getTableData(tokens[1],tokens[3])
    
    columns = tokens[1].split(",")
    if '*' in columns:
        columns = columnNames
        
    whereId = 0
    if(len(tokens)>4 and tokens[5] in columnNames):
        whereId = columnId[columnNames.index(tokens[5])]
    records = []
    
    if(len(columnNames) == 0):
        print("No records\n")
        return
    
    with open("data/"+tokens[3]+".tbl","rb") as t:
        rowId,lastInsert = getRecordData(t)
        for i in range(rowId):
            t.seek(lastInsert)
            payLoadSize = getData(bytearray(t.read(2)),'smallint')
            t.read(4)
            nCol = getData(bytearray(t.read(1)),'tinyint')
            types = []
            sizes = []
            for j in range(nCol):
                size = getData(bytearray(t.read(1)),'tinyint')
                if size>12:
                    types.append('text')
                    sizes.append(size-12)
                else:
                    dt = hexDataType[size]
                    types.append(dt)
                    sizes.append(dataTypeSize[dt])
            
            t.seek(lastInsert+7+nCol)
            record = []
            for j in range(len(sizes)):
                record.append(getData(t.read(sizes[j]),types[j]))
            
            #to process where condition
            if whereId:
                value1 = str(record[whereId-1])
                value2 = tokens[7]
                if(types[whereId-1] in ["int","tinyint","smallint","long","bigint"]):
                    value1 = int(value1)
                    value2 = int(value2)
                elif(types[whereId-1] in ["float","double"]):
                    value1 = float(value1)
                    value2 = float(value2)
                    
                if tokens[6] == '=' and  value1 != value2:
                    record = []
                elif tokens[6] == '>' and value1 <= value2:
                    record = []
                elif tokens[6] == '<' and value1 >= value2:
                    record = []
                elif tokens[6] == '>=' and value1 < value2:
                    record = []
                elif tokens[6] == '<=' and value1 > value2:
                    record = []
                elif (tokens[6] == '<>' or tokens[6] == '!=') and value == tokens[7]:
                    record = []

            if len(record) != 0:      
                filteredRecord = []
                for j in range(len(columnNames)):
                    if columnNames[j] in columns:
                        filteredRecord.append(record[j])
                records.append(filteredRecord)
                
            lastInsert += payLoadSize+6
    
    records.append(columns)
    records = records[::-1]
    prettyPrint(records)
    
    
def parseInsert(command):
    try:
        tokens = command.split(" ")
        if(len(tokens)!=6 or tokens[1]!='into' or tokens[2]!='table' or tokens[4]!='values'):
            print("Invalid Syntax. Type \"help;\"\n")
            return
        
        tableName = tokens[3]
        
        if not os.path.exists("data/"+tableName+".tbl"):
            print("Table does not exist.\n")
            return
        
        if tokens[5][0] != '(' or tokens[5][-1] != ')':
            print("Invalid Syntax. Type \"help;\"\n")
            return
        
        values = tokens[5][1:-1].split(",")
        columnId, columnNames, dataTypes = getTableData("*",tableName)
        if len(dataTypes) > len(values):
            for i in range(len(values),len(dataTypes)):
                values.append("NULL")
                dataTypes[i] = 'null'
        
        insertValues("data/"+tableName+".tbl",values,dataTypes)
        print("Record inserted.")
    
    except:
        #print("Unexpected error:", sys.exc_info()[0])
        print("Invalid Syntax. Type \"help;\"\n")
        return
    

#utility to write data to table file    
def insertValues(filename,values,dataTypes):
    if not os.path.exists(filename):
        print("Table does not exist.\n")
        return

    with open(filename,"rb") as f:
        rowId,lastInsert = getRecordData(f)
    
    rowId += 1
    columns = len(dataTypes)
    recordHeader = [getHex(columns,'tinyint')]
    
    body = []
    payLoadSize = 0
    for i in range(len(values)):
        v = values[i]
        t = dataTypes[i]
        if (t=='text'):
            payLoadSize += len(v)
            tLen = len(v)
            body.append(getHex(v,t))
        elif (t=='null'):
            tLen = 0
        else:
            payLoadSize += dataTypeSize[t]
            tLen = 0
            body.append(getHex(v,t))
        recordHeader.append(getHex(dataTypeHex[t]+tLen,'tinyint'))
    
    for i in range(columns-len(values)):
        recordHeader.append(getHex(dataTypeHex['null'],'tinyint'))
              
    payLoadSize += len(recordHeader)
    lastInsert = lastInsert - payLoadSize - 6
    
    if (9+2*(rowId-1)) > lastInsert :
        print("Table is full. Aborting operation.")
        return
    
    with open(filename,"r+b") as f:
        f.seek(1)
        f.write(getHex(rowId,'smallint'))
        f.write(getHex(lastInsert,'smallint'))
        f.seek(9+2*(rowId-1))
        f.write(getHex(lastInsert,'smallint'))
        f.seek(lastInsert)
        f.write(getHex(payLoadSize,'smallint'))
        f.write(getHex(rowId,'int'))
        for i in recordHeader:
            f.write(i)
        for i in body:
            f.write(i)


def parseDrop(command):
    tokens = command.split(" ")
    if len(tokens)!=3 or tokens[1]!='table':
        print("Invalid Syntax. Type \"help;\"\n")
        return
    
    if(tokens[2] == "davisbase_columns" or tokens[2] == "davisbase_tables"):
        print("This operation is not allowed.")
        return
    
    filename = "data/"+tokens[2]+".tbl"
    if not os.path.exists(filename):
        print("Table does not exist.\n")
        return
    
    confirm = input("Confirm delete of table file? (y/n) ")
    if confirm == "y":
       
        with open("data/davisbase_tables.tbl","r+b") as t:
            rowId,lastInsert = getRecordData(t)
            for i in range(rowId):
                t.seek(lastInsert)
                payLoadSize = getData(bytearray(t.read(2)),'smallint')
                t.read(4)
                nCol = getData(bytearray(t.read(1)),'tinyint')
                types = []
                sizes = []
                for j in range(nCol):
                    size = getData(bytearray(t.read(1)),'tinyint')
                    if size>12:
                        types.append('text')
                        sizes.append(size-12)
                    else:
                        dt = hexDataType[size]
                        types.append(dt)
                        sizes.append(dataTypeSize[dt])
                
                t.seek(lastInsert+7+nCol)
                record = []
                for j in range(len(sizes)):
                    record.append(getData(t.read(sizes[j]),types[j]))
                    if j == 1 and record[j] == tokens[2]:
                        t.write(getHex(1,"tinyint"))
                        os.remove(filename) 
                        print("Table Deleted!") 
                        return
        
       
def parseShow(command):
    tokens = command.split(" ")
    if(len(tokens)!=2 or tokens[1]!="tables"):
        print("Invalid Syntax. Type \"help;\"\n")
        return
    parseSelect("select rowid,table_name from davisbase_tables where is_deleted = 0")

    
def parseHelp(command):
    print("*"*150)
    print("SUPPORTED COMMANDS\n")
    print("All commands below are case insensitive\n")
    print("SHOW TABLES;")
    print("\tDisplay the names of all tables.\n")
    print("CREATE TABLE <table_name> (<column_name1> INT PRIMARY KEY,<column_name2> <data_type> [NOT NULL],...);")
    print("\tCreate a table with given columns.\n")
    print("SELECT <column_list> FROM <table_name> [WHERE <condition>];")
    print("\tDisplay table records whose optional <condition>")
    print("\tis <column_name> = <value>.\n")
    print("DROP TABLE <table_name>;")
    print("\tRemove table data (i.e. all records) and its schema.\n")
    print("INSERT INTO TABLE <table_name> VALUES (<value1,value2,etc.>);")
    print("\tInsert the values into table <table_name>.\n")
    print("HELP;")
    print("\tDisplay this help information.\n")
    print("EXIT;")
    print("\tExit the program.\n")
    print("*"*150)

    
def splashScreen(version):
    print("-"*150)
    print("Welcome to DavisBaseNano")
    print("DavisBaseNano Version "+version)
    print("\nType \"help;\" to display supported commands.");
    print("-"*150)	


def getCommand(prompt):
    lines = []
    line = input(prompt)
    while True:
        lines.append(line)
        if ';' in line:
            break
        line = input()
    return ' '.join(lines)[:-1].replace("\r","",).strip().lower()


def parseCommand(command):
    commandTokens = command.split(" ")
    if commandTokens[0] == 'create': 
        parseCreate(command)
    elif commandTokens[0] == 'select': 
        parseSelect(command)
    elif commandTokens[0] == 'insert':
        parseInsert(command)
    elif commandTokens[0] == 'drop': 
        parseDrop(command)
    elif commandTokens[0] == 'show': 
        parseShow(command)
    elif commandTokens[0] == 'help': 
        parseHelp(command)
    else:
        print("Invalid Command.\n")


def initializeDB(pageSize):
    if not os.path.exists('data'):
        os.mkdir('data') 
    
    if not os.path.exists('data/davisbase_tables.tbl'):
        with open("data/davisbase_tables.tbl","wb") as dt:
            dt.write(getHex(13,'tinyint'))
            dt.seek(3)
            dt.write(getHex(pageSize,'smallint'))
            dt.write(getHex(-1,'int'))
        
        insertValues("data/davisbase_tables.tbl",[1,"davisbase_columns",0],['int','text','tinyint'])
        insertValues("data/davisbase_tables.tbl",[2,"davisbase_tables",0],['int','text','tinyint'])
    
    if not os.path.exists('data/davisbase_columns.tbl'):    
        with open("data/davisbase_columns.tbl","wb") as dc:
            dc.write(getHex(13,'tinyint'))
            dc.seek(3)
            dc.write(getHex(pageSize,'smallint'))
            dc.write(getHex(-1,'int'))
                  
        insertValues("data/davisbase_columns.tbl",[1,"davisbase_tables","rowid","INT",1,"NO"],['int','text','text','text','tinyint','text','null'])
        insertValues("data/davisbase_columns.tbl",[2,"davisbase_tables","table_name","TEXT",2,"NO"],['int','text','text','text','tinyint','text','null'])
        insertValues("data/davisbase_columns.tbl",[3,"davisbase_tables","is_deleted","TINYINT",3,"NO"],['int','text','text','text','tinyint','text','null'])
        insertValues("data/davisbase_columns.tbl",[4,"davisbase_columns","rowid","INT",1,"NO"],['int','text','text','text','tinyint','text','null'])
        insertValues("data/davisbase_columns.tbl",[5,"davisbase_columns","table_name","TEXT",2,"NO"],['int','text','text','text','tinyint','text','null'])
        insertValues("data/davisbase_columns.tbl",[6,"davisbase_columns","column_name","TEXT",3,"NO"],['int','text','text','text','tinyint','text','null'])
        insertValues("data/davisbase_columns.tbl",[7,"davisbase_columns","data_type","TEXT",4,"NO"],['int','text','text','text','tinyint','text','null'])
        insertValues("data/davisbase_columns.tbl",[8,"davisbase_columns","ordinal_position","TINYINT",5,"NO"],['int','text','text','text','tinyint','text','null'])
        insertValues("data/davisbase_columns.tbl",[9,"davisbase_columns","is_nullable","TEXT",6,"NO"],['int','text','text','text','tinyint','text','null'])
        insertValues("data/davisbase_columns.tbl",[10,"davisbase_columns","column_key","TEXT",7,"YES"],['int','text','text','text','tinyint','text','null'])
 
    
if __name__ == '__main__':
    
    prompt = "davisql> "
    version = "v1.0"
    pageSize = 2**10
    splashScreen(version);
    initializeDB(pageSize);
    while(True):
        command = getCommand(prompt)  
        if(command == "exit" or command == "quit" or command == "q"):
            break
        parseCommand(command)
    print("Exiting...")
 