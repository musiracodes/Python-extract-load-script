import gzip
import os
import cx_Oracle

# Environment variables
os.environ['LD_LIBRARY_PATH'] = 'LIBRARY_PATH'
os.environ['ORACLE_HOME'] = 'ORACLE_HOME'
os.environ['ORACLE_SID'] = 'ORACLE_SID'

# connection to the db
con = cx_Oracle.connect('user/password@hostIP/database')

print("\nConnected to Oracle!!\n")

# reads the compressed file without extracting it
lft = gzip.open("PATH_TO_COMPRESSED_FILE.GZ", "r")

##################### First Table ####################
p = lft.readline()
col_one = p[:1]
col_two = p[1:3]
col_n = p[20:30]  # upto column n

##################### Loading Data To First Table ####################
print("Loading Data To First Record Table....")
myCur = con.cursor()
# upto nth
queryTableOne = 'insert into TABLE_ONE (COL_ONE,COL_TWO,COL_N) values ( :col_one, :col_two, :col_n)'
myCur.execute(queryTableOne, col_one=col_one,
              col_two=col_two, col_n=col_n)  # upto nth
con.commit()

print("Loading Data To First Record Table Completed!!\n")

print("###########################################################\n")

##################### Second Table #####################

# first pass to count
n_lines = sum(1 for line in lft)
n_skip = 1

# second pass to actually do something
lft = gzip.open("PATH_TO_COMPRESSED_FILE.GZ", "r")

print("Loading Data To Data Record Tables....Please Wait....")
t = lft.readline()
for i_line in range(n_lines - n_skip):  # does nothing if n_lines <= n_skip
    i = lft.readline()
    col_one = p[:1]
    col_two = p[1:3]
    col_n = p[20:30]  # upto column n

    myCur = con.cursor()
    # query statement
    # upto nth
    queryTableTwo = 'insert into TABLE_TWO (COL_ONE,COL_TWO,COL_N) values ( :col_one, :col_two, :col_n)'

    # Execution Statement
    myCur.execute(queryTableTwo, col_one=col_one,
                  col_two=col_two, col_n=col_n)  # upto nth
    con.commit()

print("Loading Data to Data Record Tables Completed!!\n")

print("###########################################################\n")

# #################### Trailer Record ####################
j = lft.readlines()[-1]

col_one = p[:1]
col_two = p[1:3]
col_n = p[20:30]  # upto column n

print("Loading Data To Trailer Record Table....")
myCur = con.cursor()
# upto nth
queryTableThree = 'insert into TABLE_THREE (COL_ONE,COL_TWO,COL_N) values ( :col_one, :col_two, :col_n)'
myCur.execute(queryTableThree, col_one=col_one,
              col_two=col_two, col_n=col_n)  # upto nth
con.commit()
con.close()
print("Loading Data To Trailer Record Table Completed!!\n")
