import MySQLdb
import pandas as pd
user = '****'
password = '****'
database = 'test'
mc = MySQLdb.connect('localhost',user,password,database) 
cursor = mc.cursor()
nr_customers = 100
colnames = ["movie%d" %i for i in range(1,33)]
pd.np.random.seed(2015)
generated_customers = pd.np.random.randint(0,2,32 * 
nr_customers).reshape(nr_customers,32)
data = pd.DataFrame(generated_customers, columns = list(colnames)) 
data.to_sql('cust',mc, flavor = 'mysql', index = True, if_exists = 
'replace', index_label = 'cust_id')
def createNum(x1,x2,x3,x4,x5,x6,x7,x8): 
 return [int('%d%d%d%d%d%d%d%d' % (i1,i2,i3,i4,i5,i6,i7,i8),2) 
for (i1,i2,i3,i4,i5,i6,i7,i8) in zip(x1,x2,x3,x4,x5,x6,x7,x8)]
assert int('1111',2) == 15
assert int('1100',2) == 12
assert createNum([1,1],[1,1],[1,1],[1,1],[1,1],[1,1],[1,0],[1,0]) 
== [255,252]
store = pd.DataFrame()
store['bit1'] = createNum(data.movie1, 
data.movie2,data.movie3,data.movie4,data.movie5,
data.movie6,data.movie7,data.movie8)
store['bit2'] = createNum(data.movie9, 
data.movie10,data.movie11,data.movie12,data.movie13,
data.movie14,data.movie15,data.movie16)
store['bit3'] = createNum(data.movie17, 
data.movie18,data.movie19,data.movie20,data.movie21,
data.movie22,data.movie23,data.movie24)
store['bit4'] = createNum(data.movie25, 
data.movie26,data.movie27,data.movie28,data.movie29,
data.movie30,data.movie31,data.movie32)
def hash_fn(x1,x2,x3): 
 return [b'%d%d%d' % (i,j,k) for (i,j,k) in zip(x1,x2,x3)]
assert hash_fn([1,0],[1,1],[0,0]) == [b'110',b'010'] 
store['bucket1'] = hash_fn(data.movie10, data.movie15,data.movie28)
store['bucket2'] = hash_fn(data.movie7, data.movie18,data.movie22)
store['bucket3'] = hash_fn(data.movie16, data.movie19,data.movie30)
store.to_sql('movie_comparison',mc, flavor = 'mysql', index = True, 
index_label = 'cust_id', if_exists = 'replace') 
def createIndex(column, cursor): 
 sql = 'CREATE INDEX %s ON movie_comparison (%s);' % (column, column)
 cursor.execute(sql)
createIndex('bucket1',cursor) 
createIndex('bucket2',cursor) 
createIndex('bucket3',cursor) 
Sql = '''
CREATE FUNCTION HAMMINGDISTANCE(
 A0 BIGINT, A1 BIGINT, A2 BIGINT, A3 BIGINT, 
 B0 BIGINT, B1 BIGINT, B2 BIGINT, B3 BIGINT
)
RETURNS INT DETERMINISTIC 
RETURN
 BIT_COUNT(A0 ^ B0) +
 BIT_COUNT(A1 ^ B1) +
 BIT_COUNT(A2 ^ B2) +
 BIT_COUNT(A3 ^ B3); ''' 
cursor.execute(Sql) 
Sql = '''Select hammingdistance(
 b'11111111',b'00000000',b'11011111',b'11111111'
,b'11111111',b'10001001',b'11011111',b'11111111'
)''' 
pd.read_sql(Sql,mc) 
customer_id = 27
sql = "select * from movie_comparison where cust_id = %s" % customer_id 
cust_data = pd.read_sql(sql,mc)
sql = """ select cust_id,hammingdistance(bit1,
bit2,bit3,bit4,%s,%s,%s,%s) as distance
from movie_comparison where bucket1 = '%s' or bucket2 ='%s' 
or bucket3='%s' order by distance limit 3""" % 
(cust_data.bit1[0],cust_data.bit2[0], 
cust_data.bit3[0], cust_data.bit4[0],
cust_data.bucket1[0], cust_data.bucket2[0],cust_data.bucket3[0])
shortlist = pd.read_sql(sql,mc) 
cust = pd.read_sql('select * from cust where cust_id in (27,2,97)',mc)
dif = cust.T 
dif[dif[0] != dif[1]]
