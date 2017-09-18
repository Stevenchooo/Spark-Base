-- 每个Map最大输入大小
set mapred.max.split.size = 300000000;
-- 每个Map最小输入大小
set mapred.min.split.size = 100000000;
-- 执行Map前进行小文件合并
set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-- hive自动根据sql，选择使用common join或者map join
set hive.auto.convert.join = false;
-- 在Map-only的任务结束时合并小文件
set hive.merge.mapfiles = true;
-- 在Map-Reduce的任务结束时不合并小文件
set hive.merge.mapredfiles = false;
-- 合并文件的大小
set hive.merge.size.per.task = 256000000;
-- 一个节点上split的至少的大小
set mapred.min.split.size.per.node=256000000;
-- 当输出文件的平均大小小于该值时，启动一个独立的map-reduce任务进行文件merge
set hive.merge.smallfiles.avgsize=256000000;
-- 是否支持可切分的 CombieInputFormat 
set hive.hadoop.supports.splittable.combineinputformat=true;
-- 一个交换机下split的至少的大小
set mapred.min.split.size.per.rack=256000000;
-- 设置最大切片大小
set mapreduce.input.fileinputformat.split.maxsize=256000000;

set hive.groupby.skewindata=true;
---数据倾斜

set mapreduce.map.memory.mb=6144;
set mapreduce.reduce.memory.mb=6144;
set mapreduce.map.java.opts=-Xmx4915M -Djava.net.preferIPv4Stack=true；
set mapreduce.reduce.java.opts=-Xmx4915M -Djava.net.preferIPv4Stack=true;


并发
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16;


不分区
set hive.mapred.mode=nonstrict; 

如果需要使用hive的表自动分区，必须在HQL头部设置动态分区参数。
示例：
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


任务名参数设置
set mapred.job.name='LBI_SOR.T_SP_COST_DTL_D_MID2_01'; 


改名： ALTER TABLE table_name RENAME TO new_table_name
use lbi_dpa;
alter table t_od_trf_shf_d  rename to  t_od_allot_shf_d;

to_char  
from_unixtime 转换成日期

regexp_replace((from_unixtime(unix_timestamp('${hivevar:statisdate}','yyyyMMdd'),'yyyy-MM-dd')),'-','')

FROM_UNIXTIME(unix_timestamp(buy_date,'dd/MMM/yyyy:HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')


hive时间差

DATEDIFF('2015-01-19 13:58:41',from_unixtime(unix_timestamp(concat(erdat,erzet),'yyyyMMddHHmmss')))

取当前时间

select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') from dual;


case when substr(statis_date,1,4)=substr(regexp_replace(from_unixtime(unix_timestamp() ,'yyyy-MM-dd HH:mm:ss'),'-','') ,1,4)  then '全年'
    when substr(statis_date,1,4)=substr(regexp_replace(from_unixtime(unix_timestamp() ,'yyyy-MM-dd HH:mm:ss'),'-','') ,1,4)-1  then '去年'
    when substr(statis_date,1,4)=substr(regexp_replace(from_unixtime(unix_timestamp() ,'yyyy-MM-dd HH:mm:ss'),'-','') ,1,4)-2 then '前年'
 end statis_year

秒数差：

unix_timestamp('20150105100220','yyyyMMddHHmmss')-unix_timestamp('20150105100320','yyyyMMddHHmmss')


concat('\'',(regexp_replace(regexp_replace(LGS_ID                 ,'\\n',' '),',',' ')),'\'')

导出
hive  -S -e "select * from lbi_sor.T_WH_REFNR_tmp">tangze.csv

导数据到hive表
mkdir -p t_dim_depot_amount_stat_d
cd ./t_dim_depot_amount_stat_d
hadoop fs -ls /user/lbi/hive/warehouse/lbi_dm.db/t_dim_depot_amount_stat_d
hadoop fs -get /user/lbi/hive/warehouse/lbi_dm.db/t_dim_depot_amount_stat_d/*
每个表建一个文件夹 把数据拉下来


拼接字段
lpad(a.matnr,18,'0')=lpad(m.matnr,18,'0')

修改字段
use lbi_dpa;
alter table t_od_sts_ep_d_tmp2  change   column  1070_FLAG   T1070_FLAG  string

hive_etl
查看表分区属性：
Desc formatted xxxxxx partition(pt_d=xxxxxxxx) 
查看表分区属性：
Desc formatted xxxxxx 
重建表分区：
Alter table xxxxx drop  if exists partition(pt_d=xxxxxxx);
Alter table xxxxxx add if not exists partition(pt_d=xxxxxxx)
修改表属性
Alter table xxx set fileformat orc;

1、分区表加字段：
alter table xj_test1 add columns (c string);

查看表的CD_ID：select CD_ID from SDS where LOCATION='hdfs://n1:8020/user/hive/warehouse/xj_test1' ，（假设我们这里查出来表的新的CD_ID值为35178）

查看表的所有分区的CD_ID：SELECT * FROM SDS WHERE LOCATION LIKE 'hdfs://n1:8020/user/hive/warehouse/xj_test1/dt=%'

我们需要手工更新一下现有分区的CD_ID的值为表CD_ID的值：

UPDATE SDS SET CD_ID=35178 WHERE LOCATION LIKE 'hdfs://n1:8020/user/hive/warehouse/xj_test1/dt=%'

然后我们再去查询一下表xj_test1，字段c的值可以正常的显示出来了。


当然，删除分区重建也是可以的，具体选择哪种方法根据实际情况决定，毕竟删除分区是会删除数据的，如果不允许删除原来的数据，建议还是修改元数据。

1.1 删除了分区表又重建，原来路径下的数据还在，
要在重新建表以后执行一个msck
hive Recover Partitions命令MSCK REPAIR TABLE table_name


1.2 性能优化，分布式处理数据
DISTRIBUTE BY unique_id;
hive中的distribute by是控制在map端如何拆分数据给reduce端的。
hive会根据distribute by后面列，根据reduce的个数进行数据分发，默认是采用hash算法。
对于distribute by进行测试，一定要分配多reduce进行处理，否则无法看到distribute by的效果。


55.        AESEncrypt4AD
56.        AESDecrypt4AD  