# 说明 

本文件夹内均可用作定时任务及单独运行

# 文件说明 

## delete_log.py 

根据配置，批量删除MySQL数据库中指定表的数据  
requirement：pymysql  
Python: 3.x  
需要配置信息，不可直接运行，如果使用Python2.x，请删除 `get_mysql` 参数 `*value` 
