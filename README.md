# CI-dataMapper

CI 3.0 框架复制出来的数据库操作类

## 如何使用
```
//直接手动传入数据库配置信息，其它用法请参考CI官网
require_once('db.php');
$config = [
    'hostname' => 'localhost',
    'username' => 'root',
    'password' => '',
    'database' => 'test',
];
$db = DB($config);
$info = $db->where('id',1)->get('info')->limit(1)->row_array();
print_r($info);
```
more example  at https://www.codeigniter.com/user_guide/database/query_builder.html
