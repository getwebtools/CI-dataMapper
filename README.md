# CI-dataMapper
a single file mysql database lib from codeignite 3.0    
CI 3.0 框架的数据库操作类封装的还是比较棒的，用起来也很舒服，由于想要脱离CI框架使用该类库，故删繁就简，针对mysql数据库和mysqli驱动器复制了一份组合成单文件，方便直接引入使用，仅复制了基本的驱动器，缓存和结果集以及查询构造器类，如果有需要可以根据需要自行复制改造。


## how to use
```
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
