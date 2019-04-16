# CI-dataMapper
a single file mysql database lib from codeignite 3.0
从CI框架中复制出来的单文件Mysql数据库操作类，只选取了mysqli驱动方式连接MySQL数据库，需要其它的驱动器或数据库，可以自行复制修改一下

## how to use
`
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
`
more example at https://www.codeigniter.com/user_guide/database/query_builder.html
