requires 'perl', '5.008001';

on 'configure' => sub {
    requires 'Devel::CheckCompiler', '0.04';
};

on 'test' => sub {
    requires 'Test::More', '0.98';
    requires 'Test::TCP', '2';
    requires 'Test::RedisServer', '0.14';
};

