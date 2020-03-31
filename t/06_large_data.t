use strict;
use Test::More;
use Redis::Jet;
use Test::RedisServer;
use File::Temp;
use Test::TCP;

my $tmp_dir = File::Temp->newdir( CLEANUP => 1 );
eval {
    my $redis_server = Test::RedisServer->new;
} or plan skip_all => 'redis-server is required for this test';

test_tcp(
    client => sub {
        my ($port, $server_pid) = @_;
        my $jet = Redis::Jet->new( server => 'localhost:'.$port, io_timeout => 5 );

        my $large_data = 'K' x 1048576; # 1mb
        is $jet->command(qw/set foo/, $large_data), 'OK';
        is $jet->command(qw/get foo/), $large_data;
        
        is $jet->command('set', $large_data, $large_data), 'OK';
        is $jet->command('get', $large_data), $large_data;

        is_deeply $jet->pipeline(['set', $large_data, $large_data]), ['OK'];
    },
    server => sub {
        my ($port) = @_;
        my $redis = Test::RedisServer->new(
            auto_start => 0,
            conf       => { port => $port },
            tmpdir     => $tmp_dir,
        );
        $redis->exec;
    },
);


done_testing();

