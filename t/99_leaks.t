use strict;
use Test::More;
use Redis::Jet;
use Test::RedisServer;
use File::Temp;
use Test::TCP;
use Test::LeakTrace;


my $tmp_dir = File::Temp->newdir( CLEANUP => 1 );
eval {
    my $redis_server = Test::RedisServer->new;
} or plan skip_all => 'redis-server is required for this test';

test_tcp(
    client => sub {
        my ($port, $server_pid) = @_;
        no_leaks_ok {
            my $jet = Redis::Jet->new( server => 'localhost:'.$port, io_timeout => 5 );
            $jet->command(qw/set foo bar/);
            $jet->command(qw/get foo/);
        }, 'normal check';
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

