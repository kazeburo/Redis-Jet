use strict;
use Test::More;
use Redis::Jet;
use Test::RedisServer;
use File::Temp;
use Test::TCP;

my $tmp_dir = File::Temp->newdir( CLEANUP => 1 );

test_tcp(
    client => sub {
        my ($port, $server_pid) = @_;
        my $jet = Redis::Jet->new( server => 'localhost:'.$port, io_timeout => 5 );
        is($jet->command(qw/set foo foovalue/),'OK');
        is($jet->command(qw/set bar barvalue/),'OK');
        is($jet->command(qw/get foo/),'foovalue');
        is_deeply([$jet->command(qw/get foo/)],['foovalue']);
        is_deeply([$jet->command(qw/get foo bar/)],[undef,q!ERR wrong number of arguments for 'get' command!]);
        is_deeply($jet->command(qw/mget foo bar/),[qw/foovalue barvalue/]);
        is_deeply($jet->command(qw/mget foo bar baz/),[qw/foovalue barvalue/,undef]);

        is_deeply([$jet->pipeline(qw/ping ping ping/)],[['PONG'],['PONG'],['PONG']]);
        is_deeply([$jet->pipeline([qw/get foo/],[qw/get bar/])],[['foovalue'],['barvalue']]);
        is_deeply([$jet->pipeline([qw/get foo/],'ping',[qw/get bar/])],[['foovalue'],['PONG'],['barvalue']]);
        is_deeply([$jet->pipeline([qw/get foo/],[qw/get bar baz/])],
                  [['foovalue'], [undef,q!ERR wrong number of arguments for 'get' command!]]);

        is($jet->command(qw/set hoge/,''),'OK');
        is($jet->command(qw/get hoge/),'');

        # large data
        my $large_data = 'あいう'x81140;
        is($jet->command(qw/set large-foo/,$large_data),'OK');
        is($jet->command(qw/get large-foo/),$large_data);
        is_deeply([$jet->pipeline([qw/get large-foo/])],[[$large_data]]);

        # large pipeline
        my @lreq;
        push @lreq, ['ping'] for 1..100;
        my @lres = $jet->pipeline(@lreq);
        is( scalar @lres, 100);
        my @pong_lres = grep { $_->[0] eq 'PONG' } @lres;
        is( scalar @pong_lres, 100);
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

