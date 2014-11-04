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
        my $jet = Redis::Jet->new( server => 'localhost:'.$port, utf8 => 1, io_timeout => 5 );
        is($jet->command(qw/set foo5/,"\xE5"),'OK');
        is($jet->command(qw/set bar5/,"\x{263A}"),'OK');
        is($jet->command(qw/get foo5/),"\xE5");
        is_deeply([$jet->command(qw/get foo5/)],["\xE5"]);
        is_deeply([$jet->command(qw/get bar5/)],["\x{263A}"]);

        {
          use utf8;
          my $large_data = 'あいう'x61440;
          is($jet->command(qw/set large-foo/,$large_data),'OK');
          is($jet->command(qw/get large-foo/),$large_data);
        };

        my $jet2 = Redis::Jet->new( server => 'localhost:'.$port, utf8 => 0, io_timeout => 5 );
        is($jet2->command(qw/set foo5/,"\xE5"),'OK',"re-1");
        is($jet2->command(qw/set bar5/,"\x{263A}"),'OK');
        is($jet2->command(qw/get foo5/),"\xE5");
        is_deeply([$jet2->command(qw/get foo5/)],["\xE5"]);
        is_deeply([$jet2->command(qw/get bar5/)],["\xE2\x98\xBA"]);
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

