use strict;
use Test::More;
use Redis::Jet;
use File::Temp;
use Test::TCP;
use IO::Socket::INET;
use IO::Select;

# corrupted
test_tcp(
    client => sub {
        my ($port, $server_pid) = @_;
        my $jet = Redis::Jet->new( server => 'localhost:'.$port, io_timeout => 5 );
        is_deeply([$jet->pipeline(qw/ping ping ping ping/)],[
            ['OK'],
            ['OK'],
            [undef,'failed to read message: corrupted message found'],
            [undef,'failed to read message: corrupted message found']
        ]);
    },
    server => sub {
        my ($port) = @_;
        my $sock = IO::Socket::INET->new(
            LocalAddr => 'localhost',
            LocalPort => $port,
            Listen => 10,
            ReuseAddr => 1,
            Proto => 'tcp'
        );
        my $i=0;
        while ( my $client = $sock->accept ) {
            $i++;
            my $s = IO::Select->new($client);
            $s->can_read(5);
            $client->sysread(my $buf,1024);
            $client->syswrite(join("\r\n","+OK","+OK","^OK","+OK",""));
            $client->close;
        }
    },
);

# timeout
test_tcp(
    client => sub {
        my ($port, $server_pid) = @_;
        my $jet = Redis::Jet->new( server => 'localhost:'.$port, io_timeout => 5 );
        is_deeply([$jet->pipeline(qw/ping ping ping ping/)],[
            ['OK'],
            ['OK'],
            [undef,'failed to read message: Resource temporarily unavailable'],
            [undef,'failed to read message: Resource temporarily unavailable']
        ]);
    },
    server => sub {
        my ($port) = @_;
        my $sock = IO::Socket::INET->new(
            LocalAddr => 'localhost',
            LocalPort => $port,
            Listen => 10,
            ReuseAddr => 1,
            Proto => 'tcp'
        );
        my $i=0;
        while ( my $client = $sock->accept ) {
            $i++;
            my $s = IO::Select->new($client);
            $s->can_read(5);
            $client->sysread(my $buf,1024);
            $client->syswrite(join("\r\n","+OK","+OK",""));
            $client->close;
        }
    },
);


done_testing;

