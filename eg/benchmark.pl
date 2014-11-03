#!/usr/bin/env perl

use strict;
use warnings;
use 5.10.0;
use Benchmark qw/cmpthese/;

use Redis::Fast;
use Redis::Jet;
use Redis;
use Redis::hiredis;


my $redis = Redis->new;
my $hiredis = Redis::hiredis->new(host=>'localhost',port=>'6379');
my $fast = Redis::Fast->new;
my $jet = Redis::Jet->new;
my $jet_noreply = Redis::Jet->new(noreply=>1);

my @val = $jet->command(qw!set foo foovalue!);
use Data::Dumper;
say Dumper(\@val);
say "fast:", $fast->get('foo');
say "jet:", $jet->command(qw/get foo/);

print "single get =======\n";

cmpthese(
    -3,
    {
        fast => sub {
            my $val = $fast->get('foo');
        },
        jet => sub {
            my $val = $jet->command(qw/get foo/);
        },
        redis => sub {
            my $data = $redis->get('foo');
        },
        hiredis => sub {
            my $val = $hiredis->command(qw/get foo/);
        },
    }
);

print "single incr =======\n";

cmpthese(
    -3,
    {
        fast => sub {
            my $val = $fast->incr('incrfoo');
        },
        jet => sub {
            my $val = $jet->command(qw/incr incrfoo/);
        },
        jet_noreply => sub {
            $jet_noreply->command(qw/incr incrfoo/);
        },
        redis => sub {
            my $val = $redis->incr('incrfoo');
        },
        hiredis => sub {
            my $val = $hiredis->command(qw/incr incrfoo/);
        },
    }
);

print "pipeline =======\n";

my $cb = sub {};
cmpthese(
    -3,
    {
        fast => sub {
            my @res;
            my $cb = sub { push @res, \@_ };
            $fast->del('user-fail',$cb);
            $fast->del('ip-fail',$cb);
            $fast->lpush('user-log','xxxxxxxxxxx',$cb);
            $fast->lpush('login-log','yyyyyyyyyyy',$cb);
            $fast->wait_all_responses;
        },
        jet => sub {
            my @res = $jet->pipeline(
                [qw/del user-fail/],
                [qw/del ip-fail/],
                [qw/lpush user-log xxxxxxxxxxx/],
                [qw/lpush login-log yyyyyyyyyyy/]
            );
        },
        jet_noreply => sub {
            $jet_noreply->pipeline(
                [qw/del user-fail/],
                [qw/del ip-fail/],
                [qw/lpush user-log xxxxxxxxxxx/],
                [qw/lpush login-log yyyyyyyyyyy/]
            );
        },
        redis => sub {
            my @res;
            my $cb = sub { push @res, \@_ };
            $redis->del('user-fail',$cb);
            $redis->del('ip-fail',$cb);
            $redis->lpush('user-log','xxxxxxxxxxx',$cb);
            $redis->lpush('login-log','yyyyyyyyyyy',$cb);
            $redis->wait_all_responses;
        },
    }
);



