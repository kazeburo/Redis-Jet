use strict;
use Test::More;
use Redis::Jet;

my $jet = Redis::Jet->new(server => 'localhost:6379');
my $large_data = 'K' x 1048576; # 1mb

is $jet->command(qw/set foo/, $large_data), 'OK';
is $jet->command(qw/get foo/), $large_data;

is $jet->command('set', $large_data, $large_data), 'OK';
is $jet->command('get', $large_data), $large_data;

done_testing();

