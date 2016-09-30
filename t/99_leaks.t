use strict;
use Test::More;
use Redis::Jet;
use Test::LeakTrace;

no_leaks_ok {
    my $jet = Redis::Jet->new(server => 'localhost:6379');
    $jet->command(qw/set foo bar/);
}, 'normal check';

done_testing();

