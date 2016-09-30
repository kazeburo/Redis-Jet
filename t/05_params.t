use strict;
use Test::More;
use Redis::Jet;

eval { Redis::Jet->new( server => 'localhost:6379', utf8 => -1 ); };
ok $@ =~ /^utf8 must be larger than zero/;

eval { Redis::Jet->new( server => 'localhost:6379', connect_timeout => -1 ); };
ok $@ =~ /^connect_timeout must be larger than zero/;

eval { Redis::Jet->new( server => 'localhost:6379', io_timeout => -1 ); };
ok $@ =~ /^io_timeout must be larger than zero/;

eval { Redis::Jet->new( server => 'localhost:6379', noreply => -1 ); };
ok $@ =~ /^noreply must be larger than zero/;

eval { Redis::Jet->new( server => 'localhost:6379', reconnect_attempts => -1 ); };
ok $@ =~ /^reconnect_attempts must be larger than zero/;

eval { Redis::Jet->new( server => 'localhost:6379', reconnect_delay => -1 ); };
ok $@ =~ /^reconnect_delay must be larger than zero/;

done_testing();

