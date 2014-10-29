use strict;
use Test::More;
use Redis::Jet;

#is(Redis::Jet::build_message(qw/set foofoo/), 
#   join("\015\012",qw/*2 $3 set $6 foofoo/,""));
#is(Redis::Jet::build_message([qw/set foofoo/],['ping']),
#   join("\015\012",qw/*2 $3 set $6 foofoo *1 $4 ping/,""));
#
#is(Redis::Jet::build_message('mget',"\xE5","\x{263A}"),
#   join("\015\012",qw/*3 $4 mget $1/,"\xE5",qw/$3/,"\xE2\x98\xBA","")
#);
#is(Redis::Jet::build_message_utf8('mget',"\xE5","\x{263A}"),
#   join("\015\012",qw/*3 $4 mget $2/,"\xC3\xA5",qw/$3/,"\xE2\x98\xBA","")
#);

sub parse_deeply {
    my ($msg,$utf8,@ret) = @_;
    my @out;
    my $ret = $utf8 ? Redis::Jet::parse_message_utf8($msg,\@out) : Redis::Jet::parse_message($msg,\@out);
    is($ret,length $msg, $msg);
    is_deeply(\@out,\@ret);
}

parse_deeply(
    join("\015\012",qw/+OK/,""),
    0,
    [qw/OK/]
);

parse_deeply(
    join("\015\012",qw/-boofy/,""),
    0,
    [undef,'boofy']
);

parse_deeply(
    join("\015\012",qw/$5 boofy/,""),
    0,
    ['boofy']
);

parse_deeply(
    join("\015\012",qw/*2 $3 set $6 foofoo/,""),
    0,
    [[qw/set foofoo/]]
);

parse_deeply(
    join("\015\012",qw/*2 $3 set $6 foofoo *1 $4 ping/,""),
    0,
    [[qw/set foofoo/]],[[qw/ping/]]
);

parse_deeply(
    join("\015\012",qw/*3 $4 mget $1/,"\xE5",qw/$3/,"\xE2\x98\xBA",""),
    0,
    [['mget',"\xE5","\xE2\x98\xBA"]]
);

parse_deeply(
    join("\015\012",qw/*3 $4 mget $2/,"\xC3\xA5",qw/$3/,"\xE2\x98\xBA",""),
    1,
    [['mget',"\xE5","\x{263A}"]],
);

done_testing;


