# NAME

Redis::Jet - Yet another XS implemented Redis Client

# SYNOPSIS

    use Redis::Jet;
    
    my $jet = Redis::Jet->new( server => 'localhost:6379' );
    my $ret = $jet->command(qw/set redis data-server/); # $ret eq 'OK'
    my $value = $jet->command(qw/get redis/); # $value eq 'data-server'
    
    my $ret = $jet->command(qw/set memcached cache-server/);
    my $values = $jet->command(qw/mget redis memcached mysql/);
    # $values eq ['data-server','memcached',undef]
    
    ## error handling
    ($values,$error) = $jet->command(qw/get redis memcached mysql/);
    # $error eq q!ERR wrong number of arguments for 'get' command!

    ## pipeline
    my @values = $jet->pipeline([qw/get redis/],[qw/get memcached/]);
    # \@values = [['data-server'],['cache-server']]

    my @values = $jet->pipeline([qw/get redis/],[qw/get memcached mysql/]);
    # \@values = [['data-server'],[undef,q!ERR wrong...!]]

# DESCRIPTION

This is project is still in a very early development stage.
IT IS NOT READY FOR PRODUCTION!

Redis::Jet is yet another XS implemented Redis Client. This module provides
simple interfaces to communicate with Redis server

# METHODS

- `my $obj = Redis::Jet->new(%args)`

    Create a new instance.

    - `server => "server:port"`

        server address and port

    - connect\_timeout

        Time seconds to wait for establish connection. default: 5

    - io\_timeout

        Time seconds to wait for send request and read response. default: 1

    - utf8

        If enabled, Redis::Jet does encode/decode strings. default: 0 (false)

    - noreply

        IF enabled. The instance does not parse any responses. Every responses to be `"0 but true"`. default: 0 (false)

- `($value,[$error]) = $obj->command($command,$args,$args)`

    send a command and retrieve a value

- `@values = $obj->pipeline([$command,$args,$args],[$command,$args,$args])`

    send several commands and retrieve values. each value has value and error string if error was occurred.

# BENCHMARK

    single get =======
                Rate   redis    fast hiredis     jet
    redis    45378/s      --    -59%    -71%    -74%
    fast    111655/s    146%      --    -28%    -37%
    hiredis 154429/s    240%     38%      --    -13%
    jet     177292/s    291%     59%     15%      --
    single incr =======
                Rate   redis    fast hiredis     jet
    redis    48830/s      --    -58%    -70%    -72%
    fast    116381/s    138%      --    -29%    -33%
    hiredis 163837/s    236%     41%      --     -6%
    jet     174880/s    258%     50%      7%      --
    pipeline =======
              Rate redis  fast   jet
    redis  15514/s    --  -73%  -87%
    fast   57985/s  274%    --  -50%
    jet   116536/s  651%  101%    --
    
    Physical server
    Intel(R) Xeon(R) CPU E3-1240 v3 @ 3.40GHz | 4core/8thread    
    redis-2.8.17
    Perl-5.20.1
    Redis 1.976
    Redis::Fast 0.13
    Redis::hiredis 0.11.0

# SEE ALSO

\* [Redis](https://metacpan.org/pod/Redis)

\* [Redis::Fast](https://metacpan.org/pod/Redis::Fast)

\* [Redis::hiredis](https://metacpan.org/pod/Redis::hiredis)

\* http://redis.io/

# LICENSE

Copyright (C) Masahiro Nagano.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

Masahiro Nagano <kazeburo@gmail.com>
