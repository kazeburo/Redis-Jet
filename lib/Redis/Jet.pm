package Redis::Jet;

use 5.010001;
use strict;
use warnings;
use POSIX qw(EINTR EAGAIN EWOULDBLOCK :sys_wait_h);
use IO::Socket qw(:crlf IPPROTO_TCP TCP_NODELAY);
use IO::Socket::INET;
use IO::Socket::UNIX;
use IO::Select;
use Time::HiRes qw/time/;
use base qw/Exporter/;

our @EXPORT_OK = qw/
                    parse_message parse_message_utf8
                /;

our $VERSION = "0.08";

use XSLoader;
XSLoader::load(__PACKAGE__, $VERSION);

sub new {
    my $class = shift;
    my %args = ref $_[0] ? %{$_[0]} : @_;
    %args = (
        server => '127.0.0.1:6379',
        connect_timeout => 5,
        io_timeout => 1,
        utf8 => 0,
        noreply => 0,
        reconnect_attempts => 0,
        reconnect_delay => 0.1,
        %args,
    );
    $class->_new(\%args);
}

sub set_socket {
    my ($self,$socket) = @_;
    $self->get_bucket->{socket} = $socket;
    $self->set_fileno(fileno($socket));
}

sub connect {
    my $self = shift;
    my $socket;
    if ($self->get_server =~ m!/!) {
        $socket = IO::Socket::UNIX->new(
            Peer => $self->get_server,
            Timeout => $self->get_connect_timeout,
        ) or return;
    } else {
        $socket = IO::Socket::INET->new(
            PeerAddr => $self->get_server,
            Timeout => $self->get_connect_timeout,
        ) or return;
        $socket->setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
            or die "setsockopt(TCP_NODELAY) failed:$!";
    }
    $socket->blocking(0) or die $!;
    $self->set_socket($socket);
    $socket;
}

sub DESTROY {
    $_[0]->_destroy();
}


1;
__END__

=encoding utf-8

=head1 NAME

Redis::Jet - Yet another XS implemented Redis Client

=head1 SYNOPSIS

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

=head1 DESCRIPTION

This project is still in a very early development stage.
IT IS NOT READY FOR PRODUCTION!

Redis::Jet is yet another XS implemented Redis Client. This module provides
simple interfaces to communicate with Redis server

=head1 METHODS

=over 4

=item C<< my $obj = Redis::Jet->new(%args) >>

Create a new instance. Can also accept args as hashref.

=over 4

=item C<< server => "server:port" >>

server address and port. For using unix_socket pass socket path to server like `server => '/path/to/redis.sock'. When server includes `/`, Redis::Jet treat server as unix_socket.

=item connect_timeout

Time seconds to wait for establish connection. default: 5

=item io_timeout

Time seconds to wait for send request and read response. default: 1

=item utf8

If enabled, Redis::Jet does encode/decode strings. default: 0 (false)

=item noreply

IF enabled. The instance does not parse any responses. Every responses to be C<"0 but true">. default: 0 (false)

=item reconnect_attempts

If Redis::Jet could not connect to redis server or failed to write requests, Redis::Jet attempts to re-connect. This parameter specify how many times to try reconnect. default: 0 (disable reconnect feature);

=item reconnect_delay

Redis::Jet inserts delay before reconnect redis-server (see C<reconnect_attempts>). default: 0.1 (100msec)

=back

=item C<< ($value,[$error]) = $obj->command($command,$args,$args) >>

send a command and retrieve a value

=item C<< @values = $obj->pipeline([$command,$args,$args],[$command,$args,$args]) >>

send several commands and retrieve values. each value has value and error string if error was occurred.

=back

=head1 BENCHMARK

https://gist.github.com/kazeburo/608fc2ba0748e6ab3663

=head1 SEE ALSO

* L<Redis>

* L<Redis::Fast>

* L<Redis::hiredis>

* http://redis.io/

=head1 LICENSE

Copyright (C) Masahiro Nagano.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Masahiro Nagano E<lt>kazeburo@gmail.comE<gt>

=cut

