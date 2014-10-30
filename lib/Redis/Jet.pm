package Redis::Jet;

use 5.008005;
use strict;
use warnings;
use POSIX qw(EINTR EAGAIN EWOULDBLOCK :sys_wait_h);
use IO::Socket qw(:crlf IPPROTO_TCP TCP_NODELAY);
use IO::Socket::INET;
use IO::Select;
use Time::HiRes qw/time/;
use base qw/Exporter/;

our @EXPORT_OK = qw/
                    parse_message parse_message_utf8
                /;

our $VERSION = "0.01";

use XSLoader;
XSLoader::load(__PACKAGE__, $VERSION);

sub new {
    my $class = shift;
    my %args = ref $_ ? %{$_[0]} : @_;
    %args = (
        server => '127.0.0.1:6379',
        connect_timeout => 5,
        io_timeout => 1,
        utf8 => 0,
        noreply => 0,
        %args,
    );
    my $server = shift;
    my $self = bless \%args, $class;
    $self;
}

sub connect {
    my $self = shift;
    return $self->{sock} if $self->{sock};
    my $socket = IO::Socket::INET->new(
        PeerAddr => $self->{server},
        Timeout => $self->{connect_timeout},
    ) or return;
    $socket->setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        or die "setsockopt(TCP_NODELAY) failed:$!";
    $socket->blocking(0) or die $!;
    $self->{sock} = $socket;
    $self->{fileno} = fileno($socket);
    $socket;
}

sub res_error {
    my $self = shift;
    delete $self->{sock};
    delete $self->{fileno};
    if ( @_ == 1 ) {
        return (undef,$_[0]);
    }
    my @res;
    push @res,[undef,$_[0]] for 1..$_[1];
    return @res;
}

sub command {
    my $self = shift;
    return unless @_;
    if ( !$self->{fileno} ) {
        $self->connect;
        $self->{fileno} or return $self->res_error('cannot connect to redis server: '. (($!) ? "$!" : "timeout"));
    }
    run_command($self, @_);
}

sub pipeline {
    my $self = shift;
    return unless @_;
    my $pipeline = @_;
    if ( !$self->{fileno} ) {
        $self->connect;
        $self->{fileno} or return $self->res_error('cannot connect to redis server: '. (($!) ? "$!" : "timeout"), $pipeline);
    }
    run_command_pipeline($self, @_);
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

This is project is still in a very early development stage.
IT IS NOT READY FOR PRODUCTION!

Redis::Jet is yet another XS implemented Redis Client. This module provides
simple interfaces to communicate with Redis server

=head1 METHODS

=over 4

=item C<< my $obj = Redis::Jet->new(%args) >>

Create a new instance.

=over 4

=item C<< server => "server:port" >>

server address and port

=item connect_timeout

Time seconds to wait for establish connection. default: 5

=item io_timeout

Time seconds to wait for send request and read response. default: 1

=item utf8

If enabled, Redis::Jet does encode/decode strings. default: 0 (false)

=item noreply

IF enabled. The instance does not parse any responses. Every responses to be C<"0 but true">. default: 0 (false)

=back

=item C<< ($value,[$error]) = $obj->command($command,$args,$args) >>

send a command and retrieve a value

=item C<< @values = $obj->pipeline([$command,$args,$args],[$command,$args,$args]) >>

send several commands and retrieve values

=back

=head1 LICENSE

Copyright (C) Masahiro Nagano.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Masahiro Nagano E<lt>kazeburo@gmail.comE<gt>

=cut

